/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorInfo;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.FutureUtils.ConjunctFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pending checkpoint is a checkpoint that has been started, but has not been acknowledged by all
 * tasks that need to acknowledge it. Once all tasks have acknowledged it, it becomes a {@link
 * CompletedCheckpoint}.
 *
 * <p>Note that the pending checkpoint, as well as the successful checkpoint keep the state handles
 * always as serialized values, never as actual values.
 */
@NotThreadSafe
public class PendingCheckpoint implements Checkpoint {

    /** Result of the {@link PendingCheckpoint#acknowledgedTasks} method. */
    public enum TaskAcknowledgeResult {
        SUCCESS, // successful acknowledge of the task
        DUPLICATE, // acknowledge message is a duplicate
        UNKNOWN, // unknown task acknowledged
        DISCARDED // pending checkpoint has been discarded
    }

    // ------------------------------------------------------------------------

    /** The PendingCheckpoint logs to the same logger as the CheckpointCoordinator. */
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

    private final Object lock = new Object();

    private final JobID jobId;

    private final long checkpointId;

    private final long checkpointTimestamp;

    private final Map<OperatorID, OperatorState> operatorStates;

    private final CheckpointPlan checkpointPlan;

    private final Map<ExecutionAttemptID, ExecutionVertex> notYetAcknowledgedTasks;

    private final Set<OperatorID> notYetAcknowledgedOperatorCoordinators;

    private final List<MasterState> masterStates;

    private final Set<String> notYetAcknowledgedMasterStates;

    /** Set of acknowledged tasks. */
    private final Set<ExecutionAttemptID> acknowledgedTasks;

    /** The checkpoint properties. */
    private final CheckpointProperties props;

    /**
     * The promise to fulfill once the checkpoint has been completed. Note that it will be completed
     * only after the checkpoint is successfully added to CompletedCheckpointStore.
     */
    private final CompletableFuture<CompletedCheckpoint> onCompletionPromise;

    @Nullable private final PendingCheckpointStats pendingCheckpointStats;

    private final CompletableFuture<Void> masterTriggerCompletionPromise;

    /** Target storage location to persist the checkpoint metadata to. */
    @Nullable private CheckpointStorageLocation targetLocation;

    private int numAcknowledgedTasks;

    private boolean disposed;

    private boolean discarded;

    private volatile ScheduledFuture<?> cancellerHandle;

    private CheckpointException failureCause;

    // --------------------------------------------------------------------------------------------

    public PendingCheckpoint(
            JobID jobId,
            long checkpointId,
            long checkpointTimestamp,
            CheckpointPlan checkpointPlan,
            Collection<OperatorID> operatorCoordinatorsToConfirm,
            Collection<String> masterStateIdentifiers,
            CheckpointProperties props,
            CompletableFuture<CompletedCheckpoint> onCompletionPromise,
            @Nullable PendingCheckpointStats pendingCheckpointStats,
            CompletableFuture<Void> masterTriggerCompletionPromise) {
        checkArgument(
                checkpointPlan.getTasksToWaitFor().size() > 0,
                "Checkpoint needs at least one vertex that commits the checkpoint");

        this.jobId = checkNotNull(jobId);
        this.checkpointId = checkpointId;
        this.checkpointTimestamp = checkpointTimestamp;
        this.checkpointPlan = checkNotNull(checkpointPlan);

        this.notYetAcknowledgedTasks =
                CollectionUtil.newHashMapWithExpectedSize(
                        checkpointPlan.getTasksToWaitFor().size());
        for (Execution execution : checkpointPlan.getTasksToWaitFor()) {
            notYetAcknowledgedTasks.put(execution.getAttemptId(), execution.getVertex());
        }

        this.props = checkNotNull(props);

        this.operatorStates = new HashMap<>();
        this.masterStates = new ArrayList<>(masterStateIdentifiers.size());
        this.notYetAcknowledgedMasterStates =
                masterStateIdentifiers.isEmpty()
                        ? Collections.emptySet()
                        : new HashSet<>(masterStateIdentifiers);
        this.notYetAcknowledgedOperatorCoordinators =
                operatorCoordinatorsToConfirm.isEmpty()
                        ? Collections.emptySet()
                        : new HashSet<>(operatorCoordinatorsToConfirm);
        this.acknowledgedTasks =
                CollectionUtil.newHashSetWithExpectedSize(
                        checkpointPlan.getTasksToWaitFor().size());
        this.onCompletionPromise = checkNotNull(onCompletionPromise);
        this.pendingCheckpointStats = pendingCheckpointStats;
        this.masterTriggerCompletionPromise = checkNotNull(masterTriggerCompletionPromise);
    }

    // --------------------------------------------------------------------------------------------

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    public JobID getJobId() {
        return jobId;
    }

    @Override
    public long getCheckpointID() {
        return checkpointId;
    }

    public void setCheckpointTargetLocation(CheckpointStorageLocation targetLocation) {
        this.targetLocation = targetLocation;
    }

    public CheckpointStorageLocation getCheckpointStorageLocation() {
        return targetLocation;
    }

    public long getCheckpointTimestamp() {
        return checkpointTimestamp;
    }

    public int getNumberOfNonAcknowledgedTasks() {
        return notYetAcknowledgedTasks.size();
    }

    public int getNumberOfNonAcknowledgedOperatorCoordinators() {
        return notYetAcknowledgedOperatorCoordinators.size();
    }

    public CheckpointPlan getCheckpointPlan() {
        return checkpointPlan;
    }

    public int getNumberOfAcknowledgedTasks() {
        return numAcknowledgedTasks;
    }

    public Map<OperatorID, OperatorState> getOperatorStates() {
        return operatorStates;
    }

    public List<MasterState> getMasterStates() {
        return masterStates;
    }

    public boolean isFullyAcknowledged() {
        return areTasksFullyAcknowledged()
                && areCoordinatorsFullyAcknowledged()
                && areMasterStatesFullyAcknowledged();
    }

    boolean areMasterStatesFullyAcknowledged() {
        return notYetAcknowledgedMasterStates.isEmpty() && !disposed;
    }

    boolean areCoordinatorsFullyAcknowledged() {
        return notYetAcknowledgedOperatorCoordinators.isEmpty() && !disposed;
    }

    boolean areTasksFullyAcknowledged() {
        return notYetAcknowledgedTasks.isEmpty() && !disposed;
    }

    public boolean isAcknowledgedBy(ExecutionAttemptID executionAttemptId) {
        return !notYetAcknowledgedTasks.containsKey(executionAttemptId);
    }

    public boolean isDisposed() {
        return disposed;
    }

    /**
     * Checks whether this checkpoint can be subsumed or whether it should always continue,
     * regardless of newer checkpoints in progress.
     *
     * @return True if the checkpoint can be subsumed, false otherwise.
     */
    public boolean canBeSubsumed() {
        // If the checkpoint is forced, it cannot be subsumed.
        return !props.isSavepoint();
    }

    CheckpointProperties getProps() {
        return props;
    }

    /**
     * Sets the handle for the canceller to this pending checkpoint. This method fails with an
     * exception if a handle has already been set.
     *
     * @return true, if the handle was set, false, if the checkpoint is already disposed;
     */
    public boolean setCancellerHandle(ScheduledFuture<?> cancellerHandle) {
        synchronized (lock) {
            if (this.cancellerHandle == null) {
                if (!disposed) {
                    this.cancellerHandle = cancellerHandle;
                    return true;
                } else {
                    return false;
                }
            } else {
                throw new IllegalStateException("A canceller handle was already set");
            }
        }
    }

    public CheckpointException getFailureCause() {
        return failureCause;
    }

    // ------------------------------------------------------------------------
    //  Progress and Completion
    // ------------------------------------------------------------------------

    /**
     * Returns the completion future.
     *
     * @return A future to the completed checkpoint
     */
    public CompletableFuture<CompletedCheckpoint> getCompletionFuture() {
        return onCompletionPromise;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 最终完成Checkpoint
    */
    public CompletedCheckpoint finalizeCheckpoint(
            CheckpointsCleaner checkpointsCleaner, Runnable postCleanup, Executor executor)
            throws IOException {
        // 使用同步块确保在finalizeCheckpoint方法执行期间，对象的状态不会被其他线程修改
        synchronized (lock) {
            // 检查检查点是否已被丢弃
            checkState(!isDisposed(), "checkpoint is discarded");
            // 检查检查点是否已完全确认
            checkState(
                    isFullyAcknowledged(),
                    "Pending checkpoint has not been fully acknowledged yet");

            // make sure we fulfill the promise with an exception if something fails
            // 尝试完成检查点，如果在完成过程中发生异常，则确保通过异常来履行承诺
            try {
                // 更新操作员状态以标记它们已完成
                checkpointPlan.fulfillFinishedTaskStatus(operatorStates);

                // write out the metadata
                // 写入元数据
                // 创建一个包含检查点ID、操作员状态、主状态以及属性的检查点元数据对象
                final CheckpointMetadata savepoint =
                        new CheckpointMetadata(
                                checkpointId, operatorStates.values(), masterStates, props);
                // 创建一个存储位置的输出流，用于保存检查点元数据
                // 存储位置可能是文件系统、数据库等
                final CompletedCheckpointStorageLocation finalizedLocation;

                try (CheckpointMetadataOutputStream out =
                        targetLocation.createMetadataOutputStream()) {
                    // 将检查点元数据写入输出流
                    Checkpoints.storeCheckpointMetadata(savepoint, out);
                    // 关闭输出流并获取最终的存储位置
                    // 这可能涉及一些后处理步骤，如同步、刷新缓存等
                    finalizedLocation = out.closeAndFinalizeCheckpoint();
                }
                // 创建一个已完成的检查点对象
                // 包含作业ID、检查点ID、时间戳、操作员状态、主状态、属性以及最终的存储位置等信息
                CompletedCheckpoint completed =
                        new CompletedCheckpoint(
                                jobId,
                                checkpointId,
                                checkpointTimestamp,
                                System.currentTimeMillis(),
                                operatorStates,
                                masterStates,
                                props,
                                finalizedLocation,
                                toCompletedCheckpointStats(finalizedLocation));

                // mark this pending checkpoint as disposed, but do NOT drop the state
                // 标记此待处理检查点为已丢弃，但不丢弃状态
                // 这可能意味着将检查点从待处理列表中移除，但保留其状态以便后续恢复或其他用途
                dispose(false, checkpointsCleaner, postCleanup, executor);
                // 返回已完成的检查点对象
                return completed;
            } catch (Throwable t) {
                //异步编程封装异常
                onCompletionPromise.completeExceptionally(t);
                //抛出异常
                ExceptionUtils.rethrowIOException(t);
                //返回null
                return null; // silence the compiler
            }
        }
    }

    @Nullable
    private CompletedCheckpointStats toCompletedCheckpointStats(
            CompletedCheckpointStorageLocation finalizedLocation) {
        return pendingCheckpointStats != null
                ? pendingCheckpointStats.toCompletedCheckpointStats(
                        finalizedLocation.getExternalPointer())
                : null;
    }

    /**
     * Acknowledges the task with the given execution attempt id and the given subtask state.
     *
     * @param executionAttemptId of the acknowledged task
     * @param operatorSubtaskStates of the acknowledged task
     * @param metrics Checkpoint metrics for the stats
     * @return TaskAcknowledgeResult of the operation
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 确认具有给定执行尝试ID和子任务状态的任务。
     *
     * @param executionAttemptId 确认的任务的执行尝试ID
     * @param operatorSubtaskStates 确认的任务的子任务状态快照
     * @param metrics 检查点的度量统计信息
     * @return 操作的任务确认结果
    */
    public TaskAcknowledgeResult acknowledgeTask(
            ExecutionAttemptID executionAttemptId,
            TaskStateSnapshot operatorSubtaskStates,
            CheckpointMetrics metrics) {
        // 使用锁进行同步，确保并发安全
        synchronized (lock) {
            // 如果当前对象已经被销毁（disposed），则返回任务确认结果为“已丢弃”
            if (disposed) {
                return TaskAcknowledgeResult.DISCARDED;
            }
            // 从未确认的任务集合中移除该执行尝试ID对应的任务执行顶点
            final ExecutionVertex vertex = notYetAcknowledgedTasks.remove(executionAttemptId);
            // 如果顶点为null（即找不到该执行尝试ID对应的任务），则进行进一步判断
            if (vertex == null) {
                // 如果已确认的任务集合中包含该执行尝试ID，则返回任务确认结果为“重复”
                if (acknowledgedTasks.contains(executionAttemptId)) {
                    return TaskAcknowledgeResult.DUPLICATE;
                    // 否则，返回任务确认结果为“未知”
                } else {
                    return TaskAcknowledgeResult.UNKNOWN;
                }
                // 如果顶点不为null（即找到了该执行尝试ID对应的任务），则将其加入已确认的任务集合
            } else {
                acknowledgedTasks.add(executionAttemptId);
            }
            // 获取当前系统时间戳作为确认时间
            long ackTimestamp = System.currentTimeMillis();
            // 如果子任务状态快照不为null且标记为任务已完成（即部署为完成状态）
            if (operatorSubtaskStates != null && operatorSubtaskStates.isTaskDeployedAsFinished()) {
                // 在恢复时向检查点计划报告该任务已完成
                checkpointPlan.reportTaskFinishedOnRestore(vertex);
            } else {
                // 否则，遍历该任务的所有操作符ID
                List<OperatorIDPair> operatorIDs = vertex.getJobVertex().getOperatorIDs();
                for (OperatorIDPair operatorID : operatorIDs) {
                    // 更新操作符的状态
                    updateOperatorState(vertex, operatorSubtaskStates, operatorID);
                }
                // 如果子任务状态快照不为null且标记为任务已完成
                if (operatorSubtaskStates != null && operatorSubtaskStates.isTaskFinished()) {
                    // 在检查点计划中报告该任务的操作符已完成
                    checkpointPlan.reportTaskHasFinishedOperators(vertex);
                }
            }
            // 增加已确认任务的数量
            ++numAcknowledgedTasks;

            // publish the checkpoint statistics
            // to prevent null-pointers from concurrent modification, copy reference onto stack
            // 发布检查点统计信息
            // 为了防止并发修改导致的空指针异常，将引用复制到栈上
            if (pendingCheckpointStats != null) {
                // Do this in millis because the web frontend works with them
                // 由于前端通常以毫秒为单位显示时间，因此这里将纳秒转换为毫秒
                long alignmentDurationMillis = metrics.getAlignmentDurationNanos() / 1_000_000;
                long checkpointStartDelayMillis =
                        metrics.getCheckpointStartDelayNanos() / 1_000_000;

                // 创建一个子任务状态统计对象，用于记录该子任务在检查点过程中的各种统计信息
                // SubtaskStateStats 对象包含了子任务的索引、确认时间戳、持久化的字节数、同步和异步持续时间等
                SubtaskStateStats subtaskStateStats =
                        new SubtaskStateStats(
                                vertex.getParallelSubtaskIndex(),
                                ackTimestamp,
                                metrics.getBytesPersistedOfThisCheckpoint(),
                                metrics.getTotalBytesPersisted(),
                                metrics.getSyncDurationMillis(),
                                metrics.getAsyncDurationMillis(),
                                metrics.getBytesProcessedDuringAlignment(),
                                metrics.getBytesPersistedDuringAlignment(),
                                alignmentDurationMillis,
                                checkpointStartDelayMillis,
                                metrics.getUnalignedCheckpoint(),
                                true);
                // 记录检查点统计信息的日志（trace级别），包括：
                // - 检查点ID（checkpointId）
                // - 子任务名称（包含子任务索引）
                // - 状态大小（转换为KB，如果为0则直接记录为0）
                // - 总持续时间（从触发到结束的时间，单位毫秒）
                // - 同步部分持续时间（单位毫秒）
                // - 异步部分持续时间（单位毫秒）
                LOG.trace(
                        "Checkpoint {} stats for {}: size={}Kb, duration={}ms, sync part={}ms, async part={}ms",
                        checkpointId,
                        vertex.getTaskNameWithSubtaskIndex(),
                        subtaskStateStats.getStateSize() == 0
                                ? 0
                                : subtaskStateStats.getStateSize() / 1024,
                        subtaskStateStats.getEndToEndDuration(
                                pendingCheckpointStats.getTriggerTimestamp()),
                        subtaskStateStats.getSyncCheckpointDuration(),
                        subtaskStateStats.getAsyncCheckpointDuration());
                // 将子任务的状态统计信息报告给待处理的检查点统计信息对象
                // 这可能意味着将统计信息存储到某个中央存储系统，或用于后续的分析和报告
                pendingCheckpointStats.reportSubtaskStats(
                        vertex.getJobvertexId(), subtaskStateStats);
            }
            // 无论是否处理了统计信息，都返回成功的任务确认结果
            return TaskAcknowledgeResult.SUCCESS;
        }
    }

    private void updateOperatorState(
            ExecutionVertex vertex,
            TaskStateSnapshot operatorSubtaskStates,
            OperatorIDPair operatorID) {
        OperatorState operatorState = operatorStates.get(operatorID.getGeneratedOperatorID());

        if (operatorState == null) {
            operatorState =
                    new OperatorState(
                            operatorID.getGeneratedOperatorID(),
                            vertex.getTotalNumberOfParallelSubtasks(),
                            vertex.getMaxParallelism());
            operatorStates.put(operatorID.getGeneratedOperatorID(), operatorState);
        }
        OperatorSubtaskState operatorSubtaskState =
                operatorSubtaskStates == null
                        ? null
                        : operatorSubtaskStates.getSubtaskStateByOperatorID(
                                operatorID.getGeneratedOperatorID());

        if (operatorSubtaskState != null) {
            operatorState.putState(vertex.getParallelSubtaskIndex(), operatorSubtaskState);
        }
    }

    public TaskAcknowledgeResult acknowledgeCoordinatorState(
            OperatorInfo coordinatorInfo, @Nullable ByteStreamStateHandle stateHandle) {

        synchronized (lock) {
            if (disposed) {
                return TaskAcknowledgeResult.DISCARDED;
            }

            final OperatorID operatorId = coordinatorInfo.operatorId();
            OperatorState operatorState = operatorStates.get(operatorId);

            // sanity check for better error reporting
            if (!notYetAcknowledgedOperatorCoordinators.remove(operatorId)) {
                return operatorState != null && operatorState.getCoordinatorState() != null
                        ? TaskAcknowledgeResult.DUPLICATE
                        : TaskAcknowledgeResult.UNKNOWN;
            }

            if (operatorState == null) {
                operatorState =
                        new OperatorState(
                                operatorId,
                                coordinatorInfo.currentParallelism(),
                                coordinatorInfo.maxParallelism());
                operatorStates.put(operatorId, operatorState);
            }
            if (stateHandle != null) {
                operatorState.setCoordinatorState(stateHandle);
            }

            return TaskAcknowledgeResult.SUCCESS;
        }
    }

    /**
     * Acknowledges a master state (state generated on the checkpoint coordinator) to the pending
     * checkpoint.
     *
     * @param identifier The identifier of the master state
     * @param state The state to acknowledge
     */
    public void acknowledgeMasterState(String identifier, @Nullable MasterState state) {

        synchronized (lock) {
            if (!disposed) {
                if (notYetAcknowledgedMasterStates.remove(identifier) && state != null) {
                    masterStates.add(state);
                }
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Cancellation
    // ------------------------------------------------------------------------

    /** Aborts a checkpoint with reason and cause. */
    public void abort(
            CheckpointFailureReason reason,
            @Nullable Throwable cause,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup,
            Executor executor,
            CheckpointStatsTracker statsTracker) {
        try {
            failureCause = new CheckpointException(reason, cause);
            onCompletionPromise.completeExceptionally(failureCause);
            masterTriggerCompletionPromise.completeExceptionally(failureCause);
            assertAbortSubsumedForced(reason);
        } finally {
            dispose(true, checkpointsCleaner, postCleanup, executor);
        }
    }

    private void assertAbortSubsumedForced(CheckpointFailureReason reason) {
        if (props.isSavepoint() && reason == CheckpointFailureReason.CHECKPOINT_SUBSUMED) {
            throw new IllegalStateException(
                    "Bug: savepoints must never be subsumed, "
                            + "the abort reason is : "
                            + reason.message());
        }
    }

    private void dispose(
            boolean releaseState,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup,
            Executor executor) {

        synchronized (lock) {
            try {
                numAcknowledgedTasks = -1;
                checkpointsCleaner.cleanCheckpoint(this, releaseState, postCleanup, executor);
            } finally {
                disposed = true;
                notYetAcknowledgedTasks.clear();
                acknowledgedTasks.clear();
                cancelCanceller();
            }
        }
    }

    @Override
    public DiscardObject markAsDiscarded() {
        return new PendingCheckpointDiscardObject();
    }

    private void cancelCanceller() {
        try {
            final ScheduledFuture<?> canceller = this.cancellerHandle;
            if (canceller != null) {
                canceller.cancel(false);
            }
        } catch (Exception e) {
            // this code should not throw exceptions
            LOG.warn("Error while cancelling checkpoint timeout task", e);
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return String.format(
                "Pending Checkpoint %d @ %d - confirmed=%d, pending=%d",
                checkpointId,
                checkpointTimestamp,
                getNumberOfAcknowledgedTasks(),
                getNumberOfNonAcknowledgedTasks());
    }

    /**
     * Implementation of {@link org.apache.flink.runtime.checkpoint.Checkpoint.DiscardObject} for
     * {@link PendingCheckpoint}.
     */
    public class PendingCheckpointDiscardObject implements DiscardObject {
        /**
         * Discard state. Must be called after {@link #dispose(boolean, CheckpointsCleaner,
         * Runnable, Executor) dispose}.
         */
        @Override
        public void discard() {
            synchronized (lock) {
                if (discarded) {
                    Preconditions.checkState(
                            disposed, "Checkpoint should be disposed before being discarded");
                    return;
                } else {
                    discarded = true;
                }
            }
            // discard the private states.
            // unregistered shared states are still considered private at this point.
            try {
                StateUtil.bestEffortDiscardAllStateObjects(operatorStates.values());
                if (targetLocation != null) {
                    targetLocation.disposeOnFailure();
                }
            } catch (Throwable t) {
                LOG.warn(
                        "Could not properly dispose the private states in the pending checkpoint {} of job {}.",
                        checkpointId,
                        jobId,
                        t);
            } finally {
                operatorStates.clear();
            }
        }

        @Override
        public CompletableFuture<Void> discardAsync(Executor ioExecutor) {
            synchronized (lock) {
                if (discarded) {
                    Preconditions.checkState(
                            disposed, "Checkpoint should be disposed before being discarded");
                } else {
                    discarded = true;
                }
            }
            List<StateObject> discardables =
                    operatorStates.values().stream()
                            .flatMap(op -> op.getDiscardables().stream())
                            .collect(Collectors.toList());

            ConjunctFuture<Void> discardStates =
                    FutureUtils.completeAll(
                            discardables.stream()
                                    .map(
                                            item ->
                                                    FutureUtils.runAsync(
                                                            item::discardState, ioExecutor))
                                    .collect(Collectors.toList()));

            return FutureUtils.runAfterwards(
                    discardStates,
                    () -> {
                        operatorStates.clear();
                        if (targetLocation != null) {
                            targetLocation.disposeOnFailure();
                        }
                    });
        }
    }
}
