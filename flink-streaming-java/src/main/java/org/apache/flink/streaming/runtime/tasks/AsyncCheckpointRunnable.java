/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.taskmanager.AsyncExceptionHandler;
import org.apache.flink.runtime.taskmanager.AsynchronousException;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This runnable executes the asynchronous parts of all involved backend snapshots for the subtask.
 */
final class AsyncCheckpointRunnable implements Runnable, Closeable {

    public static final Logger LOG = LoggerFactory.getLogger(AsyncCheckpointRunnable.class);
    private final String taskName;
    private final Consumer<AsyncCheckpointRunnable> unregisterConsumer;
    private final boolean isTaskDeployedAsFinished;
    private final boolean isTaskFinished;
    private final Supplier<Boolean> isTaskRunning;
    private final Environment taskEnvironment;
    private final CompletableFuture<Void> finishedFuture = new CompletableFuture<>();

    public boolean isRunning() {
        return asyncCheckpointState.get() == AsyncCheckpointState.RUNNING;
    }

    enum AsyncCheckpointState {
        RUNNING,
        DISCARDED,
        COMPLETED
    }

    private final AsyncExceptionHandler asyncExceptionHandler;
    private final Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress;
    private final CheckpointMetaData checkpointMetaData;
    private final CheckpointMetricsBuilder checkpointMetrics;
    private final long asyncConstructionNanos;
    private final AtomicReference<AsyncCheckpointState> asyncCheckpointState =
            new AtomicReference<>(AsyncCheckpointState.RUNNING);

    AsyncCheckpointRunnable(
            Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
            CheckpointMetaData checkpointMetaData,
            CheckpointMetricsBuilder checkpointMetrics,
            long asyncConstructionNanos,
            String taskName,
            Consumer<AsyncCheckpointRunnable> unregister,
            Environment taskEnvironment,
            AsyncExceptionHandler asyncExceptionHandler,
            boolean isTaskDeployedAsFinished,
            boolean isTaskFinished,
            Supplier<Boolean> isTaskRunning) {

        this.operatorSnapshotsInProgress = checkNotNull(operatorSnapshotsInProgress);
        this.checkpointMetaData = checkNotNull(checkpointMetaData);
        this.checkpointMetrics = checkNotNull(checkpointMetrics);
        this.asyncConstructionNanos = asyncConstructionNanos;
        this.taskName = checkNotNull(taskName);
        this.unregisterConsumer = unregister;
        this.taskEnvironment = checkNotNull(taskEnvironment);
        this.asyncExceptionHandler = checkNotNull(asyncExceptionHandler);
        this.isTaskDeployedAsFinished = isTaskDeployedAsFinished;
        this.isTaskFinished = isTaskFinished;
        this.isTaskRunning = isTaskRunning;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *
    */
    @Override
    public void run() {
        // 记录异步操作开始的时间（纳秒）
        final long asyncStartNanos = System.nanoTime();
        // 计算从构造异步操作对象到开始执行之间的延迟时间（毫秒）
        final long asyncStartDelayMillis = (asyncStartNanos - asyncConstructionNanos) / 1_000_000L;
        // 记录日志，显示异步检查点开始执行的信息，包括任务名、检查点ID和异步启动延迟时间
        LOG.debug(
                "{} - started executing asynchronous part of checkpoint {}. Asynchronous start delay: {} ms",
                taskName,
                checkpointMetaData.getCheckpointId(),
                asyncStartDelayMillis);

        // 初始化线程的文件系统安全网 确保线程在访问文件系统时的安全或恢复措施
        FileSystemSafetyNet.initializeSafetyNetForThread();
        try {

            // 根据任务是否以已完成状态部署，决定是完成已完成的快照还是非完成的快照
            SnapshotsFinalizeResult snapshotsFinalizeResult =
                    isTaskDeployedAsFinished
                            ? finalizedFinishedSnapshots()// 如果任务以已完成状态部署，则完成已完成的快照
                            : finalizeNonFinishedSnapshots();// 否则，完成非完成的快照
            // 记录异步操作结束的时间（纳秒）
            final long asyncEndNanos = System.nanoTime();
            // 计算异步操作的持续时间（毫秒）
            final long asyncDurationMillis = (asyncEndNanos - asyncConstructionNanos) / 1_000_000L;

            // 更新检查点指标（metrics），设置在对齐期间持久化的字节数
            checkpointMetrics.setBytesPersistedDuringAlignment(
                    snapshotsFinalizeResult.bytesPersistedDuringAlignment);
            // 更新检查点指标，设置异步操作的持续时间（毫秒）
            checkpointMetrics.setAsyncDurationMillis(asyncDurationMillis);

            // 尝试将异步检查点的状态从RUNNING更改为COMPLETED
            // 如果状态更改成功，说明异步检查点成功完成
            if (asyncCheckpointState.compareAndSet(
                    AsyncCheckpointState.RUNNING, AsyncCheckpointState.COMPLETED)) {
                // 报告已完成的快照状态，包括JobManager、Task和Operator的子任务状态
                // 同时传入异步操作的持续时间
                reportCompletedSnapshotStates(
                        snapshotsFinalizeResult.jobManagerTaskOperatorSubtaskStates,
                        snapshotsFinalizeResult.localTaskOperatorSubtaskStates,
                        asyncDurationMillis);

            } else {
                // 如果状态更改失败，可能是因为检查点在之前已经被关闭或取消
                // 因此，日志中记录一条debug级别的消息
                LOG.debug(
                        "{} - asynchronous part of checkpoint {} could not be completed because it was closed before.",
                        taskName,
                        checkpointMetaData.getCheckpointId());
            }
            // 无论是否成功完成，都完成future对象，表示异步操作已经结束
            // 在这里传入null作为结果，因为可能并不需要具体的返回值
            finishedFuture.complete(null);
        } catch (Exception e) {
            // 捕获异常，如果异步操作在执行过程中出现异常
            LOG.info(
                    "{} - asynchronous part of checkpoint {} could not be completed.",
                    taskName,
                    checkpointMetaData.getCheckpointId(),
                    e);
            handleExecutionException(e);
            finishedFuture.completeExceptionally(e);
        } finally {
            // 无论成功还是失败，都执行清理操作
            unregisterConsumer.accept(this);
            // 关闭线程的文件系统安全网，并清理受保护的资源
            FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 阻塞等待所有的状态操作执行完成
    */
    private SnapshotsFinalizeResult finalizedFinishedSnapshots() throws Exception {
        // 遍历所有正在进行的操作员快照
        for (Map.Entry<OperatorID, OperatorSnapshotFutures> entry :
                operatorSnapshotsInProgress.entrySet()) {
            // 获取当前操作员快照的状态和结果
            OperatorSnapshotFutures snapshotInProgress = entry.getValue();
            // We should wait for the channels states get completed before continuing,
            // otherwise the alignment of barriers might have not finished yet.
            // 等待输入通道的状态完成。这是必要的，因为如果在通道状态完成之前继续，
            // 屏障的对齐可能还没有完成，这可能导致数据不一致或其他问题。
            snapshotInProgress.getInputChannelStateFuture().get();
            // 等待结果子分区状态完成。这也是确保所有数据都已正确处理和持久化的重要步骤。
            snapshotInProgress.getResultSubpartitionStateFuture().get();
        }

        return new SnapshotsFinalizeResult(
                TaskStateSnapshot.FINISHED_ON_RESTORE, TaskStateSnapshot.FINISHED_ON_RESTORE, 0L);
    }

    private SnapshotsFinalizeResult finalizeNonFinishedSnapshots() throws Exception {
        TaskStateSnapshot jobManagerTaskOperatorSubtaskStates =
                new TaskStateSnapshot(operatorSnapshotsInProgress.size(), isTaskFinished);
        TaskStateSnapshot localTaskOperatorSubtaskStates =
                new TaskStateSnapshot(operatorSnapshotsInProgress.size(), isTaskFinished);

        long bytesPersistedDuringAlignment = 0;
        for (Map.Entry<OperatorID, OperatorSnapshotFutures> entry :
                operatorSnapshotsInProgress.entrySet()) {

            OperatorID operatorID = entry.getKey();
            OperatorSnapshotFutures snapshotInProgress = entry.getValue();

            // finalize the async part of all by executing all snapshot runnables
            OperatorSnapshotFinalizer finalizedSnapshots =
                    new OperatorSnapshotFinalizer(snapshotInProgress);

            jobManagerTaskOperatorSubtaskStates.putSubtaskStateByOperatorID(
                    operatorID, finalizedSnapshots.getJobManagerOwnedState());

            localTaskOperatorSubtaskStates.putSubtaskStateByOperatorID(
                    operatorID, finalizedSnapshots.getTaskLocalState());

            bytesPersistedDuringAlignment +=
                    finalizedSnapshots
                            .getJobManagerOwnedState()
                            .getResultSubpartitionState()
                            .getStateSize();
            bytesPersistedDuringAlignment +=
                    finalizedSnapshots
                            .getJobManagerOwnedState()
                            .getInputChannelState()
                            .getStateSize();
        }

        return new SnapshotsFinalizeResult(
                jobManagerTaskOperatorSubtaskStates,
                localTaskOperatorSubtaskStates,
                bytesPersistedDuringAlignment);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 报告Checkpoint快照完成状态
     * @param acknowledgedTaskStateSnapshot 已被确认的任务状态快照
     * @param localTaskStateSnapshot 本地任务状态快照
     * @param asyncDurationMillis 异步快照操作持续时间（毫秒）
    */
    private void reportCompletedSnapshotStates(
            TaskStateSnapshot acknowledgedTaskStateSnapshot,
            TaskStateSnapshot localTaskStateSnapshot,
            long asyncDurationMillis) {
        // 检查是否存在已确认的状态快照
        boolean hasAckState = acknowledgedTaskStateSnapshot.hasState();
        // 检查是否存在本地状态快照
        boolean hasLocalState = localTaskStateSnapshot.hasState();

        // 校验状态：确保如果本地存在状态，则必须有对应的已确认状态，
        // 或者如果没有已确认状态，则本地也不应该有状态
        // 这用于防止本地有缓存状态但未被作业管理器确认的情况
        checkState(
                hasAckState || !hasLocalState,
                "Found cached state but no corresponding primary state is reported to the job "
                        + "manager. This indicates a problem.");

        // we signal stateless tasks by reporting null, so that there are no attempts to assign
        // empty state
        // to stateless tasks on restore. This enables simple job modifications that only concern
        // stateless without the need to assign them uids to match their (always empty) states.

        // 如果任务是无状态的（即没有状态需要保存），我们通过报告null状态来标记，
        // 这样在恢复时就不会尝试将空状态分配给无状态任务。
        taskEnvironment
                .getTaskStateManager()
                .reportTaskStateSnapshots(
                        checkpointMetaData,// 检查点元数据
                        checkpointMetrics
                                .setBytesPersistedOfThisCheckpoint(
                                        acknowledgedTaskStateSnapshot.getCheckpointedSize())
                                .setTotalBytesPersisted(
                                        acknowledgedTaskStateSnapshot.getStateSize())
                                .build(),// 构建检查点指标
                        hasAckState ? acknowledgedTaskStateSnapshot : null,// 如果有已确认状态，则报告；否则报告null
                        hasLocalState ? localTaskStateSnapshot : null);// 如果有本地状态，则报告；否则报告null
        // 记录日志：调试级别，显示任务名称、完成的检查点ID以及异步操作的持续时间
        LOG.debug(
                "{} - finished asynchronous part of checkpoint {}. Asynchronous duration: {} ms",
                taskName,
                checkpointMetaData.getCheckpointId(),
                asyncDurationMillis);
        // 这是一个跟踪级别的日志记录，用于在调试或诊断时捕获详细的信息
        // 该日志记录报告了在给定检查点中，针对特定任务所报告的状态快照
        LOG.trace(
                "{} - reported the following states in snapshot for checkpoint {}: {}.",
                taskName,
                checkpointMetaData.getCheckpointId(),
                acknowledgedTaskStateSnapshot);
    }

    private void reportAbortedSnapshotStats(long stateSize, long checkpointedSize) {
        CheckpointMetrics metrics =
                checkpointMetrics
                        .setTotalBytesPersisted(stateSize)
                        .setBytesPersistedOfThisCheckpoint(checkpointedSize)
                        .buildIncomplete();
        LOG.trace(
                "{} - report failed checkpoint stats: {} {}",
                taskName,
                checkpointMetaData.getCheckpointId(),
                metrics);

        taskEnvironment
                .getTaskStateManager()
                .reportIncompleteTaskStateSnapshots(checkpointMetaData, metrics);
    }

    private void handleExecutionException(Exception e) {

        boolean didCleanup = false;
        AsyncCheckpointState currentState = asyncCheckpointState.get();

        while (AsyncCheckpointState.DISCARDED != currentState) {

            if (asyncCheckpointState.compareAndSet(currentState, AsyncCheckpointState.DISCARDED)) {

                didCleanup = true;

                try {
                    cleanup();
                } catch (Exception cleanupException) {
                    e.addSuppressed(cleanupException);
                }

                Exception checkpointException =
                        new Exception(
                                "Could not materialize checkpoint "
                                        + checkpointMetaData.getCheckpointId()
                                        + " for operator "
                                        + taskName
                                        + '.',
                                e);

                if (isTaskRunning.get()) {
                    // We only report the exception for the original cause of fail and cleanup.
                    // Otherwise this followup exception could race the original exception in
                    // failing the task.
                    try {
                        Optional<CheckpointException> underlyingCheckpointException =
                                ExceptionUtils.findThrowable(
                                        checkpointException, CheckpointException.class);

                        // If this failure is already a CheckpointException, do not overwrite the
                        // original CheckpointFailureReason
                        CheckpointFailureReason reportedFailureReason =
                                underlyingCheckpointException
                                        .map(exception -> exception.getCheckpointFailureReason())
                                        .orElse(CheckpointFailureReason.CHECKPOINT_ASYNC_EXCEPTION);
                        taskEnvironment.declineCheckpoint(
                                checkpointMetaData.getCheckpointId(),
                                new CheckpointException(
                                        reportedFailureReason, checkpointException));
                    } catch (Exception unhandled) {
                        AsynchronousException asyncException = new AsynchronousException(unhandled);
                        asyncExceptionHandler.handleAsyncException(
                                "Failure in asynchronous checkpoint materialization",
                                asyncException);
                    }
                } else {
                    // We never decline checkpoint after task is not running to avoid unexpected job
                    // failover, which caused by exceeding checkpoint tolerable failure threshold.
                    LOG.info(
                            "Ignore decline of checkpoint {} as task is not running anymore.",
                            checkpointMetaData.getCheckpointId());
                }

                currentState = AsyncCheckpointState.DISCARDED;
            } else {
                currentState = asyncCheckpointState.get();
            }
        }

        if (!didCleanup) {
            LOG.trace(
                    "Caught followup exception from a failed checkpoint thread. This can be ignored.",
                    e);
        }
    }

    @Override
    public void close() {
        if (asyncCheckpointState.compareAndSet(
                AsyncCheckpointState.RUNNING, AsyncCheckpointState.DISCARDED)) {

            try {
                final Tuple2<Long, Long> tuple = cleanup();
                reportAbortedSnapshotStats(tuple.f0, tuple.f1);
            } catch (Exception cleanupException) {
                LOG.warn(
                        "Could not properly clean up the async checkpoint runnable.",
                        cleanupException);
            }
        } else {
            logFailedCleanupAttempt();
        }
    }

    long getCheckpointId() {
        return checkpointMetaData.getCheckpointId();
    }

    public CompletableFuture<Void> getFinishedFuture() {
        return finishedFuture;
    }

    /** @return discarded full/incremental size (if available). */
    private Tuple2<Long, Long> cleanup() throws Exception {
        LOG.debug(
                "Cleanup AsyncCheckpointRunnable for checkpoint {} of {}.",
                checkpointMetaData.getCheckpointId(),
                taskName);

        Exception exception = null;

        // clean up ongoing operator snapshot results and non partitioned state handles
        long stateSize = 0, checkpointedSize = 0;
        for (OperatorSnapshotFutures operatorSnapshotResult :
                operatorSnapshotsInProgress.values()) {
            if (operatorSnapshotResult != null) {
                try {
                    Tuple2<Long, Long> tuple2 = operatorSnapshotResult.cancel();
                    stateSize += tuple2.f0;
                    checkpointedSize += tuple2.f1;
                } catch (Exception cancelException) {
                    exception = ExceptionUtils.firstOrSuppressed(cancelException, exception);
                }
            }
        }

        if (null != exception) {
            throw exception;
        }
        return Tuple2.of(stateSize, checkpointedSize);
    }

    private void logFailedCleanupAttempt() {
        LOG.debug(
                "{} - asynchronous checkpointing operation for checkpoint {} has "
                        + "already been completed. Thus, the state handles are not cleaned up.",
                taskName,
                checkpointMetaData.getCheckpointId());
    }

    private static class SnapshotsFinalizeResult {
        final TaskStateSnapshot jobManagerTaskOperatorSubtaskStates;
        final TaskStateSnapshot localTaskOperatorSubtaskStates;
        final long bytesPersistedDuringAlignment;

        public SnapshotsFinalizeResult(
                TaskStateSnapshot jobManagerTaskOperatorSubtaskStates,
                TaskStateSnapshot localTaskOperatorSubtaskStates,
                long bytesPersistedDuringAlignment) {
            this.jobManagerTaskOperatorSubtaskStates = jobManagerTaskOperatorSubtaskStates;
            this.localTaskOperatorSubtaskStates = localTaskOperatorSubtaskStates;
            this.bytesPersistedDuringAlignment = bytesPersistedDuringAlignment;
        }
    }
}
