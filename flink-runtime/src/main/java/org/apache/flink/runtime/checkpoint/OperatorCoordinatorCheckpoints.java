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

import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorInfo;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

/**
 * All the logic related to taking checkpoints of the {@link OperatorCoordinator}s.
 *
 * <p>NOTE: This class has a simplified error handling logic. If one of the several coordinator
 * checkpoints fail, no cleanup is triggered for the other concurrent ones. That is okay, since they
 * all produce just byte[] as the result. We have to change that once we allow then to create
 * external resources that actually need to be cleaned up.
 */
final class OperatorCoordinatorCheckpoints {

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 触发单个协调器的检查点，并返回包含协调器快照数据的 CompletableFuture。
     *
     * @param coordinatorContext 协调器的上下文，其中包含触发检查点所需的信息
     * @param checkpointId 检查点的唯一标识符
     * @return 一个 CompletableFuture 对象，该对象在协调器检查点触发并处理完成后，将包含协调器的快照数据
     * @throws Exception 如果在触发或处理协调器检查点的过程中发生异常，则抛出异常
    */
    public static CompletableFuture<CoordinatorSnapshot> triggerCoordinatorCheckpoint(
            final OperatorCoordinatorCheckpointContext coordinatorContext, final long checkpointId)
            throws Exception {
        // 创建一个新的 CompletableFuture 对象，用于异步处理检查点的结果
        final CompletableFuture<byte[]> checkpointFuture = new CompletableFuture<>();
        // 调用协调器上下文的 checkpointCoordinator 方法来触发检查点，并将结果存储在之前创建的 CompletableFuture 中
        coordinatorContext.checkpointCoordinator(checkpointId, checkpointFuture);

        // 使用 thenApply 方法转换 checkpointFuture 的结果
        // 当 checkpointFuture 完成时（即检查点触发并处理完成后），将结果（字节数组状态）转换为 CoordinatorSnapshot 对象
        // CoordinatorSnapshot 对象封装了协调器的上下文和状态数据
        return checkpointFuture.thenApply(
                (state) ->
                        new CoordinatorSnapshot(
                                coordinatorContext,
                                new ByteStreamStateHandle(
                                        coordinatorContext.operatorId().toString(), state)));
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 触发所有协调器的检查点，并返回包含所有协调器快照的 CompletableFuture。
     *
     * @param coordinators 包含需要触发检查点的 OperatorCoordinatorCheckpointContext 对象的集合
     * @param checkpointId 检查点的唯一标识符
     * @return 一个 CompletableFuture 对象，该对象在所有协调器的检查点触发完成后，将包含所有协调器的快照数据（封装在 AllCoordinatorSnapshots 对象中）
     * @throws Exception 如果在触发任何协调器检查点的过程中发生异常，则抛出异常
    */
    public static CompletableFuture<AllCoordinatorSnapshots> triggerAllCoordinatorCheckpoints(
            final Collection<OperatorCoordinatorCheckpointContext> coordinators,
            final long checkpointId)
            throws Exception {
        // 创建一个集合来存储每个协调器检查点的 CompletableFuture
        final Collection<CompletableFuture<CoordinatorSnapshot>> individualSnapshots =
                new ArrayList<>(coordinators.size());
        // 遍历所有协调器上下文，并为每个协调器触发检查点
        for (final OperatorCoordinatorCheckpointContext coordinator : coordinators) {
            // 触发协调器的检查点，并获取一个包含快照数据的 CompletableFuture
            final CompletableFuture<CoordinatorSnapshot> checkpointFuture =
                    triggerCoordinatorCheckpoint(coordinator, checkpointId);
            // 将该 CompletableFuture 添加到集合中
            individualSnapshots.add(checkpointFuture);
        }
        // 当所有检查点都完成时，使用 thenApply 方法将结果集合转换为 AllCoordinatorSnapshots 对象
        return FutureUtils.combineAll(individualSnapshots).thenApply(AllCoordinatorSnapshots::new);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 触发所有协调器的检查点，并在完成后确认它们。
     *
     * @param coordinators 一个 OperatorCoordinatorCheckpointContext 对象的集合，代表需要触发检查点的协调器上下文
     * @param checkpoint 待处理的 PendingCheckpoint 对象
     * @param acknowledgeExecutor 用于执行确认操作的 Executor
     * @return 一个 CompletableFuture 对象，该对象在所有协调器检查点被触发并确认后完成（但不包含任何结果）
     * @throws Exception 如果在触发或确认检查点的过程中发生异常，则抛出异常（但通常会被封装为 CompletionException 并在异步处理中重新抛出）
    */
    public static CompletableFuture<Void> triggerAndAcknowledgeAllCoordinatorCheckpoints(
            final Collection<OperatorCoordinatorCheckpointContext> coordinators,
            final PendingCheckpoint checkpoint,
            final Executor acknowledgeExecutor)
            throws Exception {
        // 触发所有协调器的检查点，并返回一个 CompletableFuture，该 CompletableFuture 在所有检查点触发后将包含所有协调器的快照
        final CompletableFuture<AllCoordinatorSnapshots> snapshots =
                triggerAllCoordinatorCheckpoints(coordinators, checkpoint.getCheckpointID());

        // 使用 thenAcceptAsync 方法异步处理快照结果，并在完成后进行确认
        // 注意：这里使用了 acknowledgeExecutor 来异步执行确认操作
        return snapshots.thenAcceptAsync(
                (allSnapshots) -> {
                    try {
                        // 确认所有协调器的检查点
                        acknowledgeAllCoordinators(checkpoint, allSnapshots.snapshots);
                    } catch (Exception e) {
                        // 捕获异常并封装为 CompletionException
                        throw new CompletionException(e);
                    }
                },
                acknowledgeExecutor);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 触发并确认所有协调器检查点，并在完成时返回一个 CompletableFuture 对象。
     *
     * @param coordinators 一个 OperatorCoordinatorCheckpointContext 对象的集合，代表需要触发检查点的协调器上下文
     * @param checkpoint 待处理的 PendingCheckpoint 对象
     * @param acknowledgeExecutor 用于执行确认操作的 Executor
     * @return 一个 CompletableFuture 对象，该对象将在所有协调器检查点被触发并确认后完成
     * @throws CompletionException 如果在触发或确认检查点的过程中发生异常，则抛出 CompletionException 包装原始异常
    */
    public static CompletableFuture<Void>
            triggerAndAcknowledgeAllCoordinatorCheckpointsWithCompletion(
                    final Collection<OperatorCoordinatorCheckpointContext> coordinators,
                    final PendingCheckpoint checkpoint,
                    final Executor acknowledgeExecutor)
                    throws CompletionException {

        try {
            // 调用实际的触发和确认方法
            // 该方法会处理所有协调器的检查点，并返回结果（可能是 CompletableFuture）
            return triggerAndAcknowledgeAllCoordinatorCheckpoints(
                    coordinators, checkpoint, acknowledgeExecutor);
        } catch (Exception e) {
            //抛出异常
            throw new CompletionException(e);
        }
    }

    // ------------------------------------------------------------------------

    private static void acknowledgeAllCoordinators(
            PendingCheckpoint checkpoint, Collection<CoordinatorSnapshot> snapshots)
            throws CheckpointException {
        for (final CoordinatorSnapshot snapshot : snapshots) {
            final PendingCheckpoint.TaskAcknowledgeResult result =
                    checkpoint.acknowledgeCoordinatorState(snapshot.coordinator, snapshot.state);

            if (result != PendingCheckpoint.TaskAcknowledgeResult.SUCCESS) {
                final String errorMessage =
                        "Coordinator state not acknowledged successfully: " + result;
                final Throwable error =
                        checkpoint.isDisposed() ? checkpoint.getFailureCause() : null;

                CheckpointFailureReason reason = CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE;
                if (error != null) {
                    final Optional<IOException> ioExceptionOptional =
                            ExceptionUtils.findThrowable(error, IOException.class);
                    if (ioExceptionOptional.isPresent()) {
                        reason = CheckpointFailureReason.IO_EXCEPTION;
                    }

                    throw new CheckpointException(errorMessage, reason, error);
                } else {
                    throw new CheckpointException(errorMessage, reason);
                }
            }
        }
    }

    // ------------------------------------------------------------------------

    static final class AllCoordinatorSnapshots {

        private final Collection<CoordinatorSnapshot> snapshots;

        AllCoordinatorSnapshots(Collection<CoordinatorSnapshot> snapshots) {
            this.snapshots = snapshots;
        }

        public Iterable<CoordinatorSnapshot> snapshots() {
            return snapshots;
        }
    }

    static final class CoordinatorSnapshot {

        final OperatorInfo coordinator;
        final ByteStreamStateHandle state;

        CoordinatorSnapshot(OperatorInfo coordinator, ByteStreamStateHandle state) {
            this.coordinator = coordinator;
            this.state = state;
        }
    }
}
