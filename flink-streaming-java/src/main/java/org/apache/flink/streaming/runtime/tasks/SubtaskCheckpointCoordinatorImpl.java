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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter.ChannelStateWriteResult;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriterImpl;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStateToolset;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.filesystem.FsMergingCheckpointStorageLocation;
import org.apache.flink.runtime.taskmanager.AsyncExceptionHandler;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.runtime.io.checkpointing.BarrierAlignmentUtil;
import org.apache.flink.streaming.runtime.io.checkpointing.BarrierAlignmentUtil.Cancellable;
import org.apache.flink.streaming.runtime.io.checkpointing.BarrierAlignmentUtil.DelayableTimer;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.function.BiFunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.flink.util.IOUtils.closeQuietly;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

class SubtaskCheckpointCoordinatorImpl implements SubtaskCheckpointCoordinator {

    private static final Logger LOG =
            LoggerFactory.getLogger(SubtaskCheckpointCoordinatorImpl.class);

    private static final int CHECKPOINT_EXECUTION_DELAY_LOG_THRESHOLD_MS = 30_000;

    private final boolean enableCheckpointAfterTasksFinished;

    private final CachingCheckpointStorageWorkerView checkpointStorage;
    private final String taskName;
    private final ExecutorService asyncOperationsThreadPool;
    private final Environment env;
    private final AsyncExceptionHandler asyncExceptionHandler;
    private final ChannelStateWriter channelStateWriter;
    private final StreamTaskActionExecutor actionExecutor;
    private final BiFunctionWithException<
                    ChannelStateWriter, Long, CompletableFuture<Void>, CheckpointException>
            prepareInputSnapshot;
    /** The IDs of the checkpoint for which we are notified aborted. */
    private final Set<Long> abortedCheckpointIds;

    private final int maxRecordAbortedCheckpoints;

    private long maxAbortedCheckpointId = 0;

    private long lastCheckpointId;

    /** Lock that guards state of AsyncCheckpointRunnable registry. * */
    private final Object lock;

    @GuardedBy("lock")
    private final Map<Long, AsyncCheckpointRunnable> checkpoints;

    /** Indicates if this registry is closed. */
    @GuardedBy("lock")
    private boolean closed;

    private final DelayableTimer registerTimer;

    private final Clock clock;

    /** It always be called in Task Thread. */
    private Cancellable alignmentTimer;

    /**
     * It is the checkpointId corresponding to alignmentTimer. And It should be always updated with
     * {@link #alignmentTimer}.
     */
    private long alignmentCheckpointId;

    SubtaskCheckpointCoordinatorImpl(
            CheckpointStorage checkpointStorage,
            CheckpointStorageWorkerView checkpointStorageView,
            String taskName,
            StreamTaskActionExecutor actionExecutor,
            ExecutorService asyncOperationsThreadPool,
            Environment env,
            AsyncExceptionHandler asyncExceptionHandler,
            boolean unalignedCheckpointEnabled,
            boolean enableCheckpointAfterTasksFinished,
            BiFunctionWithException<
                            ChannelStateWriter, Long, CompletableFuture<Void>, CheckpointException>
                    prepareInputSnapshot,
            int maxRecordAbortedCheckpoints,
            DelayableTimer registerTimer,
            int maxSubtasksPerChannelStateFile) {
        this(
                checkpointStorageView,
                taskName,
                actionExecutor,
                asyncOperationsThreadPool,
                env,
                asyncExceptionHandler,
                prepareInputSnapshot,
                maxRecordAbortedCheckpoints,
                unalignedCheckpointEnabled
                        ? openChannelStateWriter(
                                taskName, checkpointStorage, env, maxSubtasksPerChannelStateFile)
                        : ChannelStateWriter.NO_OP,
                enableCheckpointAfterTasksFinished,
                registerTimer);
    }

    @VisibleForTesting
    SubtaskCheckpointCoordinatorImpl(
            CheckpointStorageWorkerView checkpointStorage,
            String taskName,
            StreamTaskActionExecutor actionExecutor,
            ExecutorService asyncOperationsThreadPool,
            Environment env,
            AsyncExceptionHandler asyncExceptionHandler,
            BiFunctionWithException<
                            ChannelStateWriter, Long, CompletableFuture<Void>, CheckpointException>
                    prepareInputSnapshot,
            int maxRecordAbortedCheckpoints,
            ChannelStateWriter channelStateWriter,
            boolean enableCheckpointAfterTasksFinished,
            DelayableTimer registerTimer) {
        this.checkpointStorage =
                new CachingCheckpointStorageWorkerView(checkNotNull(checkpointStorage));
        this.taskName = checkNotNull(taskName);
        this.checkpoints = new HashMap<>();
        this.lock = new Object();
        this.asyncOperationsThreadPool = checkNotNull(asyncOperationsThreadPool);
        this.env = checkNotNull(env);
        this.asyncExceptionHandler = checkNotNull(asyncExceptionHandler);
        this.actionExecutor = checkNotNull(actionExecutor);
        this.channelStateWriter = checkNotNull(channelStateWriter);
        this.prepareInputSnapshot = prepareInputSnapshot;
        this.abortedCheckpointIds =
                createAbortedCheckpointSetWithLimitSize(maxRecordAbortedCheckpoints);
        this.maxRecordAbortedCheckpoints = maxRecordAbortedCheckpoints;
        this.lastCheckpointId = -1L;
        this.closed = false;
        this.enableCheckpointAfterTasksFinished = enableCheckpointAfterTasksFinished;
        this.registerTimer = registerTimer;
        this.clock = SystemClock.getInstance();
    }

    private static ChannelStateWriter openChannelStateWriter(
            String taskName,
            CheckpointStorage checkpointStorage,
            Environment env,
            int maxSubtasksPerChannelStateFile) {
        return new ChannelStateWriterImpl(
                env.getJobVertexId(),
                taskName,
                env.getTaskInfo().getIndexOfThisSubtask(),
                checkpointStorage,
                env.getChannelStateExecutorFactory(),
                maxSubtasksPerChannelStateFile);
    }

    @Override
    public void abortCheckpointOnBarrier(
            long checkpointId, CheckpointException cause, OperatorChain<?, ?> operatorChain)
            throws IOException {
        LOG.debug("Aborting checkpoint via cancel-barrier {} for task {}", checkpointId, taskName);
        lastCheckpointId = Math.max(lastCheckpointId, checkpointId);
        Iterator<Long> iterator = abortedCheckpointIds.iterator();
        while (iterator.hasNext()) {
            long next = iterator.next();
            if (next < lastCheckpointId) {
                iterator.remove();
            } else {
                break;
            }
        }

        checkpointStorage.clearCacheFor(checkpointId);

        channelStateWriter.abort(checkpointId, cause, true);

        // notify the coordinator that we decline this checkpoint
        env.declineCheckpoint(checkpointId, cause);

        actionExecutor.runThrowing(
                () -> {
                    if (checkpointId == alignmentCheckpointId) {
                        cancelAlignmentTimer();
                    }
                    // notify all downstream operators that they should not wait for a barrier from
                    // us and abort checkpoint.
                    operatorChain.abortCheckpoint(checkpointId, cause);
                    operatorChain.broadcastEvent(new CancelCheckpointMarker(checkpointId));
                });
    }

    private void cancelAlignmentTimer() {
        if (alignmentTimer == null) {
            return;
        }
        alignmentTimer.cancel();
        alignmentTimer = null;
    }

    @Override
    public CheckpointStorageWorkerView getCheckpointStorage() {
        return checkpointStorage;
    }

    @Override
    public ChannelStateWriter getChannelStateWriter() {
        return channelStateWriter;
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * checkpoint状态
     */
    @Override
    public void checkpointState(
            CheckpointMetaData metadata,
            CheckpointOptions options,
            CheckpointMetricsBuilder metrics,
            OperatorChain<?, ?> operatorChain,
            boolean isTaskFinished,
            Supplier<Boolean> isRunning)
            throws Exception {
        // 检查参数是否非空
        checkNotNull(options);
        checkNotNull(metrics);

        // All of the following steps happen as an atomic step from the perspective of barriers and
        // records/watermarks/timers/callbacks.
        // We generally try to emit the checkpoint barrier as soon as possible to not affect
        // downstream
        // checkpoint alignments
        // 从屏障和记录/水印/计时器/回调的角度来看，以下所有步骤都作为一个原子步骤发生。
        // 我们通常试图尽快发出检查点屏障，以免影响下游的检查点对齐。
        // 如果当前检查点的ID小于或等于上一个检查点的ID，则可能是乱序的检查点屏障（之前可能已中止？）
        /**
         * 判断检查点是否大于等于当前检查点，如果大于等于表示已经做过Checkpoint无需重做
         */
        if (lastCheckpointId >= metadata.getCheckpointId()) {
            LOG.info(
                    "Out of order checkpoint barrier (aborted previously?): {} >= {}",
                    lastCheckpointId,
                    metadata.getCheckpointId());
            // 中止该检查点ID对应的检查点，并抛出取消异常
            channelStateWriter.abort(metadata.getCheckpointId(), new CancellationException(), true);
            // 检查并清除已中止状态
            checkAndClearAbortedStatus(metadata.getCheckpointId());
            return;
        }
        // 记录检查点处理的延迟
        logCheckpointProcessingDelay(metadata);

        // Step (0): Record the last triggered checkpointId and abort the sync phase of checkpoint
        // if necessary.
        // todo 步骤 (0): 记录最后触发的检查点ID，并在必要时中止检查点的同步阶段
        lastCheckpointId = metadata.getCheckpointId();
        // 检查并清除已中止状态，如果已中止，则执行以下操作
        if (checkAndClearAbortedStatus(metadata.getCheckpointId())) {
            // broadcast cancel checkpoint marker to avoid downstream back-pressure due to
            // checkpoint barrier align.
            // 通过向操作符链广播取消检查点标记来避免由于检查点屏障对齐导致的下游背压
            operatorChain.broadcastEvent(new CancelCheckpointMarker(metadata.getCheckpointId()));
            // 中止该检查点ID对应的检查点，并抛出带有通知的取消异常
            channelStateWriter.abort(
                    metadata.getCheckpointId(),
                    new CancellationException("checkpoint aborted via notification"),
                    true);
            LOG.info(
                    "Checkpoint {} has been notified as aborted, would not trigger any checkpoint.",
                    metadata.getCheckpointId());
            return;
        }

        // if checkpoint has been previously unaligned, but was forced to be aligned (pointwise
        // connection), revert it here so that it can jump over output data
        // 如果检查点之前未对齐，但被强制对齐（点对点连接），则在这里恢复它，使其可以跳过输出数据
        if (options.getAlignment() == CheckpointOptions.AlignmentType.FORCED_ALIGNED) {
            // 将选项设置为支持非对齐检查点
            options = options.withUnalignedSupported();
            // 初始化输入检查点
            initInputsCheckpoint(metadata.getCheckpointId(), options);
        }

        // Step (1): Prepare the checkpoint, allow operators to do some pre-barrier work.
        //           The pre-barrier work should be nothing or minimal in the common case.
        // todo 步骤 (1): 准备检查点，允许操作符执行一些屏障前的工作。正常情况下不会调用，其实就是对应AbstractStreamOperator
        operatorChain.prepareSnapshotPreBarrier(metadata.getCheckpointId());

        // Step (2): Send the checkpoint barrier downstream
        // 步骤 (2): 向下游发送检查点屏障
        LOG.debug(
                "Task {} broadcastEvent at {}, triggerTime {}, passed time {}",
                taskName,
                System.currentTimeMillis(),
                metadata.getTimestamp(),
                System.currentTimeMillis() - metadata.getTimestamp());
        //核心点1.构造CheckpointBarrier
        CheckpointBarrier checkpointBarrier =
                new CheckpointBarrier(metadata.getCheckpointId(), metadata.getTimestamp(), options);
        //核心点2. 以广播形式发送检查点屏障，并根据选项确定是否为非对齐检查点(我们当前程序中相当于向Map发送checkpoint)
        operatorChain.broadcastEvent(checkpointBarrier, options.isUnalignedCheckpoint());

        // Step (3): Register alignment timer to timeout aligned barrier to unaligned barrier

        // 步骤 (3): 注册对齐计时器，以便在屏障对齐超时后将对齐屏障转换为非对齐屏障
//           （仅当检查点需要对齐时）
        registerAlignmentTimer(metadata.getCheckpointId(), operatorChain, checkpointBarrier);

        // Step (4): Prepare to spill the in-flight buffers for input and output

        // 步骤 (4): 准备将正在传输的输入和输出缓冲区溢出
        if (options.needsChannelState()) {
            // output data already written while broadcasting event
            // 在广播事件时可能已经写入了输出数据
            channelStateWriter.finishOutput(metadata.getCheckpointId());
        }

        // Step (5): Take the state snapshot. This should be largely asynchronous, to not impact
        // progress of the
        // streaming topology

        // 步骤 (5): 获取状态快照。这个过程应该大部分是异步的，以避免影响流处理拓扑的进度。

       // 创建一个映射，用于存储每个操作符的快照未来对象（OperatorSnapshotFutures）
       // 映射的键是操作符的ID，值是对应的快照未来对象
        Map<OperatorID, OperatorSnapshotFutures> snapshotFutures =
                CollectionUtil.newHashMapWithExpectedSize(operatorChain.getNumberOfOperators());
        try {
            /**
             * 数据源端口同步执行快照
             * 并执行成功了向JobMaster汇报Ack确认
             */
            if (takeSnapshotSync(
                    snapshotFutures, metadata, metrics, options, operatorChain, isRunning)) {
                // 如果所有操作符都成功完成了快照，则调用该方法来结束并报告检查点结果
                // 这里的结束和报告可能是异步的，即快照结果可能在后续某个时间点才完成并报告给协调器
                finishAndReportAsync(
                        snapshotFutures,
                        metadata,
                        metrics,
                        operatorChain.isTaskDeployedAsFinished(),
                        isTaskFinished,
                        isRunning);
            } else {
                // 如果快照捕获过程中有操作符拒绝或失败，则执行清理操作
                cleanup(snapshotFutures, metadata, metrics, new Exception("Checkpoint declined"));
            }
            // 如果在快照捕获过程中捕获到异常，则执行清理操作并重新抛出异常
        } catch (Exception ex) {
            cleanup(snapshotFutures, metadata, metrics, ex);
            throw ex;// 重新抛出异常，以便上层能够处理
        }
    }

    private void registerAlignmentTimer(
            long checkpointId,
            OperatorChain<?, ?> operatorChain,
            CheckpointBarrier checkpointBarrier) {
        // The timer isn't triggered when the checkpoint completes quickly, so cancel timer here.
        cancelAlignmentTimer();
        if (!checkpointBarrier.getCheckpointOptions().isTimeoutable()) {
            return;
        }

        long timerDelay = BarrierAlignmentUtil.getTimerDelay(clock, checkpointBarrier);

        alignmentTimer =
                registerTimer.registerTask(
                        () -> {
                            try {
                                operatorChain.alignedBarrierTimeout(checkpointId);
                            } catch (Exception e) {
                                ExceptionUtils.rethrowIOException(e);
                            }
                            alignmentTimer = null;
                            return null;
                        },
                        Duration.ofMillis(timerDelay));
        alignmentCheckpointId = checkpointId;
    }

    @Override
    public void notifyCheckpointComplete(
            long checkpointId, OperatorChain<?, ?> operatorChain, Supplier<Boolean> isRunning)
            throws Exception {

        notifyCheckpoint(
                checkpointId, operatorChain, isRunning, Task.NotifyCheckpointOperation.COMPLETE);
    }

    @Override
    public void notifyCheckpointAborted(
            long checkpointId, OperatorChain<?, ?> operatorChain, Supplier<Boolean> isRunning)
            throws Exception {

        notifyCheckpoint(
                checkpointId, operatorChain, isRunning, Task.NotifyCheckpointOperation.ABORT);
    }

    @Override
    public void notifyCheckpointSubsumed(
            long checkpointId, OperatorChain<?, ?> operatorChain, Supplier<Boolean> isRunning)
            throws Exception {

        notifyCheckpoint(
                checkpointId, operatorChain, isRunning, Task.NotifyCheckpointOperation.SUBSUME);
    }

    private void notifyCheckpoint(
            long checkpointId,
            OperatorChain<?, ?> operatorChain,
            Supplier<Boolean> isRunning,
            Task.NotifyCheckpointOperation notifyCheckpointOperation)
            throws Exception {

        Exception previousException = null;
        try {
            if (!isRunning.get()) {
                LOG.debug(
                        "Ignoring notification of checkpoint {} {} for not-running task {}",
                        notifyCheckpointOperation,
                        checkpointId,
                        taskName);
            } else {
                LOG.debug(
                        "Notification of checkpoint {} {} for task {}",
                        notifyCheckpointOperation,
                        checkpointId,
                        taskName);

                if (notifyCheckpointOperation.equals(Task.NotifyCheckpointOperation.ABORT)) {
                    boolean canceled = cancelAsyncCheckpointRunnable(checkpointId);

                    if (!canceled) {
                        if (checkpointId > lastCheckpointId) {
                            // only record checkpoints that have not triggered on task side.
                            abortedCheckpointIds.add(checkpointId);
                            maxAbortedCheckpointId = Math.max(maxAbortedCheckpointId, checkpointId);
                        }
                    }

                    channelStateWriter.abort(
                            checkpointId,
                            new CancellationException("checkpoint aborted via notification"),
                            false);
                }

                try {
                    switch (notifyCheckpointOperation) {
                        case ABORT:
                            operatorChain.notifyCheckpointAborted(checkpointId);
                            break;
                        case COMPLETE:
                            operatorChain.notifyCheckpointComplete(checkpointId);
                            break;
                        case SUBSUME:
                            operatorChain.notifyCheckpointSubsumed(checkpointId);
                    }
                } catch (Exception e) {
                    previousException = ExceptionUtils.firstOrSuppressed(e, previousException);
                }
            }
        } finally {
            try {
                switch (notifyCheckpointOperation) {
                    case ABORT:
                        env.getTaskStateManager().notifyCheckpointAborted(checkpointId);
                        break;
                    case COMPLETE:
                        env.getTaskStateManager().notifyCheckpointComplete(checkpointId);
                }
            } catch (Exception e) {
                previousException = ExceptionUtils.firstOrSuppressed(e, previousException);
            }
        }

        ExceptionUtils.tryRethrowException(previousException);
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 根据检查点选项初始化输入端的检查点流程。
     *
     * @param id           检查点的唯一标识符。
     * @param checkpointOptions 检查点选项，包含了检查点的类型和其他相关设置。
     */
    @Override
    public void initInputsCheckpoint(long id, CheckpointOptions checkpointOptions)
            throws CheckpointException {
        // 如果这是一个未对齐的检查点
        if (checkpointOptions.isUnalignedCheckpoint()) {
            // 启动通道状态写入器，为当前检查点ID和选项准备状态记录。
            channelStateWriter.start(id, checkpointOptions);
            // 准备处理中（in-flight）数据的快照。
            prepareInflightDataSnapshot(id);
            // 如果这是一个可超时的检查点
        } else if (checkpointOptions.isTimeoutable()) {
            // The output buffer may need to be snapshotted, so start the channelStateWriter here.
            // 启动通道状态写入器，因为输出缓冲区可能需要被快照记录。
            channelStateWriter.start(id, checkpointOptions);
            // 完成输入端的检查点准备工作。
            channelStateWriter.finishInput(id);
        }
    }

    public void waitForPendingCheckpoints() throws Exception {
        if (!enableCheckpointAfterTasksFinished) {
            return;
        }

        List<AsyncCheckpointRunnable> asyncCheckpointRunnables;
        synchronized (lock) {
            asyncCheckpointRunnables = new ArrayList<>(checkpoints.values());
        }

        // Waits for each checkpoint independently.
        asyncCheckpointRunnables.forEach(
                ar -> {
                    try {
                        ar.getFinishedFuture().get();
                    } catch (Exception e) {
                        LOG.debug(
                                "Async runnable for checkpoint "
                                        + ar.getCheckpointId()
                                        + " throws exception and exit",
                                e);
                    }
                });
    }

    @Override
    public void close() throws IOException {
        cancelAlignmentTimer();
        cancel();
    }

    public void cancel() throws IOException {
        List<AsyncCheckpointRunnable> asyncCheckpointRunnables = null;
        synchronized (lock) {
            if (!closed) {
                closed = true;
                asyncCheckpointRunnables = new ArrayList<>(checkpoints.values());
                checkpoints.clear();
            }
        }
        IOUtils.closeAllQuietly(asyncCheckpointRunnables);
        channelStateWriter.close();
    }

    @VisibleForTesting
    int getAsyncCheckpointRunnableSize() {
        synchronized (lock) {
            return checkpoints.size();
        }
    }

    @VisibleForTesting
    int getAbortedCheckpointSize() {
        return abortedCheckpointIds.size();
    }

    private boolean checkAndClearAbortedStatus(long checkpointId) {
        return abortedCheckpointIds.remove(checkpointId)
                || checkpointId + maxRecordAbortedCheckpoints < maxAbortedCheckpointId;
    }

    private void registerAsyncCheckpointRunnable(
            long checkpointId, AsyncCheckpointRunnable asyncCheckpointRunnable) throws IOException {
        synchronized (lock) {
            if (closed) {
                LOG.debug(
                        "Cannot register Closeable, this subtaskCheckpointCoordinator is already closed. Closing argument.");
                closeQuietly(asyncCheckpointRunnable);
                checkState(
                        !checkpoints.containsKey(checkpointId),
                        "SubtaskCheckpointCoordinator was closed without releasing asyncCheckpointRunnable for checkpoint %s",
                        checkpointId);
            } else if (checkpoints.containsKey(checkpointId)) {
                closeQuietly(asyncCheckpointRunnable);
                throw new IOException(
                        String.format(
                                "Cannot register Closeable, async checkpoint %d runnable has been register. Closing argument.",
                                checkpointId));
            } else {
                checkpoints.put(checkpointId, asyncCheckpointRunnable);
            }
        }
    }

    private boolean unregisterAsyncCheckpointRunnable(long checkpointId) {
        synchronized (lock) {
            return checkpoints.remove(checkpointId) != null;
        }
    }

    /**
     * Cancel the async checkpoint runnable with given checkpoint id. If given checkpoint id is not
     * registered, return false, otherwise return true.
     */
    private boolean cancelAsyncCheckpointRunnable(long checkpointId) {
        AsyncCheckpointRunnable asyncCheckpointRunnable;
        synchronized (lock) {
            asyncCheckpointRunnable = checkpoints.remove(checkpointId);
        }
        if (asyncCheckpointRunnable != null) {
            asyncOperationsThreadPool.execute(() -> closeQuietly(asyncCheckpointRunnable));
        }
        return asyncCheckpointRunnable != null;
    }

    private void cleanup(
            Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
            CheckpointMetaData metadata,
            CheckpointMetricsBuilder metrics,
            Exception ex) {

        channelStateWriter.abort(metadata.getCheckpointId(), ex, true);
        for (OperatorSnapshotFutures operatorSnapshotResult :
                operatorSnapshotsInProgress.values()) {
            if (operatorSnapshotResult != null) {
                try {
                    operatorSnapshotResult.cancel();
                } catch (Exception e) {
                    LOG.warn("Could not properly cancel an operator snapshot result.", e);
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "{} - did NOT finish synchronous part of checkpoint {}. Alignment duration: {} ms, snapshot duration {} ms",
                    taskName,
                    metadata.getCheckpointId(),
                    metrics.getAlignmentDurationNanosOrDefault() / 1_000_000,
                    metrics.getSyncDurationMillis());
        }
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 准备处理中（in-flight）数据的快照。
     *
     * @param checkpointId 检查点的唯一标识符。
     */
    private void prepareInflightDataSnapshot(long checkpointId) throws CheckpointException {
        // 使用一个函数式接口（可能是CompletableFuture的某个转换或执行操作）
        // 来应用prepareInputSnapshot操作，该操作涉及将channelStateWriter和checkpointId作为参数。
        prepareInputSnapshot
                .apply(channelStateWriter, checkpointId)
                // 当异步操作完成时，无论是正常完成还是异常完成，都会执行whenComplete中的lambda表达式。
                .whenComplete(
                        (unused, ex) -> {
                            // 如果异步操作因异常而完成
                            if (ex != null) {
                                // 调用channelStateWriter的abort方法，通知检查点失败。
                                channelStateWriter.abort(
                                        checkpointId,
                                        ex,
                                        false /* result is needed and cleaned by getWriteResult */);
                            } else {
                                // 如果异步操作成功完成
                                // 调用channelStateWriter的finishInput方法，标记输入端的检查点工作已完成。
                                channelStateWriter.finishInput(checkpointId);
                            }
                        });
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 异步完成并报告检查点。
     *
     * @param snapshotFutures    每个 OperatorID 的 OperatorSnapshotFutures 映射，包含操作员快照的未来结果
     * @param metadata           检查点元数据
     * @param metrics            检查点度量构建器
     * @param isTaskDeployedAsFinished 任务是否以已完成状态部署
     * @param isTaskFinished       任务是否已完成
     * @param isRunning            任务是否正在运行的提供者
     * @throws IOException 如果在异步检查点过程中发生I/O错误
    */
    private void finishAndReportAsync(
            Map<OperatorID, OperatorSnapshotFutures> snapshotFutures,
            CheckpointMetaData metadata,
            CheckpointMetricsBuilder metrics,
            boolean isTaskDeployedAsFinished,
            boolean isTaskFinished,
            Supplier<Boolean> isRunning)
            throws IOException {
        // 创建一个 AsyncCheckpointRunnable 对象，该对象将异步处理检查点逻辑
        AsyncCheckpointRunnable asyncCheckpointRunnable =
                new AsyncCheckpointRunnable(
                        snapshotFutures,// 传递操作员快照的未来结果
                        metadata,// 传递检查点元数据
                        metrics,// 传递检查点度量构建器
                        System.nanoTime(),// 获取当前系统纳秒时间戳
                        taskName,// 假设 taskName 是类的成员变量，表示任务名称
                        unregisterConsumer(),// 注销消费者
                        env,//执行环境
                        asyncExceptionHandler,// 异步异常处理器
                        isTaskDeployedAsFinished,// 传递任务是否以已完成状态部署的标志
                        isTaskFinished,// 传递任务是否已完成的标志
                        isRunning); // 传递任务是否正在运行的提供者
        // 传递任务是否正在运行的提供者
        registerAsyncCheckpointRunnable(
                asyncCheckpointRunnable.getCheckpointId(), asyncCheckpointRunnable);

        // we are transferring ownership over snapshotInProgressList for cleanup to the thread,
        // active on submit
        // 将 AsyncCheckpointRunnable 提交到异步操作线程池执行，
        // 提交后，snapshotInProgressList（假设它是类的某个成员变量）的所有权将转移给执行该任务的线程，
        // 用于后续的清理工作
        asyncOperationsThreadPool.execute(asyncCheckpointRunnable);
    }

    private Consumer<AsyncCheckpointRunnable> unregisterConsumer() {
        return asyncCheckpointRunnable ->
                unregisterAsyncCheckpointRunnable(asyncCheckpointRunnable.getCheckpointId());
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 同步执行检查点快照操作。
     *
     * @param operatorSnapshotsInProgress 当前正在进行的操作员快照
     * @param checkpointMetaData 检查点的元数据
     * @param checkpointMetrics 检查点度量构建器
     * @param checkpointOptions 检查点选项
     * @param operatorChain 操作员链
     * @param isRunning 判断任务是否正在运行的供应器
     * @return 操作是否成功
     * @throws Exception 在执行过程中可能抛出的任何异常
    */
    private boolean takeSnapshotSync(
            Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
            CheckpointMetaData checkpointMetaData,
            CheckpointMetricsBuilder checkpointMetrics,
            CheckpointOptions checkpointOptions,
            OperatorChain<?, ?> operatorChain,
            Supplier<Boolean> isRunning)
            throws Exception {
        // 检查操作员链是否已关闭，如果已关闭则抛出异常
        checkState(
                !operatorChain.isClosed(),
                "OperatorChain and Task should never be closed at this point");
        // 获取检查点ID
        long checkpointId = checkpointMetaData.getCheckpointId();
        // 记录操作开始的时间（纳秒）
        long started = System.nanoTime();
        // 根据检查点选项，确定是否需要获取通道状态，并据此获取通道状态写入结果
        ChannelStateWriteResult channelStateWriteResult =
                checkpointOptions.needsChannelState()
                        ? channelStateWriter.getAndRemoveWriteResult(checkpointId)
                        : ChannelStateWriteResult.EMPTY;
        // 解析检查点存储位置，获取检查点流工厂
        CheckpointStreamFactory storage =
                checkpointStorage.resolveCheckpointStorageLocation(
                        checkpointId, checkpointOptions.getTargetLocation());
        // 应用文件合并检查点策略
        storage = applyFileMergingCheckpoint(storage, checkpointOptions);

        try {
            // 对operatorChain链进行快照状态记录
            operatorChain.snapshotState(
                    operatorSnapshotsInProgress,
                    checkpointMetaData,
                    checkpointOptions,
                    isRunning,
                    channelStateWriteResult,
                    storage);

        } finally {
            // 清理检查点ID对应的缓存
            checkpointStorage.clearCacheFor(checkpointId);
        }
        // 记录调试日志，包括任务名称、检查点ID、对齐持续时间、快照持续时间和是否未对齐检查点等信息
        LOG.debug(
                "{} - finished synchronous part of checkpoint {}. Alignment duration: {} ms, snapshot duration {} ms, is unaligned checkpoint : {}",
                taskName,
                checkpointId,
                checkpointMetrics.getAlignmentDurationNanosOrDefault() / 1_000_000,
                checkpointMetrics.getSyncDurationMillis(),
                checkpointOptions.isUnalignedCheckpoint());
        //设置计算时间
        checkpointMetrics.setSyncDurationMillis((System.nanoTime() - started) / 1_000_000);
        //返回true
        return true;
    }

    private CheckpointStreamFactory applyFileMergingCheckpoint(
            CheckpointStreamFactory storage, CheckpointOptions checkpointOptions) {
        if (storage instanceof FsMergingCheckpointStorageLocation
                && checkpointOptions.getCheckpointType().isSavepoint()) {
            // fall back to non-fileMerging if it is a savepoint
            return ((FsMergingCheckpointStorageLocation) storage).toNonFileMerging();
        } else {
            return storage;
        }
    }

    private Set<Long> createAbortedCheckpointSetWithLimitSize(int maxRecordAbortedCheckpoints) {
        return Collections.newSetFromMap(
                new LinkedHashMap<Long, Boolean>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    protected boolean removeEldestEntry(Map.Entry<Long, Boolean> eldest) {
                        return size() > maxRecordAbortedCheckpoints;
                    }
                });
    }

    // Caches checkpoint output stream factories to prevent multiple output stream per checkpoint.
    // This could result from requesting output stream by different entities (this and
    // channelStateWriter)
    // We can't just pass a stream to the channelStateWriter because it can receive checkpoint call
    // earlier than this class
    // in some unaligned checkpoints scenarios
    private static class CachingCheckpointStorageWorkerView implements CheckpointStorageWorkerView {
        private final Map<Long, CheckpointStreamFactory> cache = new ConcurrentHashMap<>();
        private final CheckpointStorageWorkerView delegate;

        private CachingCheckpointStorageWorkerView(CheckpointStorageWorkerView delegate) {
            this.delegate = delegate;
        }

        void clearCacheFor(long checkpointId) {
            cache.remove(checkpointId);
        }

        @Override
        public CheckpointStreamFactory resolveCheckpointStorageLocation(
                long checkpointId, CheckpointStorageLocationReference reference) {
            return cache.computeIfAbsent(
                    checkpointId,
                    id -> {
                        try {
                            return delegate.resolveCheckpointStorageLocation(
                                    checkpointId, reference);
                        } catch (IOException e) {
                            throw new FlinkRuntimeException(e);
                        }
                    });
        }

        @Override
        public CheckpointStateOutputStream createTaskOwnedStateStream() throws IOException {
            return delegate.createTaskOwnedStateStream();
        }

        @Override
        public CheckpointStateToolset createTaskOwnedCheckpointStateToolset() {
            return delegate.createTaskOwnedCheckpointStateToolset();
        }
    }

    private static void logCheckpointProcessingDelay(CheckpointMetaData checkpointMetaData) {
        long delay = System.currentTimeMillis() - checkpointMetaData.getReceiveTimestamp();
        if (delay >= CHECKPOINT_EXECUTION_DELAY_LOG_THRESHOLD_MS) {
            LOG.warn(
                    "Time from receiving all checkpoint barriers/RPC to executing it exceeded threshold: {}ms",
                    delay);
        }
    }
}
