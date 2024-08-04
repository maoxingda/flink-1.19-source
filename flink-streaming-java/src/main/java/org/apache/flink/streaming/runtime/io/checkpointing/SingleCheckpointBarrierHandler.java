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

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointableTask;
import org.apache.flink.streaming.runtime.io.checkpointing.BarrierAlignmentUtil.Cancellable;
import org.apache.flink.streaming.runtime.io.checkpointing.BarrierAlignmentUtil.DelayableTimer;
import org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinator;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.FunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_DECLINED_INPUT_END_OF_STREAM;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SingleCheckpointBarrierHandler} is used for triggering checkpoint while reading the first
 * barrier and keeping track of the number of received barriers and consumed barriers. It can
 * handle/track just single checkpoint at a time. The behaviour when to actually trigger the
 * checkpoint and what the {@link CheckpointableInput} should do is controlled by {@link
 * BarrierHandlerState}.
 */
@Internal
@NotThreadSafe
public class SingleCheckpointBarrierHandler extends CheckpointBarrierHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SingleCheckpointBarrierHandler.class);

    private final String taskName;
    private final ControllerImpl context;
    private final DelayableTimer registerTimer;
    private final SubtaskCheckpointCoordinator subTaskCheckpointCoordinator;
    private final CheckpointableInput[] inputs;

    /**
     * The checkpoint id to guarantee that we would trigger only one checkpoint when reading the
     * same barrier from different channels.
     */
    private long currentCheckpointId = -1L;

    /**
     * The checkpoint barrier of the current pending checkpoint. It is to allow us to access the
     * checkpoint options when processing {@code EndOfPartitionEvent}.
     */
    @Nullable private CheckpointBarrier pendingCheckpointBarrier;

    private final Set<InputChannelInfo> alignedChannels = new HashSet<>();

    private int targetChannelCount;

    private long lastCancelledOrCompletedCheckpointId = -1L;

    private int numOpenChannels;

    private CompletableFuture<Void> allBarriersReceivedFuture = new CompletableFuture<>();

    private BarrierHandlerState currentState;
    private Cancellable currentAlignmentTimer;
    private final boolean alternating;

    @VisibleForTesting
    public static SingleCheckpointBarrierHandler createUnalignedCheckpointBarrierHandler(
            SubtaskCheckpointCoordinator checkpointCoordinator,
            String taskName,
            CheckpointableTask toNotifyOnCheckpoint,
            Clock clock,
            boolean enableCheckpointsAfterTasksFinish,
            CheckpointableInput... inputs) {
        return unaligned(
                taskName,
                toNotifyOnCheckpoint,
                checkpointCoordinator,
                clock,
                (int)
                        Arrays.stream(inputs)
                                .flatMap(gate -> gate.getChannelInfos().stream())
                                .count(),
                (callable, duration) -> {
                    throw new IllegalStateException(
                            "Strictly unaligned checkpoints should never register any callbacks");
                },
                enableCheckpointsAfterTasksFinish,
                inputs);
    }

    public static SingleCheckpointBarrierHandler unaligned(
            String taskName,
            CheckpointableTask toNotifyOnCheckpoint,
            SubtaskCheckpointCoordinator checkpointCoordinator,
            Clock clock,
            int numOpenChannels,
            DelayableTimer registerTimer,
            boolean enableCheckpointAfterTasksFinished,
            CheckpointableInput... inputs) {
        return new SingleCheckpointBarrierHandler(
                taskName,
                toNotifyOnCheckpoint,
                checkpointCoordinator,
                clock,
                numOpenChannels,
                new AlternatingWaitingForFirstBarrierUnaligned(false, new ChannelState(inputs)),
                false,
                registerTimer,
                inputs,
                enableCheckpointAfterTasksFinished);
    }

    public static SingleCheckpointBarrierHandler aligned(
            String taskName,
            CheckpointableTask toNotifyOnCheckpoint,
            Clock clock,
            int numOpenChannels,
            DelayableTimer registerTimer,
            boolean enableCheckpointAfterTasksFinished,
            CheckpointableInput... inputs) {
        return new SingleCheckpointBarrierHandler(
                taskName,
                toNotifyOnCheckpoint,
                null,
                clock,
                numOpenChannels,
                new WaitingForFirstBarrier(inputs),
                false,
                registerTimer,
                inputs,
                enableCheckpointAfterTasksFinished);
    }

    public static SingleCheckpointBarrierHandler alternating(
            String taskName,
            CheckpointableTask toNotifyOnCheckpoint,
            SubtaskCheckpointCoordinator checkpointCoordinator,
            Clock clock,
            int numOpenChannels,
            DelayableTimer registerTimer,
            boolean enableCheckpointAfterTasksFinished,
            CheckpointableInput... inputs) {
        return new SingleCheckpointBarrierHandler(
                taskName,
                toNotifyOnCheckpoint,
                checkpointCoordinator,
                clock,
                numOpenChannels,
                new AlternatingWaitingForFirstBarrier(new ChannelState(inputs)),
                true,
                registerTimer,
                inputs,
                enableCheckpointAfterTasksFinished);
    }

    private SingleCheckpointBarrierHandler(
            String taskName,
            CheckpointableTask toNotifyOnCheckpoint,
            @Nullable SubtaskCheckpointCoordinator subTaskCheckpointCoordinator,
            Clock clock,
            int numOpenChannels,
            BarrierHandlerState currentState,
            boolean alternating,
            DelayableTimer registerTimer,
            CheckpointableInput[] inputs,
            boolean enableCheckpointAfterTasksFinished) {
        super(toNotifyOnCheckpoint, clock, enableCheckpointAfterTasksFinished);

        this.taskName = taskName;
        this.numOpenChannels = numOpenChannels;
        this.currentState = currentState;
        this.alternating = alternating;
        this.registerTimer = registerTimer;
        this.subTaskCheckpointCoordinator = subTaskCheckpointCoordinator;
        this.context = new ControllerImpl();
        this.inputs = inputs;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 处理检查点屏障（CheckpointBarrier）。
     *
     * @param barrier 检查点屏障对象
     * @param channelInfo 输入通道信息
     * @param isRpcTriggered 是否由RPC触发（远程过程调用）
     * @throws IOException 如果在处理过程中发生I/O异常
    */
    @Override
    public void processBarrier(
            CheckpointBarrier barrier, InputChannelInfo channelInfo, boolean isRpcTriggered)
            throws IOException {
        // 获取检查点屏障的ID
        long barrierId = barrier.getId();
        // 记录日志，显示从哪个通道接收到了哪个ID的检查点屏障
        LOG.debug("{}: Received barrier from channel {} @ {}.", taskName, channelInfo, barrierId);

        // 如果当前检查点ID大于接收到的屏障ID，或者两者相等但当前没有挂起的检查点
        if (currentCheckpointId > barrierId
                || (currentCheckpointId == barrierId && !isCheckpointPending())) {
            // 如果屏障不是非对齐的检查点（unaligned checkpoint）
            if (!barrier.getCheckpointOptions().isUnalignedCheckpoint()) {
                // 恢复该通道的输入消费
                inputs[channelInfo.getGateIdx()].resumeConsumption(channelInfo);
            }
            // 处理完成，直接返回
            return;
        }
        // 检查新的检查点是否合法
        checkNewCheckpoint(barrier);
        // 断言当前检查点ID应该与屏障ID相等
        checkState(currentCheckpointId == barrierId);
        // 标记检查点已对齐，并转换状态
        markCheckpointAlignedAndTransformState(
                channelInfo,
                barrier,
                state -> state.barrierReceived(context, channelInfo, barrier, !isRpcTriggered));
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 标记检查点已对齐并转换状态。
     *
     * @param alignedChannel 对齐的输入通道信息
     * @param barrier 检查点屏障对象
     * @param stateTransformer 状态转换函数，用于更新当前状态
     * @throws IOException 如果在状态转换过程中发生I/O异常
    */
    protected void markCheckpointAlignedAndTransformState(
            InputChannelInfo alignedChannel,
            CheckpointBarrier barrier,
            FunctionWithException<BarrierHandlerState, BarrierHandlerState, Exception>
                    stateTransformer)
            throws IOException {
        // 将对齐的通道添加到对齐通道列表中
        /**
         * alignedChannels集合表示当前接收到的CheckpointBarrier消息的上游的InputChannelInfo信息
         * targetChannelCount代表Task需要接收CheckpointBarrier消息的所有channel数量。
         */
        alignedChannels.add(alignedChannel);
        // 如果这是第一个对齐的通道
        if (alignedChannels.size() == 1) {
            // 如果目标通道数量只有一个 那表示已经对齐了所以直接设置对齐开始并结束
            if (targetChannelCount == 1) {
                // 标记对齐开始和结束（因为只有一个通道，所以开始和结束是同时的）
                markAlignmentStartAndEnd(barrier.getId(), barrier.getTimestamp());
            } else {
                // 标记对齐开始如果目标数量是多个则表示需要等待对齐则触发开始对齐标志
                markAlignmentStart(barrier.getId(), barrier.getTimestamp());
            }
        }

        // we must mark alignment end before calling currentState.barrierReceived which might
        // trigger a checkpoint with unfinished future for alignment duration
        // 在调用currentState.barrierReceived之前，我们必须先标记对齐结束
        // 因为这可能会触发一个包含未完成的对齐持续时间的检查点
        if (alignedChannels.size() == targetChannelCount) {
            // 如果目标通道数量大于1
            if (targetChannelCount > 1) {
                // 标记对齐结束
                markAlignmentEnd();
            }
        }

        try {
            // 使用提供的状态转换函数更新当前状态
            currentState = stateTransformer.apply(currentState);
        } catch (CheckpointException e) {
            // 如果状态转换过程中抛出CheckpointException异常，则内部中止当前检查点
            abortInternal(currentCheckpointId, e);
        } catch (Exception e) {
            // 如果状态转换过程中抛出其他异常，则将其转换为IOException并重新抛出
            ExceptionUtils.rethrowIOException(e);
        }
        // 如果所有通道都已对齐
        if (alignedChannels.size() == targetChannelCount) {
            // 清空对齐通道列表
            alignedChannels.clear();
            // 更新最后取消或完成的检查点ID
            lastCancelledOrCompletedCheckpointId = currentCheckpointId;
            // 记录日志，表示所有通道都已对齐，用于检查点ID
            LOG.debug(
                    "{}: All the channels are aligned for checkpoint {}.",
                    taskName,
                    currentCheckpointId);
            // 重置对齐计时器
            resetAlignmentTimer();
            // 标记所有屏障都已接收完成
            allBarriersReceivedFuture.complete(null);
        }
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 触发检查点。
     *
     * @param trigger 触发检查点的检查点屏障对象
     * @throws IOException 如果在触发检查点过程中发生I/O错误
     */
    private void triggerCheckpoint(CheckpointBarrier trigger) throws IOException {
        // 使用日志记录器（LOG）记录触发检查点的相关信息
        // 包括任务名称（taskName）、检查点ID（trigger.getId()）和触发时间戳（trigger.getTimestamp()）
        LOG.debug(
                "{}: Triggering checkpoint {} on the barrier announcement at {}.",
                taskName,
                trigger.getId(),
                trigger.getTimestamp());
        // 调用 notifyCheckpoint 方法来通知相关组件或系统执行检查点操作
        notifyCheckpoint(trigger);
    }

    @Override
    public void processBarrierAnnouncement(
            CheckpointBarrier announcedBarrier, int sequenceNumber, InputChannelInfo channelInfo)
            throws IOException {
        checkNewCheckpoint(announcedBarrier);

        long barrierId = announcedBarrier.getId();
        if (currentCheckpointId > barrierId
                || (currentCheckpointId == barrierId && !isCheckpointPending())) {
            LOG.debug(
                    "{}: Obsolete announcement of checkpoint {} for channel {}.",
                    taskName,
                    barrierId,
                    channelInfo);
            return;
        }

        currentState = currentState.announcementReceived(context, channelInfo, sequenceNumber);
    }

    private void registerAlignmentTimer(CheckpointBarrier announcedBarrier) {
        long timerDelay = BarrierAlignmentUtil.getTimerDelay(getClock(), announcedBarrier);

        this.currentAlignmentTimer =
                registerTimer.registerTask(
                        () -> {
                            long barrierId = announcedBarrier.getId();
                            try {
                                if (currentCheckpointId == barrierId
                                        && !getAllBarriersReceivedFuture(barrierId).isDone()) {
                                    currentState =
                                            currentState.alignedCheckpointTimeout(
                                                    context, announcedBarrier);
                                }
                            } catch (CheckpointException ex) {
                                this.abortInternal(barrierId, ex);
                            } catch (Exception e) {
                                ExceptionUtils.rethrowIOException(e);
                            }
                            currentAlignmentTimer = null;
                            return null;
                        },
                        Duration.ofMillis(timerDelay));
    }

    private void checkNewCheckpoint(CheckpointBarrier barrier) throws IOException {
        long barrierId = barrier.getId();
        if (currentCheckpointId >= barrierId) {
            return; // This barrier is not the first for this checkpoint.
        }

        if (isCheckpointPending()) {
            cancelSubsumedCheckpoint(barrierId);
        }
        currentCheckpointId = barrierId;
        pendingCheckpointBarrier = barrier;
        alignedChannels.clear();
        targetChannelCount = numOpenChannels;
        allBarriersReceivedFuture = new CompletableFuture<>();

        if (alternating && barrier.getCheckpointOptions().isTimeoutable()) {
            registerAlignmentTimer(barrier);
        }
    }

    @Override
    public void processCancellationBarrier(
            CancelCheckpointMarker cancelBarrier, InputChannelInfo channelInfo) throws IOException {
        final long cancelledId = cancelBarrier.getCheckpointId();
        if (cancelledId > currentCheckpointId
                || (cancelledId == currentCheckpointId && alignedChannels.size() > 0)) {
            LOG.debug("{}: Received cancellation {}.", taskName, cancelledId);
            abortInternal(
                    cancelledId,
                    new CheckpointException(
                            CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER));
        }
    }

    private void abortInternal(long cancelledId, CheckpointFailureReason reason)
            throws IOException {
        abortInternal(cancelledId, new CheckpointException(reason));
    }

    private void abortInternal(long cancelledId, CheckpointException exception) throws IOException {
        LOG.debug(
                "{}: Aborting checkpoint {} after exception {}.",
                taskName,
                currentCheckpointId,
                exception);
        // by setting the currentCheckpointId to this checkpoint while keeping the numBarriers
        // at zero means that no checkpoint barrier can start a new alignment
        currentCheckpointId = Math.max(cancelledId, currentCheckpointId);
        lastCancelledOrCompletedCheckpointId =
                Math.max(lastCancelledOrCompletedCheckpointId, cancelledId);
        pendingCheckpointBarrier = null;
        alignedChannels.clear();
        targetChannelCount = 0;
        resetAlignmentTimer();
        currentState = currentState.abort(cancelledId);
        if (cancelledId == currentCheckpointId) {
            resetAlignment();
        }
        notifyAbort(cancelledId, exception);
        allBarriersReceivedFuture.completeExceptionally(exception);
    }

    private void resetAlignmentTimer() {
        if (currentAlignmentTimer != null) {
            currentAlignmentTimer.cancel();
            currentAlignmentTimer = null;
        }
    }

    @Override
    public void processEndOfPartition(InputChannelInfo channelInfo) throws IOException {
        numOpenChannels--;

        if (!isCheckpointAfterTasksFinishedEnabled()) {
            if (isCheckpointPending()) {
                LOG.warn(
                        "{}: Received EndOfPartition(-1) before completing current checkpoint {}. Skipping current checkpoint.",
                        taskName,
                        currentCheckpointId);
                abortInternal(currentCheckpointId, CHECKPOINT_DECLINED_INPUT_END_OF_STREAM);
            }
        } else {
            if (!isCheckpointPending()) {
                return;
            }

            checkState(
                    pendingCheckpointBarrier != null,
                    "pending checkpoint barrier should not be null when"
                            + " there is pending checkpoint.");

            markCheckpointAlignedAndTransformState(
                    channelInfo,
                    pendingCheckpointBarrier,
                    state -> state.endOfPartitionReceived(context, channelInfo));
        }
    }

    @Override
    public long getLatestCheckpointId() {
        return currentCheckpointId;
    }

    @Override
    public void close() throws IOException {
        resetAlignmentTimer();
        allBarriersReceivedFuture.cancel(false);
        super.close();
    }

    @Override
    protected boolean isCheckpointPending() {
        return currentCheckpointId != lastCancelledOrCompletedCheckpointId
                && currentCheckpointId >= 0;
    }

    private void cancelSubsumedCheckpoint(long barrierId) throws IOException {
        LOG.warn(
                "{}: Received checkpoint barrier for checkpoint {} before completing current checkpoint {}. "
                        + "Skipping current checkpoint.",
                taskName,
                barrierId,
                currentCheckpointId);
        abortInternal(currentCheckpointId, CHECKPOINT_DECLINED_SUBSUMED);
    }

    public CompletableFuture<Void> getAllBarriersReceivedFuture(long checkpointId) {
        if (checkpointId < currentCheckpointId || numOpenChannels == 0) {
            return FutureUtils.completedVoidFuture();
        }
        if (checkpointId > currentCheckpointId) {
            throw new IllegalStateException(
                    "Checkpoint " + checkpointId + " has not been started at all");
        }
        return allBarriersReceivedFuture;
    }

    @VisibleForTesting
    int getNumOpenChannels() {
        return numOpenChannels;
    }

    @Override
    public String toString() {
        return String.format(
                "%s: current checkpoint: %d, current aligned channels: %d, target channel count: %d",
                taskName, currentCheckpointId, alignedChannels.size(), targetChannelCount);
    }

    private final class ControllerImpl implements BarrierHandlerState.Controller {
        /**
         * @授课老师: 码界探索
         * @微信: 252810631
         * @版权所有: 请尊重劳动成果
         * 触发检查点
         */
        @Override
        public void triggerGlobalCheckpoint(CheckpointBarrier checkpointBarrier)
                throws IOException {
            SingleCheckpointBarrierHandler.this.triggerCheckpoint(checkpointBarrier);
        }

        @Override
        public boolean isTimedOut(CheckpointBarrier barrier) {
            return barrier.getCheckpointOptions().isTimeoutable()
                    && barrier.getId() <= currentCheckpointId
                    && barrier.getCheckpointOptions().getAlignedCheckpointTimeout()
                            < (getClock().absoluteTimeMillis() - barrier.getTimestamp());
        }

        @Override
        public boolean allBarriersReceived() {
            return alignedChannels.size() == targetChannelCount;
        }

        @Nullable
        @Override
        public CheckpointBarrier getPendingCheckpointBarrier() {
            return pendingCheckpointBarrier;
        }

        /**
         * @授课老师: 码界探索
         * @微信: 252810631
         * @版权所有: 请尊重劳动成果
         * 初始化输入检查点流程。
         *
         * @param checkpointBarrier 接收到的检查点屏障，包含了检查点的ID和选项。
         */
        @Override
        public void initInputsCheckpoint(CheckpointBarrier checkpointBarrier)
                throws CheckpointException {
            // 检查子任务检查点协调器是否已初始化，确保后续操作不会因空引用而失败。
            checkState(subTaskCheckpointCoordinator != null);
            // 获取检查点屏障的ID，这是唯一标识该检查点的关键信息。
            long barrierId = checkpointBarrier.getId();
            // 调用子任务检查点协调器的initInputsCheckpoint方法，
            // 传入检查点屏障的ID和检查点选项，以初始化输入端的检查点流程。
            // 这一步是准备阶段，为后续处理（如快照生成、状态同步等）做准备。
            subTaskCheckpointCoordinator.initInputsCheckpoint(
                    barrierId, checkpointBarrier.getCheckpointOptions());
        }
    }
}
