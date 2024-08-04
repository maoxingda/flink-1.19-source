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

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;

import java.io.IOException;

/**
 * We either timed out before seeing any barriers or started unaligned. We might've seen some
 * announcements if we started aligned.
 */
final class AlternatingWaitingForFirstBarrierUnaligned implements BarrierHandlerState {

    private final boolean alternating;
    private final ChannelState channelState;

    AlternatingWaitingForFirstBarrierUnaligned(boolean alternating, ChannelState channelState) {
        this.alternating = alternating;
        this.channelState = channelState;
    }

    @Override
    public BarrierHandlerState alignedCheckpointTimeout(
            Controller controller, CheckpointBarrier checkpointBarrier) {
        // ignore already processing unaligned checkpoints
        return this;
    }

    @Override
    public BarrierHandlerState announcementReceived(
            Controller controller, InputChannelInfo channelInfo, int sequenceNumber)
            throws IOException {
        channelState.getInputs()[channelInfo.getGateIdx()].convertToPriorityEvent(
                channelInfo.getInputChannelIdx(), sequenceNumber);
        return this;
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 处理接收到的检查点屏障（Checkpoint Barrier）的函数。
     *
     * @param controller        控制器，用于管理检查点流程。
     * @param channelInfo       接收屏障的输入通道信息。
     * @param checkpointBarrier 接收到的检查点屏障。
     * @param markChannelBlocked 标记是否需要将该通道标记为阻塞状态，通常与基于信用的网络流量控制相关。
     * @return 处理后的状态，可能表示检查点完成或需要继续等待其他屏障。
     */
    @Override
    public BarrierHandlerState barrierReceived(
            Controller controller,
            InputChannelInfo channelInfo,
            CheckpointBarrier checkpointBarrier,
            boolean markChannelBlocked)
            throws CheckpointException, IOException {

        // we received an out of order aligned barrier, we should book keep this channel as blocked,
        // as it is being blocked by the credit-based network
        // 如果需要标记通道为阻塞，并且该屏障不是未对齐的检查点屏障，则将该通道标记为阻塞。
        // 这通常是因为基于信用的网络控制需要阻塞该通道以等待资源。
        if (markChannelBlocked
                && !checkpointBarrier.getCheckpointOptions().isUnalignedCheckpoint()) {
            channelState.blockChannel(channelInfo);
        }
        // 将接收到的检查点屏障转换为未对齐的屏障（如果原本就是未对齐的，则直接返回自身）。
        CheckpointBarrier unalignedBarrier = checkpointBarrier.asUnaligned();
        // 初始化控制器的输入检查点流程，使用转换后的未对齐屏障。
        controller.initInputsCheckpoint(unalignedBarrier);
        // 通知所有输入检查点开始，使用未对齐的屏障。
        for (CheckpointableInput input : channelState.getInputs()) {
            input.checkpointStarted(unalignedBarrier);
        }
        // 触发全局检查点流程，使用未对齐的屏障。
        controller.triggerGlobalCheckpoint(unalignedBarrier);
        // 如果所有必要的检查点屏障都已接收，执行检查点停止逻辑。
        if (controller.allBarriersReceived()) {
            // 通知所有输入检查点结束，使用未对齐屏障的ID。
            for (CheckpointableInput input : channelState.getInputs()) {
                input.checkpointStopped(unalignedBarrier.getId());
            }
            // 执行停止检查点的后续逻辑，并返回处理完成的状态。
            return stopCheckpoint();
        }
        // 如果还没有接收到所有必要的检查点屏障，则返回一个表示需要继续交替收集未对齐屏障的状态。
        return new AlternatingCollectingBarriersUnaligned(alternating, channelState);
    }

    @Override
    public BarrierHandlerState abort(long cancelledId) throws IOException {
        return stopCheckpoint();
    }

    @Override
    public BarrierHandlerState endOfPartitionReceived(
            Controller controller, InputChannelInfo channelInfo)
            throws IOException, CheckpointException {
        channelState.channelFinished(channelInfo);

        // Do nothing since we have no pending checkpoint.
        return this;
    }

    private BarrierHandlerState stopCheckpoint() throws IOException {
        channelState.unblockAllChannels();
        if (alternating) {
            return new AlternatingWaitingForFirstBarrier(channelState.emptyState());
        } else {
            return new AlternatingWaitingForFirstBarrierUnaligned(false, channelState.emptyState());
        }
    }
}
