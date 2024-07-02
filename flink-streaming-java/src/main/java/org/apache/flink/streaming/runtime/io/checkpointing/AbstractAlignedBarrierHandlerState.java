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

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/** Actions to be taken when processing aligned checkpoints. */
/**
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * Actions to be taken when processing aligned checkpoints
 */
abstract class AbstractAlignedBarrierHandlerState implements BarrierHandlerState {

    protected final ChannelState state;

    protected AbstractAlignedBarrierHandlerState(ChannelState state) {
        this.state = state;
    }

    @Override
    public final BarrierHandlerState alignedCheckpointTimeout(
            Controller controller, CheckpointBarrier checkpointBarrier)
            throws IOException, CheckpointException {
        throw new IllegalStateException(
                "Alignment should not be timed out if we are not alternating.");
    }

    @Override
    public final BarrierHandlerState announcementReceived(
            Controller controller, InputChannelInfo channelInfo, int sequenceNumber) {
        return this;
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 当接收到检查点屏障时调用的方法。
     *
     * @param controller 控制器对象，用于管理检查点流程
     * @param channelInfo 输入通道信息，包含通道的相关属性
     * @param checkpointBarrier 检查点屏障对象，包含检查点的相关信息
     * @param markChannelBlocked 是否标记通道为阻塞状态
     * @return BarrierHandlerState 表示处理检查点屏障后的状态
     * @throws IOException 如果在处理过程中发生I/O错误
     * @throws CheckpointException 如果在处理检查点过程中发生异常
     */
    @Override
    public final BarrierHandlerState barrierReceived(
            Controller controller,
            InputChannelInfo channelInfo,
            CheckpointBarrier checkpointBarrier,
            boolean markChannelBlocked)
            throws IOException, CheckpointException {
        // 检查检查点选项，确保不是非对齐的检查点
        checkState(!checkpointBarrier.getCheckpointOptions().isUnalignedCheckpoint());
        // 如果需要标记通道为阻塞状态
        if (markChannelBlocked) {
            // 阻塞指定的通道
            state.blockChannel(channelInfo);
        }
        // 如果所有屏障都已接收
        if (controller.allBarriersReceived()) {
            // 触发全局检查点
            return triggerGlobalCheckpoint(controller, checkpointBarrier);
        }
        // 如果未触发全局检查点，则返回处理屏障后的状态
        return convertAfterBarrierReceived(state);
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 触发全局检查点。
     *
     * @param controller 控制器对象，用于管理检查点流程
     * @param checkpointBarrier 检查点屏障对象，包含检查点的相关信息
     * @return WaitingForFirstBarrier 对象，表示等待第一个屏障的状态
     * @throws IOException 如果在触发全局检查点过程中发生I/O错误
     */
    protected WaitingForFirstBarrier triggerGlobalCheckpoint(
            Controller controller, CheckpointBarrier checkpointBarrier) throws IOException {
        // 触发全局检查点，通知所有相关的组件进行状态快照等操作
        controller.triggerGlobalCheckpoint(checkpointBarrier);
        // 取消对所有输入通道的阻塞状态
        state.unblockAllChannels();
        // 创建一个新的 WaitingForFirstBarrier 对象，表示现在处于等待第一个屏障的状态
        return new WaitingForFirstBarrier(state.getInputs());
    }

    protected abstract BarrierHandlerState convertAfterBarrierReceived(ChannelState state);

    @Override
    public final BarrierHandlerState abort(long cancelledId) throws IOException {
        state.unblockAllChannels();
        return new WaitingForFirstBarrier(state.getInputs());
    }
}
