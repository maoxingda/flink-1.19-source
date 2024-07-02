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

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A controller for keeping track of channels state in {@link AbstractAlignedBarrierHandlerState}
 * and {@link AbstractAlternatingAlignedBarrierHandlerState}.
 */
final class ChannelState {
    private final Map<InputChannelInfo, Integer> sequenceNumberInAnnouncedChannels =
            new HashMap<>();

    /**
     * {@link #blockedChannels} are the ones for which we have already processed {@link
     * CheckpointBarrier}. {@link #sequenceNumberInAnnouncedChannels} on the other hand, are the
     * ones that we have processed announcement but not yet a barrier.
     */
    private final Set<InputChannelInfo> blockedChannels = new HashSet<>();

    private final CheckpointableInput[] inputs;

    public ChannelState(CheckpointableInput[] inputs) {
        this.inputs = inputs;
    }

    public void blockChannel(InputChannelInfo channelInfo) {
        inputs[channelInfo.getGateIdx()].blockConsumption(channelInfo);
        blockedChannels.add(channelInfo);
    }

    public void channelFinished(InputChannelInfo channelInfo) {
        blockedChannels.remove(channelInfo);
        sequenceNumberInAnnouncedChannels.remove(channelInfo);
    }

    public void prioritizeAllAnnouncements() throws IOException {
        for (Map.Entry<InputChannelInfo, Integer> announcedNumberInChannel :
                sequenceNumberInAnnouncedChannels.entrySet()) {
            InputChannelInfo channelInfo = announcedNumberInChannel.getKey();
            inputs[channelInfo.getGateIdx()].convertToPriorityEvent(
                    channelInfo.getInputChannelIdx(), announcedNumberInChannel.getValue());
        }
        sequenceNumberInAnnouncedChannels.clear();
    }
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 解锁所有被阻塞的通道。
     *
     * @throws IOException 如果在解锁通道过程中发生I/O错误
     */
    public void unblockAllChannels() throws IOException {
        // 遍历所有被阻塞的通道信息
        for (InputChannelInfo blockedChannel : blockedChannels) {
            // 调用对应输入通道（由gateIdx索引）的resumeConsumption方法，传入被阻塞的通道信息
            // 此方法用于恢复通道的消费能力，即允许其继续接收或处理数据
            inputs[blockedChannel.getGateIdx()].resumeConsumption(blockedChannel);
        }
        // 清空阻塞通道列表，表示这些通道已经不再处于阻塞状态
        blockedChannels.clear();
    }

    public CheckpointableInput[] getInputs() {
        return inputs;
    }

    public void addSeenAnnouncement(InputChannelInfo channelInfo, int sequenceNumber) {
        this.sequenceNumberInAnnouncedChannels.put(channelInfo, sequenceNumber);
    }

    public void removeSeenAnnouncement(InputChannelInfo channelInfo) {
        this.sequenceNumberInAnnouncedChannels.remove(channelInfo);
    }

    public ChannelState emptyState() {
        checkState(
                blockedChannels.isEmpty(),
                "We should not reset to an empty state if there are blocked channels: %s",
                blockedChannels);
        sequenceNumberInAnnouncedChannels.clear();
        return this;
    }
}
