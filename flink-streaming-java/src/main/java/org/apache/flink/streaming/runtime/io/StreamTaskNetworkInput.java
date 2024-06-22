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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.tasks.StreamTask.CanEmitBatchOfRecordsChecker;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

/**
 * Implementation of {@link StreamTaskInput} that wraps an input from network taken from {@link
 * CheckpointedInputGate}.
 *
 * <p>This internally uses a {@link StatusWatermarkValve} to keep track of {@link Watermark} and
 * {@link WatermarkStatus} events, and forwards them to event subscribers once the {@link
 * StatusWatermarkValve} determines the {@link Watermark} from all inputs has advanced, or that a
 * {@link WatermarkStatus} needs to be propagated downstream to denote a status change.
 *
 * <p>Forwarding elements, watermarks, or status elements must be protected by synchronizing on the
 * given lock object. This ensures that we don't call methods on a {@link StreamInputProcessor}
 * concurrently with the timer callback or other things.
 */
@Internal
public final class StreamTaskNetworkInput<T>
        extends AbstractStreamTaskNetworkInput<
                T,
                SpillingAdaptiveSpanningRecordDeserializer<
                        DeserializationDelegate<StreamElement>>> {

    public StreamTaskNetworkInput(
            CheckpointedInputGate checkpointedInputGate,
            TypeSerializer<T> inputSerializer,
            IOManager ioManager,
            StatusWatermarkValve statusWatermarkValve,
            int inputIndex,
            CanEmitBatchOfRecordsChecker canEmitBatchOfRecords) {
        super(
                checkpointedInputGate,
                inputSerializer,
                statusWatermarkValve,
                inputIndex,
                getRecordDeserializers(checkpointedInputGate, ioManager),
                canEmitBatchOfRecords);
    }

    // Initialize one deserializer per input channel
    private static Map<
                    InputChannelInfo,
                    SpillingAdaptiveSpanningRecordDeserializer<
                            DeserializationDelegate<StreamElement>>>
            getRecordDeserializers(
                    CheckpointedInputGate checkpointedInputGate, IOManager ioManager) {
        return checkpointedInputGate.getChannelInfos().stream()
                .collect(
                        toMap(
                                identity(),
                                unused ->
                                        new SpillingAdaptiveSpanningRecordDeserializer<>(
                                                ioManager.getSpillingDirectoriesPaths())));
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 用于准备快照
     * @param channelStateWriter 通道状态写入器，用于将状态写入到某个地方（如持久化存储）
     * @param checkpointId 检查点的ID，通常用于标识和追踪不同的快照
    */
    @Override
    public CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {

        // 遍历记录反序列化器的映射表
        for (Map.Entry<
                        InputChannelInfo,
                        SpillingAdaptiveSpanningRecordDeserializer<
                                DeserializationDelegate<StreamElement>>>
                e : recordDeserializers.entrySet()) {

            try {
                // 调用channelStateWriter的addInputData方法，将未消费的数据添加到快照中
                channelStateWriter.addInputData(
                        checkpointId,
                        e.getKey(),// 输入通道信息
                        ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
                        e.getValue().getUnconsumedBuffer());// 获取反序列化器中未消费的缓冲区
            } catch (IOException ioException) {
                throw new CheckpointException(CheckpointFailureReason.IO_EXCEPTION, ioException);
            }
        }
        return checkpointedInputGate.getAllBarriersReceivedFuture(checkpointId);
    }

    @Override
    public void close() throws IOException {
        super.close();

        // cleanup the resources of the checkpointed input gate
        checkpointedInputGate.close();
    }
}
