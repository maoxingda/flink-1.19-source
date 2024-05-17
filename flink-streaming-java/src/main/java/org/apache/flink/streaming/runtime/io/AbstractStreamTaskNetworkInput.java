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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.EndOfChannelStateEvent;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.tasks.StreamTask.CanEmitBatchOfRecordsChecker;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base class for network-based StreamTaskInput where each channel has a designated {@link
 * RecordDeserializer} for spanning records. Specific implementation bind it to a specific {@link
 * RecordDeserializer}.
 */
public abstract class AbstractStreamTaskNetworkInput<
                T, R extends RecordDeserializer<DeserializationDelegate<StreamElement>>>
        implements StreamTaskInput<T> {
    protected final CheckpointedInputGate checkpointedInputGate;
    protected final DeserializationDelegate<StreamElement> deserializationDelegate;
    protected final TypeSerializer<T> inputSerializer;
    protected final Map<InputChannelInfo, R> recordDeserializers;
    protected final Map<InputChannelInfo, Integer> flattenedChannelIndices = new HashMap<>();
    /** Valve that controls how watermarks and watermark statuses are forwarded. */
    protected final StatusWatermarkValve statusWatermarkValve;

    protected final int inputIndex;
    private final RecordAttributesCombiner recordAttributesCombiner;
    private InputChannelInfo lastChannel = null;
    private R currentRecordDeserializer = null;

    protected final CanEmitBatchOfRecordsChecker canEmitBatchOfRecords;

    public AbstractStreamTaskNetworkInput(
            CheckpointedInputGate checkpointedInputGate,
            TypeSerializer<T> inputSerializer,
            StatusWatermarkValve statusWatermarkValve,
            int inputIndex,
            Map<InputChannelInfo, R> recordDeserializers,
            CanEmitBatchOfRecordsChecker canEmitBatchOfRecords) {
        super();
        this.checkpointedInputGate = checkpointedInputGate;
        deserializationDelegate =
                new NonReusingDeserializationDelegate<>(
                        new StreamElementSerializer<>(inputSerializer));
        this.inputSerializer = inputSerializer;

        for (InputChannelInfo i : checkpointedInputGate.getChannelInfos()) {
            flattenedChannelIndices.put(i, flattenedChannelIndices.size());
        }

        this.statusWatermarkValve = checkNotNull(statusWatermarkValve);
        this.inputIndex = inputIndex;
        this.recordDeserializers = checkNotNull(recordDeserializers);
        this.canEmitBatchOfRecords = checkNotNull(canEmitBatchOfRecords);
        this.recordAttributesCombiner =
                new RecordAttributesCombiner(checkpointedInputGate.getNumberOfInputChannels());
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 获取数据并消费
    */
    @Override
    public DataInputStatus emitNext(DataOutput<T> output) throws Exception {
        // while循环
        while (true) {
            // get the stream element from the deserializer
            // 从反序列化器中获取流元素
            if (currentRecordDeserializer != null) {
                RecordDeserializer.DeserializationResult result;
                try {
                    // 尝试从反序列化委托中获取下一个记录
                    result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
                } catch (IOException e) {
                    // 如果无法获取记录，则抛出IO异常
                    throw new IOException(
                            String.format("Can't get next record for channel %s", lastChannel), e);
                }
                // 如果缓冲区已经被消耗完毕
                if (result.isBufferConsumed()) {
                    currentRecordDeserializer = null;
                }
                // 如果获取到完整的记录
                if (result.isFullRecord()) {
                    //处理数据
                    final boolean breakBatchEmitting =
                            processElement(deserializationDelegate.getInstance(), output);
                    if (canEmitBatchOfRecords.check() && !breakBatchEmitting) {
                        continue;
                    }
                    // 表明还有更多可用的输入
                    return DataInputStatus.MORE_AVAILABLE;
                }
            }
            // 从checkpointedInputGate中轮询下一个Buffer或事件
            Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();
            if (bufferOrEvent.isPresent()) {
                // return to the mailbox after receiving a checkpoint barrier to avoid processing of
                // data after the barrier before checkpoint is performed for unaligned checkpoint
                // mode
                // 如果轮询到的是Buffer
                if (bufferOrEvent.get().isBuffer()) {
                    // 处理Buffer
                    processBuffer(bufferOrEvent.get());
                } else {
                    // 如果轮询到的是数据处理状态
                    DataInputStatus status = processEvent(bufferOrEvent.get());
                    // 如果事件处理后表明还有更多可用的输入，并且可以emit
                    if (status == DataInputStatus.MORE_AVAILABLE && canEmitBatchOfRecords.check()) {
                        //继续处理
                        continue;
                    }
                    // 返回处理事件后的状态
                    return status;
                }
            } else {
                // 如果没有更多的数据或事件
                if (checkpointedInputGate.isFinished()) {
                    // 检查是否已完成并且Future已完成
                    checkState(
                            checkpointedInputGate.getAvailableFuture().isDone(),
                            "Finished BarrierHandler should be available");
                    // 表明输入已经结束
                    return DataInputStatus.END_OF_INPUT;
                }
                // 表明当前没有可用的数据或事件
                return DataInputStatus.NOTHING_AVAILABLE;
            }
        }
    }

    /**
     * Process the given stream element and returns whether to stop processing and return from the
     * emitNext method so that the emitNext is invoked again right after processing the element to
     * allow behavior change in emitNext method. For example, the behavior of emitNext may need to
     * change right after process a RecordAttributes.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 处理给定的流数据，并返回一个布尔值表示是否停止处理并从emitNext方法中返回，
     * @param streamElement 要处理的流元素
     * @param output        用于发射处理结果的数据输出
     * @return              如果处理完RecordAttributes后需要改变emitNext的行为，则返回true；否则返回false
     * @throws Exception    处理过程中出现异常时抛出
    */
    private boolean processElement(StreamElement streamElement, DataOutput<T> output)
            throws Exception {
        // 如果流元素是记录类型
        if (streamElement.isRecord()) {
            // DataOutput发送给HeadOperator
            output.emitRecord(streamElement.asRecord());
            // 不需要改变emitNext的行为，返回false
            return false;
            // 如果元素是水印类型
        } else if (streamElement.isWatermark()) {
            // 获取水印信息
            // 获取最后一个通道的索引
            statusWatermarkValve.inputWatermark(
                    streamElement.asWatermark(), flattenedChannelIndices.get(lastChannel), output);
            // 不需要改变emitNext的行为，返回false
            return false;
            // 如果元素是延迟标记类型
        } else if (streamElement.isLatencyMarker()) {
            //延迟发射
            output.emitLatencyMarker(streamElement.asLatencyMarker());
            // 不需要改变emitNext的行为，返回false
            return false;
        } else if (streamElement.isWatermarkStatus()) {
            // 如果流元素是水印状态类型
            statusWatermarkValve.inputWatermarkStatus(
                    streamElement.asWatermarkStatus(),
                    flattenedChannelIndices.get(lastChannel),
                    output);
            // 不需要改变emitNext的行为，返回false
            return false;
            // 如果元素是记录属性类型
        } else if (streamElement.isRecordAttributes()) {
            recordAttributesCombiner.inputRecordAttributes(
                    streamElement.asRecordAttributes(),
                    flattenedChannelIndices.get(lastChannel),
                    output);
            // 不需要改变emitNext的行为，返回false
            return true;
        } else {
            //抛出异常
            throw new UnsupportedOperationException("Unknown type of StreamElement");
        }
    }

    protected DataInputStatus processEvent(BufferOrEvent bufferOrEvent) {
        // Event received
        final AbstractEvent event = bufferOrEvent.getEvent();
        if (event.getClass() == EndOfData.class) {
            switch (checkpointedInputGate.hasReceivedEndOfData()) {
                case NOT_END_OF_DATA:
                    // skip
                    break;
                case DRAINED:
                    return DataInputStatus.END_OF_DATA;
                case STOPPED:
                    return DataInputStatus.STOPPED;
            }
        } else if (event.getClass() == EndOfPartitionEvent.class) {
            // release the record deserializer immediately,
            // which is very valuable in case of bounded stream
            releaseDeserializer(bufferOrEvent.getChannelInfo());
            if (checkpointedInputGate.isFinished()) {
                return DataInputStatus.END_OF_INPUT;
            }
        } else if (event.getClass() == EndOfChannelStateEvent.class) {
            if (checkpointedInputGate.allChannelsRecovered()) {
                return DataInputStatus.END_OF_RECOVERY;
            }
        }
        return DataInputStatus.MORE_AVAILABLE;
    }

    protected void processBuffer(BufferOrEvent bufferOrEvent) throws IOException {
        lastChannel = bufferOrEvent.getChannelInfo();
        checkState(lastChannel != null);
        currentRecordDeserializer = getActiveSerializer(bufferOrEvent.getChannelInfo());
        checkState(
                currentRecordDeserializer != null,
                "currentRecordDeserializer has already been released");

        currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
    }

    protected R getActiveSerializer(InputChannelInfo channelInfo) {
        return recordDeserializers.get(channelInfo);
    }

    @Override
    public int getInputIndex() {
        return inputIndex;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        if (currentRecordDeserializer != null) {
            return AVAILABLE;
        }
        return checkpointedInputGate.getAvailableFuture();
    }

    @Override
    public void close() throws IOException {
        // release the deserializers . this part should not ever fail
        for (InputChannelInfo channelInfo : new ArrayList<>(recordDeserializers.keySet())) {
            releaseDeserializer(channelInfo);
        }
    }

    protected void releaseDeserializer(InputChannelInfo channelInfo) {
        R deserializer = recordDeserializers.get(channelInfo);
        if (deserializer != null) {
            // recycle buffers and clear the deserializer.
            deserializer.clear();
            recordDeserializers.remove(channelInfo);
        }
    }
}
