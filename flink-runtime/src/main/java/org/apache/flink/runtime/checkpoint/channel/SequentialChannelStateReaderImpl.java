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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.logger.NetworkActionsLogger;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.state.AbstractChannelStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

/** {@link SequentialChannelStateReader} implementation. */
public class SequentialChannelStateReaderImpl implements SequentialChannelStateReader {

    private final TaskStateSnapshot taskStateSnapshot;
    private final ChannelStateSerializer serializer;
    private final ChannelStateChunkReader chunkReader;

    public SequentialChannelStateReaderImpl(TaskStateSnapshot taskStateSnapshot) {
        this.taskStateSnapshot = taskStateSnapshot;
        serializer = new ChannelStateSerializerImpl();
        chunkReader = new ChannelStateChunkReader(serializer);
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 从InputGate中恢复或初始化数据状态。
     */
    @Override
    public void readInputData(InputGate[] inputGates) throws IOException, InterruptedException {
        // 使用try-with-resources语句自动管理资源，确保InputChannelRecoveredStateHandler在使用后能够被正确关闭
        try (InputChannelRecoveredStateHandler stateHandler =
                new InputChannelRecoveredStateHandler(
                        inputGates, taskStateSnapshot.getInputRescalingDescriptor())) {
            // 读取状态信息
            read(
                    stateHandler,
                    groupByDelegate(
                            streamSubtaskStates(), OperatorSubtaskState::getInputChannelState));
        }
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 读取输出数据并处理ResultPartitionWriter数组。
     */
    @Override
    public void readOutputData(ResultPartitionWriter[] writers, boolean notifyAndBlockOnCompletion)
            throws IOException, InterruptedException {
        // 使用try-with-resources语句自动管理ResultSubpartitionRecoveredStateHandler的生命周期
        // 这个处理器用于恢复分区状态和读取数据
        try (ResultSubpartitionRecoveredStateHandler stateHandler =
                new ResultSubpartitionRecoveredStateHandler(
                        writers,
                        notifyAndBlockOnCompletion,
                        taskStateSnapshot.getOutputRescalingDescriptor())) {
            // 调用read方法，传入状态处理器和通过groupByDelegate处理的结果子任务状态
            read(
                    stateHandler,
                    groupByDelegate(
                            streamSubtaskStates(),
                            OperatorSubtaskState::getResultSubpartitionState));
        }
    }
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 读取并处理从各个流状态句柄中恢复的状态信息。
     */
    private <Info, Context, Handle extends AbstractChannelStateHandle<Info>> void read(
            RecoveredChannelStateHandler<Info, Context> stateHandler,
            Map<StreamStateHandle, List<Handle>> streamStateHandleListMap)
            throws IOException, InterruptedException {
        // 遍历streamStateHandleListMap中的每个条目
        for (Map.Entry<StreamStateHandle, List<Handle>> delegateAndHandles :
                streamStateHandleListMap.entrySet()) {
            // 对于每个条目，使用其键（StreamStateHandle）和值（Handle列表）调用readSequentially方法
            readSequentially(
                    delegateAndHandles.getKey(), delegateAndHandles.getValue(), stateHandler);
        }
    }

    private <Info, Context, Handle extends AbstractChannelStateHandle<Info>> void readSequentially(
            StreamStateHandle streamStateHandle,
            List<Handle> channelStateHandles,
            RecoveredChannelStateHandler<Info, Context> stateHandler)
            throws IOException, InterruptedException {
        try (FSDataInputStream is = streamStateHandle.openInputStream()) {
            serializer.readHeader(is);
            for (RescaledOffset<Info> offsetAndChannelInfo :
                    extractOffsetsSorted(channelStateHandles)) {
                chunkReader.readChunk(
                        is,
                        offsetAndChannelInfo.offset,
                        stateHandler,
                        offsetAndChannelInfo.channelInfo,
                        offsetAndChannelInfo.oldSubtaskIndex);
            }
        }
    }

    private Stream<OperatorSubtaskState> streamSubtaskStates() {
        return taskStateSnapshot.getSubtaskStateMappings().stream().map(Map.Entry::getValue);
    }

    private static <Info, Handle extends AbstractChannelStateHandle<Info>>
            Map<StreamStateHandle, List<Handle>> groupByDelegate(
                    Stream<OperatorSubtaskState> states,
                    Function<OperatorSubtaskState, StateObjectCollection<Handle>>
                            stateHandleExtractor) {
        return states.map(stateHandleExtractor)
                .flatMap(Collection::stream)
                .peek(validate())
                .collect(groupingBy(AbstractChannelStateHandle::getDelegate));
    }

    private static <Info, Handle extends AbstractChannelStateHandle<Info>>
            Consumer<Handle> validate() {
        Set<Tuple2<Info, Integer>> seen = new HashSet<>();
        // expect each channel/subtask to be described only once; otherwise, buffers in channel
        // could be
        // re-ordered
        return handle -> {
            if (!seen.add(new Tuple2<>(handle.getInfo(), handle.getSubtaskIndex()))) {
                throw new IllegalStateException("Duplicate channel info: " + handle);
            }
        };
    }

    private static <Info, Handle extends AbstractChannelStateHandle<Info>>
            List<RescaledOffset<Info>> extractOffsetsSorted(List<Handle> channelStateHandles) {
        return channelStateHandles.stream()
                .flatMap(SequentialChannelStateReaderImpl::extractOffsets)
                .sorted(comparingLong(offsetAndInfo -> offsetAndInfo.offset))
                .collect(toList());
    }

    private static <Info, Handle extends AbstractChannelStateHandle<Info>>
            Stream<RescaledOffset<Info>> extractOffsets(Handle handle) {
        return handle.getOffsets().stream()
                .map(
                        offset ->
                                new RescaledOffset<>(
                                        offset, handle.getInfo(), handle.getSubtaskIndex()));
    }

    @Override
    public void close() throws Exception {}

    static class RescaledOffset<Info> {
        final Long offset;
        final Info channelInfo;
        final int oldSubtaskIndex;

        RescaledOffset(Long offset, Info channelInfo, int oldSubtaskIndex) {
            this.offset = offset;
            this.channelInfo = channelInfo;
            this.oldSubtaskIndex = oldSubtaskIndex;
        }
    }
}

class ChannelStateChunkReader {
    private final ChannelStateSerializer serializer;

    ChannelStateChunkReader(ChannelStateSerializer serializer) {
        this.serializer = serializer;
    }

    <Info, Context> void readChunk(
            FSDataInputStream source,
            long sourceOffset,
            RecoveredChannelStateHandler<Info, Context> stateHandler,
            Info channelInfo,
            int oldSubtaskIndex)
            throws IOException, InterruptedException {
        if (source.getPos() != sourceOffset) {
            source.seek(sourceOffset);
        }
        int length = serializer.readLength(source);
        while (length > 0) {
            RecoveredChannelStateHandler.BufferWithContext<Context> bufferWithContext =
                    stateHandler.getBuffer(channelInfo);
            try (Closeable ignored =
                    NetworkActionsLogger.measureIO(
                            "ChannelStateChunkReader#readChunk", bufferWithContext.buffer)) {
                while (length > 0 && bufferWithContext.buffer.isWritable()) {
                    length -= serializer.readData(source, bufferWithContext.buffer, length);
                }
            } catch (Exception e) {
                bufferWithContext.close();
                throw e;
            }

            // Passing the ownership of buffer to inside.
            stateHandler.recover(channelInfo, oldSubtaskIndex, bufferWithContext);
        }
    }
}
