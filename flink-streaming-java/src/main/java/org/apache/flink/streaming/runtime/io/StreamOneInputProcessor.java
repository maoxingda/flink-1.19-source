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
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.OneInputStreamTask}.
 *
 * @param <IN> The type of the record that can be read with this record reader.
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 输入读取器，用于 OneInputStreamTask。
*/
@Internal
public final class StreamOneInputProcessor<IN> implements StreamInputProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(StreamOneInputProcessor.class);
    /** 基本数据输出接口，用于从数据输入中发出下一个元素。从StreamOneInputProcessor中读取数据。 */
    private StreamTaskInput<IN> input;
    /** */
    private DataOutput<IN> output;
    /** 针对需要被通知输入的逻辑/语义结束的多输入操作符的接口。 */
    private final BoundedMultiInput endOfInputAware;

    public StreamOneInputProcessor(
            StreamTaskInput<IN> input, DataOutput<IN> output, BoundedMultiInput endOfInputAware) {

        this.input = checkNotNull(input);
        this.output = checkNotNull(output);
        this.endOfInputAware = checkNotNull(endOfInputAware);
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return input.getAvailableFuture();
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 处理输入的数据
    */
    @Override
    public DataInputStatus processInput() throws Exception {
        // 调用input的emitNext方法，尝试从输入源中获取下一个数据片段并发送到output
        DataInputStatus status = input.emitNext(output);
        // 检查返回的状态是否为数据的结束
        if (status == DataInputStatus.END_OF_DATA) {
            // 如果是数据的结束，则调用endOfInputAware的endInput方法，传入当前输入索引加1作为参数
            // 这里的输入索引可能用于追踪输入数据的位置或顺序
            endOfInputAware.endInput(input.getInputIndex() + 1);
            // 创建一个新的FinishedDataOutput对象，表示输出已经完成
            output = new FinishedDataOutput<>();
        } else if (status == DataInputStatus.END_OF_RECOVERY) {
            // 如果是恢复结束，并且input是RecoverableStreamTaskInput的实例
            if (input instanceof RecoverableStreamTaskInput) {
                // 调用finishRecovery方法，结束当前的恢复流程，并可能返回一个新的输入源
                input = ((RecoverableStreamTaskInput<IN>) input).finishRecovery();
            }
            // 通知调用者还有更多数据可用
            return DataInputStatus.MORE_AVAILABLE;
        }
        // 如果不是上述两种状态，直接返回当前的状态
        return status;
    }

    @Override
    public CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
        return input.prepareSnapshot(channelStateWriter, checkpointId);
    }

    @Override
    public void close() throws IOException {
        input.close();
    }
}
