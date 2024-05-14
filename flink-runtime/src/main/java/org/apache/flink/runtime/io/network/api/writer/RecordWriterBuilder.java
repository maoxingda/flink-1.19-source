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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;

/** Utility class to encapsulate the logic of building a {@link RecordWriter} instance. */
public class RecordWriterBuilder<T extends IOReadableWritable> {

    private ChannelSelector<T> selector = new RoundRobinChannelSelector<>();

    private long timeout = -1;

    private String taskName = "test";

    public RecordWriterBuilder<T> setChannelSelector(ChannelSelector<T> selector) {
        this.selector = selector;
        return this;
    }

    public RecordWriterBuilder<T> setTimeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    public RecordWriterBuilder<T> setTaskName(String taskName) {
        this.taskName = taskName;
        return this;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 根据提供的 ResultPartitionWriter 构建一个 RecordWriter 对象。
     * RecordWriter 用于将数据写入 ResultPartitionWriter。
     * @param writer ResultPartitionWriter 对象，用于实际写入数据
     * @return 返回一个 RecordWriter 对象，用于将数据写入给定的 ResultPartitionWriter
    */
    public RecordWriter<T> build(ResultPartitionWriter writer) {
        // 检查选择器是否配置为广播模式
        if (selector.isBroadcast()) {
            // 如果是广播模式，则使用 BroadcastRecordWriter 来广播数据到所有接收者
            // BroadcastRecordWriter 会将数据复制到所有分区
            return new BroadcastRecordWriter<>(writer, timeout, taskName);
        } else {
            // 如果不是广播模式，则使用 ChannelSelectorRecordWriter 来根据选择器策略将数据写入特定分区
            // ChannelSelectorRecordWriter 会根据选择器的配置来决定数据写入哪个分区
            return new ChannelSelectorRecordWriter<>(writer, selector, timeout, taskName);
        }
    }
}
