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

package org.apache.flink.streaming.runtime.partitioner;

/**
 * Interface for {@link StreamPartitioner} which have to be configured with the maximum parallelism
 * of the stream transformation. The configure method is called by the StreamGraph when adding
 * internal edges.
 *
 * <p>This interface is required since the stream partitioners are instantiated eagerly. Due to that
 * the maximum parallelism might not have been determined and needs to be set at a stage when the
 * maximum parallelism could have been determined.
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * StreamPartitioner 的接口，该接口需要与流转换的最大并行度进行配置。当在流图中添加内部边时，configure 方法会被调用。
 * 这里的“流转换”可能指的是在流处理框架（如 Apache Flink）中数据流的各种转换操作（如 map、filter、reduce 等）。而“最大并行度”指的是在分布式系统中，可以并行执行这些转换操作的最大任务数。StreamPartitioner 可能是用于确定如何将数据分区到这些并行任务中的一个组件。
 * 在流图中添加内部边时，框架会调用 configure 方法来确保 StreamPartitioner 被正确配置，以便根据流的最大并行度来执行其分区逻辑。
*/
public interface ConfigurableStreamPartitioner {

    /**
     * Configure the {@link StreamPartitioner} with the maximum parallelism of the down stream
     * operator.
     *
     * @param maxParallelism Maximum parallelism of the down stream operator.
     */
    void configure(int maxParallelism);
}
