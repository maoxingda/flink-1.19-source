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

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * When the parallelism of both upstream and downstream is {@link
 * ExecutionConfig#PARALLELISM_DEFAULT} and the edge's partitioner is not specified
 * (partitioner==null), the edge's partitioner will be set to FORWARD by default(See {@link
 * StreamGraph#createActualEdge} method for details). When using the AdaptiveBatchScheduler, this
 * will result in the parallelism of many job vertices is not calculated based on the amount of data
 * but has to align with the parallelism of their upstream vertices due to forward edges, which is
 * contrary to the original intention of the AdaptiveBatchScheduler.
 *
 * <p>To solve it, we introduce the {@link ForwardForUnspecifiedPartitioner}. This partitioner will
 * be set for unspecified edges(partitioner==null), and then the runtime framework will change it to
 * FORWARD/RESCALE after the operator chain creation:
 *
 * <p>1. Convert to {@link ForwardPartitioner} if the partitioner is intra-chain.
 *
 * <p>2. Convert to {@link RescalePartitioner} if the partitioner is inter-chain.
 *
 * <p>This partitioner should only be used when using AdaptiveBatchScheduler.
 *
 * @param <T> Type of the elements in the Stream
 */
/**
  * @授课老师(V): yi_locus
  * email: 156184212@qq.com
  * 当上游和下游的平行度为ExecutionConfig#PARALLISM_DEFAULT,
 * 并且未指定边缘的partitioner（partitioner==null），
 * 则默认情况下，边缘的partitioner将设置为FORWARD。
 * 当使用AdaptiveBatchScheduler时，这将导致许多作业顶点的平行度不是基于数据量计算的，
 * 而是由于前向边缘而必须与其上游顶点的平行性对齐，这与AdaptiveBatch调度器的初衷背道而驰。
  */
@Internal
public class ForwardForUnspecifiedPartitioner<T> extends ForwardPartitioner<T> {

    @Override
    public StreamPartitioner<T> copy() {
        throw new RuntimeException(
                "ForwardForUnspecifiedPartitioner is a intermediate partitioner in optimization phase, "
                        + "should be converted to a ForwardPartitioner/RescalePartitioner.");
    }

    @Override
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        throw new RuntimeException(
                "ForwardForUnspecifiedPartitioner is a intermediate partitioner in optimization phase, "
                        + "should be converted to a ForwardPartitioner/RescalePartitioner.");
    }

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
        throw new RuntimeException(
                "ForwardForUnspecifiedPartitioner is a intermediate partitioner in optimization phase, "
                        + "should be converted to a ForwardPartitioner/RescalePartitioner.");
    }
}
