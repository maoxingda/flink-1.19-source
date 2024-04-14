/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.graph.StreamGraph;

/** The data exchange mode between operators during {@link StreamGraph} generation. */
/**
  * @授课老师(V): yi_locus
  * email: 156184212@qq.com
  * StreamGraph构建期间期间确定StreamOperator之间的数据交换模式
  */
@Internal
public enum StreamExchangeMode {
    /**
     * Producer and consumer are online at the same time. Produced data is received by consumer
     * immediately.
     */
    /** 生产者和消费者同时在线。生成的数据会立即被消费者接收。 */
    PIPELINED,

    /**
     * The producer first produces its entire result and finishes. After that, the consumer is
     * started and may consume the data.
     */
    /** 生产者首先产生其全部结果并完成。之后，消费者被启动，并且可以消费数据。 */
    BATCH,

    /**
     * The consumer can start consuming data anytime as long as the producer has started producing.
     *
     * <p>This exchange mode is re-consumable.
     */
    /** 只要生产者已经开始生产，消费者就可以随时开始消费数据。 */
    HYBRID_FULL,

    /**
     * The consumer can start consuming data anytime as long as the producer has started producing.
     *
     * <p>This exchange mode is not re-consumable.
     */
    /** 只要生产者已经开始生产，消费者就可以随时开始消费数据。 */
    HYBRID_SELECTIVE,

    /**
     * The exchange mode is undefined. It leaves it up to the framework to decide the exchange mode.
     * The framework will pick one of {@link StreamExchangeMode#BATCH} or {@link
     * StreamExchangeMode#PIPELINED} in the end.
     */
    /**
     * 交换模式未定义。会通过框架来决定交换模式。框架将选择 StreamExchangeMode#BATCH、StreamExchangeMode#PIPELINED。
     */
    UNDEFINED
}
