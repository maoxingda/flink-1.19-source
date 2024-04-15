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

package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.graph.SimpleTransformationTranslator;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link TransformationTranslator} for the {@link PartitionTransformation}.
 *
 * @param <OUT> The type of the elements that result from the {@code PartitionTransformation} being
 *     translated.
 */
@Internal
public class PartitionTransformationTranslator<OUT>
        extends SimpleTransformationTranslator<OUT, PartitionTransformation<OUT>> {

    @Override
    protected Collection<Integer> translateForBatchInternal(
            final PartitionTransformation<OUT> transformation, final Context context) {
        return translateInternal(transformation, context, true);
    }

    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * 内部方法调用translateInternal，对PartitionTransformation进行转换
      */
    @Override
    protected Collection<Integer> translateForStreamingInternal(
            final PartitionTransformation<OUT> transformation, final Context context) {
        return translateInternal(transformation, context, false);
    }

    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * translateInternal的私有方法，它主要用于处理流图（StreamGraph）中的分区转换（PartitionTransformation）。
     * @transformation 当前transformation
     * @context 环境对象
     * supportsBatchExchange 是否支持批量交换
      */
    private Collection<Integer> translateInternal(
            final PartitionTransformation<OUT> transformation,
            final Context context,
            boolean supportsBatchExchange) {
        /**
         * 使用checkNotNull方法检查transformation和context参数是否为null
         */
        checkNotNull(transformation);
        checkNotNull(context);
        /**
         * 从context对象中获取StreamGraph对象。
         */
        final StreamGraph streamGraph = context.getStreamGraph();
        /**
         * 从transformation对象中获取其输入的转换列表。
         * 检查这个列表的大小是否为1，如果不是，则抛出异常。
         * 获取这个唯一的输入转换。
         */
        final List<Transformation<?>> parentTransformations = transformation.getInputs();
        checkState(
                parentTransformations.size() == 1,
                "Expected exactly one input transformation but found "
                        + parentTransformations.size());
        /**
         * 获取上游输入的Transformation
         */
        final Transformation<?> input = parentTransformations.get(0);
        /**
         * 创建一个ArrayList用于存储生成的虚拟分区节点的ID。
         */
        List<Integer> resultIds = new ArrayList<>();
        /**
         * 获取StreamOperator之间的数据交换模式
         */
        StreamExchangeMode exchangeMode = transformation.getExchangeMode();
        // StreamExchangeMode#BATCH has no effect in streaming mode so we can safely reset it to
        // UNDEFINED and let Flink decide on the best exchange mode.
        /**
         * 如果不支持批量交换，并且当前的交换模式是批量交换，则将交换模式重置为UNDEFINED，让Flink决定最佳的交换模式。
         */
        if (!supportsBatchExchange && exchangeMode == StreamExchangeMode.BATCH) {
            exchangeMode = StreamExchangeMode.UNDEFINED;
        }
        /**
         * 循环遍历已经转换过得， Map<Transformation<?>, Collection<Integer>> alreadyTransformed;
         * 基于当前input对应的transformation 获取id
         */
        for (Integer inputId : context.getStreamNodeIds(input)) {
            /**
             * getNewNodeId => AtomicInteger ID_COUNTER = new AtomicInteger(0)
             * 生成一个新的虚拟ID
             */
            final int virtualId = Transformation.getNewNodeId();
            /**
             * 添加一个新的虚拟分区节点，使用输入的ID、生成的虚拟ID、StreamPartitioner、交换模式。
             */
            streamGraph.addVirtualPartitionNode(
                    inputId, virtualId, transformation.getPartitioner(), exchangeMode);
            /**
             * 将生成的虚拟ID添加到结果列表中(List)
             */
            resultIds.add(virtualId);
        }
        return resultIds;
    }
}
