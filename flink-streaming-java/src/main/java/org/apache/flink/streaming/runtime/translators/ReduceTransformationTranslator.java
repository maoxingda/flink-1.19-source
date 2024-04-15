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

package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.operators.BatchGroupedReduceOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamGroupedReduceOperator;
import org.apache.flink.streaming.api.transformations.ReduceTransformation;

import java.util.Collection;

/**
 * A {@link TransformationTranslator} for the {@link ReduceTransformation}.
 *
 * @param <IN> The type of the elements in the input {@code Transformation} of the transformation to
 *     translate.
 */
public class ReduceTransformationTranslator<IN, KEY>
        extends AbstractOneInputTransformationTranslator<IN, IN, ReduceTransformation<IN, KEY>> {
    @Override
    public Collection<Integer> translateForBatchInternal(
            final ReduceTransformation<IN, KEY> transformation, final Context context) {
        BatchGroupedReduceOperator<IN, KEY> groupedReduce =
                new BatchGroupedReduceOperator<>(
                        transformation.getReducer(),
                        transformation
                                .getInputType()
                                .createSerializer(
                                        context.getStreamGraph()
                                                .getExecutionConfig()
                                                .getSerializerConfig()));
        SimpleOperatorFactory<IN> operatorFactory = SimpleOperatorFactory.of(groupedReduce);
        operatorFactory.setChainingStrategy(transformation.getChainingStrategy());
        Collection<Integer> ids =
                translateInternal(
                        transformation,
                        operatorFactory,
                        transformation.getInputType(),
                        transformation.getKeySelector(),
                        transformation.getKeyTypeInfo(),
                        context);
        BatchExecutionUtils.applyBatchExecutionSettings(
                transformation.getId(), context, StreamConfig.InputRequirement.SORTED);

        return ids;
    }

    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * 用于处理流式处理中的ReduceTransformation
      * 该方法主要是将ReduceTransformation转换为流处理操作，并返回生成的虚拟分区节点的ID集合
      */
    @Override
    public Collection<Integer> translateForStreamingInternal(
            final ReduceTransformation<IN, KEY> transformation, final Context context) {
        /**
         * 创建一个StreamGroupedReduceOperator对象groupedReduce。这个操作符是用于执行分组归约操作的。
         * 构造函数接受归约函数（reducer），以及通过输入类型（inputType）和序列化配置（从StreamGraph的执行配置中获取）创建的序列化器。
         */
        StreamGroupedReduceOperator<IN> groupedReduce =
                new StreamGroupedReduceOperator<>(
                        transformation.getReducer(),
                        transformation
                                .getInputType()
                                .createSerializer(
                                        context.getStreamGraph()
                                                .getExecutionConfig()
                                                .getSerializerConfig()));
        /**
         * 创建一个SimpleOperatorFactory对象operatorFactory，并使用groupedReduce作为参数来初始化它。这个工厂用于创建操作符实例。
         */
        SimpleOperatorFactory<IN> operatorFactory = SimpleOperatorFactory.of(groupedReduce);
        /**
         * 通过setChainingStrategy方法设置操作符的链接策略，该策略由ReduceTransformation提供
         */
        operatorFactory.setChainingStrategy(transformation.getChainingStrategy());
        /**
         * 使用translateInternal方法来执行实际的转换过程。
         */
        return translateInternal(
                transformation,
                operatorFactory,
                transformation.getInputType(),
                transformation.getKeySelector(),
                transformation.getKeyTypeInfo(),
                context);
    }
}
