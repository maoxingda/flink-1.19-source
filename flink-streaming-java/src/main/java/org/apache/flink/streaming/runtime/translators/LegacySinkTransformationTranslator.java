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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.graph.SimpleTransformationTranslator;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link TransformationTranslator} for the {@link LegacySinkTransformation}.
 *
 * @param <IN> The type of the elements that are coming in the {@link LegacySinkTransformation}.
 */
@Internal
public class LegacySinkTransformationTranslator<IN>
        extends SimpleTransformationTranslator<IN, LegacySinkTransformation<IN>> {

    @Override
    protected Collection<Integer> translateForBatchInternal(
            final LegacySinkTransformation<IN> transformation, final Context context) {
        final Collection<Integer> ids = translateInternal(transformation, context);
        boolean isKeyed = transformation.getStateKeySelector() != null;
        if (isKeyed) {
            BatchExecutionUtils.applyBatchExecutionSettings(
                    transformation.getId(), context, StreamConfig.InputRequirement.SORTED);
        }
        return ids;
    }

    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * 调用内部方法translateInternal进行SinkTransformation
      */
    @Override
    protected Collection<Integer> translateForStreamingInternal(
            final LegacySinkTransformation<IN> transformation, final Context context) {
        return translateInternal(transformation, context);
    }

    private Collection<Integer> translateInternal(
            final LegacySinkTransformation<IN> transformation, final Context context) {
        /**
         * 使用checkNotNull方法确保transformation和context不是null
         */
        checkNotNull(transformation);
        checkNotNull(context);
        /**
         * 从context中获取流图（streamGraph）
         */
        final StreamGraph streamGraph = context.getStreamGraph();
        /**
         * 获取槽共享组（slotSharingGroup），这通常用于资源管理和优化
         */
        final String slotSharingGroup = context.getSlotSharingGroup();
        /**
         * 获取Transformation的ID
         */
        final int transformationId = transformation.getId();
        /**
         * 从流图中获取执行配置（executionConfig）
         */
        final ExecutionConfig executionConfig = streamGraph.getExecutionConfig();
        /**
         * 从transformation中获取输入转换Transformation
         */
        final List<Transformation<?>> parentTransformations = transformation.getInputs();
        /**
         * 使用checkState确保只有一个输入转换。如果不满足条件，则抛出异常。
         */
        checkState(
                parentTransformations.size() == 1,
                "Expected exactly one input transformation but found "
                        + parentTransformations.size());
        /**
         * 获取上游（父类的）Transformation
         */
        final Transformation<?> input = parentTransformations.get(0);
        /**
         * 使用streamGraph.addSink方法将transformation作为一个sink（输出节点）添加到流图中。
         */
        streamGraph.addSink(
                transformationId,
                slotSharingGroup,
                transformation.getCoLocationGroupKey(),
                transformation.getOperatorFactory(),
                input.getOutputType(),
                null,
                "Sink: " + transformation.getName());
        /**
         *  这里体现的就是配置的优先级，如果在代码中配置了transformation的并行度，那就以配置的并行度为主
         *  如果没有配置i这使用配置文件的
         */
        final int parallelism =
                transformation.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT
                        ? transformation.getParallelism()
                        : executionConfig.getParallelism();
        /**
         * 设置当前算子并行度
         */
        streamGraph.setParallelism(
                transformationId, parallelism, transformation.isParallelismConfigured());
        /**
         * 设置最大并行度
         */
        streamGraph.setMaxParallelism(transformationId, transformation.getMaxParallelism());
        /**
         * 检查transformation是否支持并发执行尝试。
         */
        streamGraph.setSupportsConcurrentExecutionAttempts(
                transformationId, transformation.isSupportsConcurrentExecutionAttempts());
        /**
         * 遍历输入转换（input）的所有输出节点ID。
         * 循环为SinkTransformation添加边
         */
        for (Integer inputId : context.getStreamNodeIds(input)) {
            streamGraph.addEdge(inputId, transformationId, 0);
        }
        /**
         * 检查transformation是否有一个状态键选择器（getStateKeySelector）。状态键选择器用于确定状态在分布式系统中的键。
         */
        if (transformation.getStateKeySelector() != null) {
            /**
             * 如果存在状态键选择器，代码会获取与该选择器关联的状态键的类型，并基于执行配置创建一个类型序列化器（TypeSerializer）。
             */
            TypeSerializer<?> keySerializer =
                    transformation
                            .getStateKeyType()
                            .createSerializer(executionConfig.getSerializerConfig());
            /**
             * 使用streamGraph.setOneInputStateKey方法将状态键选择器和序列化器设置到流图中的相应转换。
             */
            streamGraph.setOneInputStateKey(
                    transformationId, transformation.getStateKeySelector(), keySerializer);
        }

        return Collections.emptyList();
    }
}
