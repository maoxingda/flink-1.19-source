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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.graph.SimpleTransformationTranslator;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A utility base class for one input {@link Transformation transformations} that provides a
 * function for configuring common graph properties.
 */
abstract class AbstractOneInputTransformationTranslator<IN, OUT, OP extends Transformation<OUT>>
        extends SimpleTransformationTranslator<OUT, OP> {

    protected Collection<Integer> translateInternal(
            final Transformation<OUT> transformation,
            final StreamOperatorFactory<OUT> operatorFactory,
            final TypeInformation<IN> inputType,
            @Nullable final KeySelector<IN, ?> stateKeySelector,
            @Nullable final TypeInformation<?> stateKeyType,
            final Context context) {
        /**
         * 方法首先校验传入的参数，确保transformation、operatorFactory、inputType和context都不为null。
         * 这是为了避免在后续的处理中出现NullPointerException。
         */
        checkNotNull(transformation);
        checkNotNull(operatorFactory);
        checkNotNull(inputType);
        checkNotNull(context);
        /**
         * 从context中获取StreamGraph对象，代表流处理图
         */
        final StreamGraph streamGraph = context.getStreamGraph();
        /**
         * 从context中获取slotSharingGroup（用于指定任务槽共享组）和。
         */
        final String slotSharingGroup = context.getSlotSharingGroup();
        /**
         * 从context中获取transformationId（转换操作的唯一标识符）
         */
        final int transformationId = transformation.getId();
        /**
         * 通过streamGraph获取了executionConfig，这是执行配置对象，包含了流处理作业的执行参数,比如checkpoint相关的参数
         */
        final ExecutionConfig executionConfig = streamGraph.getExecutionConfig();
        /**
         * 使用streamGraph.addOperator方法构建OneInputTransformation类型的StreamNode
         */
        streamGraph.addOperator(
                transformationId,
                slotSharingGroup,
                transformation.getCoLocationGroupKey(),
                operatorFactory,
                inputType,
                transformation.getOutputType(),
                transformation.getName());
        /**
         * 设置状态Selector
         * 如果stateKeySelector不为null，
         */
        if (stateKeySelector != null) {
            /** 创建一个状态键的序列化器 */
            TypeSerializer<?> keySerializer =
                    stateKeyType.createSerializer(executionConfig.getSerializerConfig());
            /**
             * 将状态键选择器和序列化器与转换操作关联起来，支持状态管理，比如键值状态
             * 也就是设置Map FlatMap对应的StreamNode
             */
            streamGraph.setOneInputStateKey(transformationId, stateKeySelector, keySerializer);
        }
        /**
         * 检查了transformation的并行度设置。如果transformation设置了特定的并行度，则使用transformation设置的并行度；
         * 否则，使用executionConfig中配置的默认并行度。
         * 使用streamGraph.setParallelism方法设置了
         */
        int parallelism =
                transformation.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT
                        ? transformation.getParallelism()
                        : executionConfig.getParallelism();
        /**
         * 通过transformationId获取到StreamNode，然后设置StreamNode并行度，并指示这个并行度是否被显式配置。
         */
        streamGraph.setParallelism(
                transformationId, parallelism, transformation.isParallelismConfigured());
        /**
         * 使用streamGraph.setMaxParallelism方法设置了转换操作的最大并行度。
         */
        streamGraph.setMaxParallelism(transformationId, transformation.getMaxParallelism());
        /**
         * 获取当前transformation对应的父类依赖列表，每个转换可能依赖于一个或多个父转换的输出
         */
        final List<Transformation<?>> parentTransformations = transformation.getInputs();
        /**
         * 来检查parentTransformations列表的大小是否为1。如果不是，则抛出一个异常，
         * 说明期望恰好有一个输入转换，但发现了其他数量的输入转换。这是为了确保当前的转换操作只依赖于一个父转换。
         * 为什么校验是否为1？因为OneInput就是一对一的逻辑
         */
        checkState(
                parentTransformations.size() == 1,
                "Expected exactly one input transformation but found "
                        + parentTransformations.size());
        /**
         * 添加流图边，添加StreamNode对应的出边、入边
         * 代码遍历父转换的输入节点ID，并在StreamNode中添加从父转换的节点到当前转换的边。
         */
        for (Integer inputId : context.getStreamNodeIds(parentTransformations.get(0))) {
            streamGraph.addEdge(inputId, transformationId, 0);
        }

        /**
         * 如果当前的transformation是PhysicalTransformation的实例，那么代码会检查这个物理转换是否支持并发执行尝试，
         * 并据此设置流图中对应转换的属性。这通常用于优化执行，允许某些操作并行执行以提高性能。
         */
        if (transformation instanceof PhysicalTransformation) {
            streamGraph.setSupportsConcurrentExecutionAttempts(
                    transformationId,
                    ((PhysicalTransformation<OUT>) transformation)
                            .isSupportsConcurrentExecutionAttempts());
        }
        /**
         * 返回一个只包含当前转换ID的集合
         */
        return Collections.singleton(transformationId);
    }
}
