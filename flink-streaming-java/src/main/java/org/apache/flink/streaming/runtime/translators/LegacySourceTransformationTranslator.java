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
import org.apache.flink.streaming.api.graph.SimpleTransformationTranslator;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.operators.InputFormatOperatorFactory;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;

import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TransformationTranslator} for the {@link LegacySourceTransformation}.
 *
 * @param <OUT> The type of the elements that the {@link LegacySourceTransformation} we are
 *     translating is producing.
 */
@Internal
public class LegacySourceTransformationTranslator<OUT>
        extends SimpleTransformationTranslator<OUT, LegacySourceTransformation<OUT>> {

    @Override
    protected Collection<Integer> translateForBatchInternal(
            final LegacySourceTransformation<OUT> transformation, final Context context) {
        return translateInternal(transformation, context);
    }

    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      *  translateInternal进行转换
      */
    @Override
    protected Collection<Integer> translateForStreamingInternal(
            final LegacySourceTransformation<OUT> transformation, final Context context) {
        return translateInternal(transformation, context);
    }

    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * 对LegacySourceTransformation进行转换
      */
    private Collection<Integer> translateInternal(
            final LegacySourceTransformation<OUT> transformation, final Context context) {
        /**
         * 校验是否为空
         */
        checkNotNull(transformation);
        checkNotNull(context);
        /**
         * 从context中获取StreamGraph、slotSharingGroup
         * 这里知道Context环境是做什么用的了吧
         */
        final StreamGraph streamGraph = context.getStreamGraph();
        final String slotSharingGroup = context.getSlotSharingGroup();
        /**获取当前transformationId*/
        final int transformationId = transformation.getId();
        /**
         * 获取ExecutionConfig，里面存放checkpoint等相关配置信息
         */
        final ExecutionConfig executionConfig = streamGraph.getExecutionConfig();
        /**
         * 构建Srouce类型的StreamNode
         */
        /**
          * @授课老师(V): yi_locus
          * email: 156184212@qq.com
          * addLegacySource方法被用来向流图中添加一个数据源。这个方法接受多个参数，包括：
          * transformationId: 转换操作的唯一标识符。
          * slotSharingGroup: 槽共享组，用于优化资源使用。
          * coLocationGroupKey: 协同定位组的键，用于确保节点在同一位置执行。
          * operatorFactory: 操作符工厂，用于创建操作该数据源的操作符。
          * null: 这个位置通常可能用于传递其他配置或参数，但在这里是null。
          * outputType: 数据源的输出类型。
          * "Source: " + transformation.getName(): 数据源的名称，用于日志或调试。
          */
        streamGraph.addLegacySource(
                transformationId,
                slotSharingGroup,
                transformation.getCoLocationGroupKey(),
                transformation.getOperatorFactory(),
                null,
                transformation.getOutputType(),
                "Source: " + transformation.getName());

        /**
         * 如果transformation的StreamOperator工厂是InputFormatOperatorFactory的实例，则调用streamGraph.setInputFormat方法设置输入格式。
         */
        if (transformation.getOperatorFactory() instanceof InputFormatOperatorFactory) {
            /**
             * 设置getStreamNode的输入格式
             * 内部通过transformationId(vertexId)获取到StreamNode
             */
            streamGraph.setInputFormat(
                    transformationId,
                    ((InputFormatOperatorFactory<OUT>) transformation.getOperatorFactory())
                            .getInputFormat());
        }

        /**
         * 获取并行度
         * 首先，它检查transformation对象的并行度是否使用了默认值ExecutionConfig.PARALLELISM_DEFAULT。
         * 如果不是使用默认值，则直接采用transformation对象的并行度。
         * 如果是默认值，则使用executionConfig中配置的并行度。
         * 这样确保了并行度是根据transformation的特定配置或全局executionConfig来确定的。
         * 总结：这里其实就是并行度配置优先级。
         */
        final int parallelism =
                transformation.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT
                        ? transformation.getParallelism()
                        : executionConfig.getParallelism();
        /**
         * 使用streamGraph.setParallelism方法，为指定的transformationId对应的StreamNode设置并行度。
         */
        streamGraph.setParallelism(
                transformationId, parallelism, transformation.isParallelismConfigured());
        /**
         *使用streamGraph.setMaxParallelism方法，为指定的transformationId对应的StreamNode设置并行度。这通常用于限制某个操作可以使用的最大并行实例数量。
         */
        streamGraph.setMaxParallelism(transformationId, transformation.getMaxParallelism());
        /**
         * 设置是否支持并发执行尝试。这在某些流处理场景中可能很有用，比如当任务失败时，可以立即尝试并发执行另一个任务实例来替代它
         */
        streamGraph.setSupportsConcurrentExecutionAttempts(
                transformationId, transformation.isSupportsConcurrentExecutionAttempts());
        /**
         * 返回了一个只包含transformationId的集合
         */
        return Collections.singleton(transformationId);
    }
}
