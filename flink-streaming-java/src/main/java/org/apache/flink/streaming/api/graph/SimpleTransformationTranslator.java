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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.util.graph.StreamGraphUtils;

import java.util.Collection;

import static org.apache.flink.runtime.util.config.memory.ManagedMemoryUtils.validateUseCaseWeightsNotConflict;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A base class for all {@link TransformationTranslator TransformationTranslators} who translate
 * {@link Transformation Transformations} that have a single operator in their runtime
 * implementation. These include most of the currently supported operations.
 *
 * @param <OUT> The type of the output elements of the transformation being translated.
 * @param <T> The type of transformation being translated.
 */
@Internal
public abstract class SimpleTransformationTranslator<OUT, T extends Transformation<OUT>>
        implements TransformationTranslator<OUT, T> {

    @Override
    public final Collection<Integer> translateForBatch(
            final T transformation, final Context context) {
        checkNotNull(transformation);
        checkNotNull(context);

        final Collection<Integer> transformedIds =
                translateForBatchInternal(transformation, context);
        configure(transformation, context);

        return transformedIds;
    }

    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * 对流处理的Transformation进行转换
      */
    @Override
    public final Collection<Integer> translateForStreaming(
            final T transformation, final Context context) {
        /**
         *调用了 checkNotNull方法,如果为空抛出异常
         */
        checkNotNull(transformation);
        checkNotNull(context);
        /**
         * translateForStreamingInternal方法开始转换
         */
        final Collection<Integer> transformedIds =
                translateForStreamingInternal(transformation, context);
        /**
         * 主要目的是将transformation对象的属性配置到streamGraph中的对应节点上，确保流图中的节点具有正确的属性和设置，以便于后续的流处理操作。
         */
        configure(transformation, context);

        return transformedIds;
    }

    /**
     * Translates a given {@link Transformation} to its runtime implementation for BATCH-style
     * execution.
     *
     * @param transformation The transformation to be translated.
     * @param context The translation context.
     * @return The ids of the "last" {@link StreamNode StreamNodes} in the transformation graph
     *     corresponding to this transformation. These will be the nodes that a potential following
     *     transformation will need to connect to.
     */
    protected abstract Collection<Integer> translateForBatchInternal(
            final T transformation, final Context context);

    /**
     * Translates a given {@link Transformation} to its runtime implementation for STREAMING-style
     * execution.
     *
     * @param transformation The transformation to be translated.
     * @param context The translation context.
     * @return The ids of the "last" {@link StreamNode StreamNodes} in the transformation graph
     *     corresponding to this transformation. These will be the nodes that a potential following
     *     transformation will need to connect to.
     */
    protected abstract Collection<Integer> translateForStreamingInternal(
            final T transformation, final Context context);

    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * configure的私有方法，用于配置给定的transformation（转换）对象在context（上下文）中对应的streamGraph（流图）。
     * 主要目的是将transformation对象的属性配置到streamGraph中的对应节点上，确保流图中的节点具有正确的属性和设置，以便于后续的流处理操作。
      */
    private void configure(final T transformation, final Context context) {
        /**
         * 通过context获取到StreamGraph实例对象
         */
        final StreamGraph streamGraph = context.getStreamGraph();
        /**
         * 通过transformation获取transformationId
         */
        final int transformationId = transformation.getId();

        /**
         * 使用StreamGraphUtils工具类，配置streamGraph中对应transformationId的缓冲区超时时间。
         * 超时时间取自context的默认缓冲区超时时间。
         */
        StreamGraphUtils.configureBufferTimeout(
                streamGraph, transformationId, transformation, context.getDefaultBufferTimeout());
        /**
         * 如果transformation的UID不为空，将其设置为streamGraph中对应transformationId的UID。
         */
        if (transformation.getUid() != null) {
            streamGraph.setTransformationUID(transformationId, transformation.getUid());
        }
        /**
         * 如果transformation有用户提供的节点哈希，将其设置为streamGraph中对应transformationId的用户哈希。
         */
        if (transformation.getUserProvidedNodeHash() != null) {
            streamGraph.setTransformationUserHash(
                    transformationId, transformation.getUserProvidedNodeHash());
        }
        /** 使用StreamGraphUtils工具类验证streamGraph中的transformation的UID。 */
        StreamGraphUtils.validateTransformationUid(streamGraph, transformation);
        /**
         * 如果transformation有最小资源和首选资源，将它们设置为streamGraph中对应transformationId的资源
         */
        if (transformation.getMinResources() != null
                && transformation.getPreferredResources() != null) {
            streamGraph.setResources(
                    transformationId,
                    transformation.getMinResources(),
                    transformation.getPreferredResources());
        }
        /**
         * 从streamGraph中获取对应transformationId的streamNode。
         */
        final StreamNode streamNode = streamGraph.getStreamNode(transformationId);
        /** 如果streamNode不为空，进行以下操作： */
        if (streamNode != null) {
            /**
             * 验证streamNode的托管内存操作符范围使用案例权重与transformation的权重是否冲突。
             * 设置streamNode的托管内存使用案例权重和托管内存槽范围使用案例。
             * 如果transformation有描述，将其设置为streamNode的操作符描述。
             */
            validateUseCaseWeightsNotConflict(
                    streamNode.getManagedMemoryOperatorScopeUseCaseWeights(),
                    transformation.getManagedMemoryOperatorScopeUseCaseWeights());
            streamNode.setManagedMemoryUseCaseWeights(
                    transformation.getManagedMemoryOperatorScopeUseCaseWeights(),
                    transformation.getManagedMemorySlotScopeUseCases());
            if (null != transformation.getDescription()) {
                streamNode.setOperatorDescription(transformation.getDescription());
            }
        }
    }
}
