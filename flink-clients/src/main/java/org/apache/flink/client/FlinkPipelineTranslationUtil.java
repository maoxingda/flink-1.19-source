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
 *
 */

package org.apache.flink.client;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;

/**
 * Utility for transforming {@link Pipeline FlinkPipelines} into a {@link JobGraph}. This uses
 * reflection or service discovery to find the right {@link FlinkPipelineTranslator} for a given
 * subclass of {@link Pipeline}.
 */
public final class FlinkPipelineTranslationUtil {

    /** Transmogrifies the given {@link Pipeline} to a {@link JobGraph}. */
    /**
     * 该方法的主要目的是将一个Pipeline对象转化为一个JobGraph对象。
     * @param userClassloader
     * @param pipeline
     * @param optimizerConfiguration
     * @param defaultParallelism
     * @return
     */
    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * 该方法的主要目的是将一个Pipeline对象转化为一个JobGraph对象。
    * @param userClassloader: 类加载器，用于加载用户提供的类。
    * @param pipeline: 要转化为JobGraph的Pipeline对象。
    * @param optimizerConfiguration: 优化器配置，可能包含用于优化Pipeline的设置。
    * @param  defaultParallelism: 默认的并行度，即任务在集群中的并行执行程度。
      */
    public static JobGraph getJobGraph(
            ClassLoader userClassloader,
            Pipeline pipeline,
            Configuration optimizerConfiguration,
            int defaultParallelism) {
        /** 获取FlinkPipelineTranslator实例 */
        FlinkPipelineTranslator pipelineTranslator =
                getPipelineTranslator(userClassloader, pipeline);
        /**
         * 使用FlinkPipelineTranslator的translateToJobGraph方法，将Pipeline、优化器配置和默认并行度作为参数，转化为JobGraph对象。
         * 完成pipeline到JobGraph的转换，提问这里问什么是pipeline?(StreamGraph是Pipeline接口的实现)
         */
        JobGraph jobGraph =
                pipelineTranslator.translateToJobGraph(
                        pipeline, optimizerConfiguration, defaultParallelism);
        /**
         * 从optimizerConfiguration中获取名为PARALLELISM_OVERRIDES的配置项。
         * 如果这个配置项存在，则将其值（一个map）设置到jobGraph的配置中。这通常用于覆盖某些特定操作的默认并行度。
         */
        optimizerConfiguration
                .getOptional(PipelineOptions.PARALLELISM_OVERRIDES)
                .ifPresent(
                        map ->
                                jobGraph.getJobConfiguration()
                                        .set(PipelineOptions.PARALLELISM_OVERRIDES, map));
        /** 返回转化后的JobGraph对象。 */
        return jobGraph;
    }

    /**
     * Transmogrifies the given {@link Pipeline} under the userClassloader to a {@link JobGraph}.
     */
    public static JobGraph getJobGraphUnderUserClassLoader(
            final ClassLoader userClassloader,
            final Pipeline pipeline,
            final Configuration configuration,
            final int defaultParallelism) {
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(userClassloader);
            return FlinkPipelineTranslationUtil.getJobGraph(
                    userClassloader, pipeline, configuration, defaultParallelism);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    /** Extracts the execution plan (as JSON) from the given {@link Pipeline}. */
    public static String translateToJSONExecutionPlan(
            ClassLoader userClassloader, Pipeline pipeline) {
        FlinkPipelineTranslator pipelineTranslator =
                getPipelineTranslator(userClassloader, pipeline);
        return pipelineTranslator.translateToJSONExecutionPlan(pipeline);
    }
    /**
    * @授课老师(微信): yi_locus
    * email: 156184212@qq.com
    * 这里主要是获取对应的FlinkPipelineTranslator，
    * PlanTranslator是针对DataSet API
    * StreamGraphTranslator是针对DataStream API
    *
    */
    private static FlinkPipelineTranslator getPipelineTranslator(
            ClassLoader userClassloader, Pipeline pipeline) {
        /** 创建了一个新的PlanTranslator对象。 */
        PlanTranslator planTranslator = new PlanTranslator();
        /** 如果是 pipeLine 则在返回 planTranslator*/
        if (planTranslator.canTranslate(pipeline)) {
            return planTranslator;
        }
        /** 创建了一个新的StreamGraphTranslator对象。 */
        StreamGraphTranslator streamGraphTranslator = new StreamGraphTranslator(userClassloader);
        /** 如果是 StreamGraph 则在返回 planTranslator*/
        if (streamGraphTranslator.canTranslate(pipeline)) {
            return streamGraphTranslator;
        }

        throw new RuntimeException(
                "Translator "
                        + streamGraphTranslator
                        + " cannot translate "
                        + "the given pipeline "
                        + pipeline
                        + ".");
    }
}
