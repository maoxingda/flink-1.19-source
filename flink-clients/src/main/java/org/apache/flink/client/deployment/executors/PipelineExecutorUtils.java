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

package org.apache.flink.client.deployment.executors;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.client.cli.ClientOptions;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.jobgraph.JobGraph;

import javax.annotation.Nonnull;

import java.net.MalformedURLException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utility class with method related to job execution. */
public class PipelineExecutorUtils {

    /**
     * Creates the {@link JobGraph} corresponding to the provided {@link Pipeline}.
     *
     * @param pipeline the pipeline whose job graph we are computing.
     * @param configuration the configuration with the necessary information such as jars and
     *     classpaths to be included, the parallelism of the job and potential savepoint settings
     *     used to bootstrap its state.
     * @param userClassloader the classloader which can load user classes.
     * @return the corresponding {@link JobGraph}.
     */
    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * getJobGraph是公共静态方法，
      * 主要目的是将给定的 Pipeline 对象转换为一个 JobGraph 对象
      * @param pipeline：非空的 Pipeline 对象，代表待转换的流程。
      * @param configuration：非空的 Configuration 对象，包含作业执行所需的配置信息。
      * @param userClassloader：类加载器，用于加载用户代码。
      */
    public static JobGraph getJobGraph(
            @Nonnull final Pipeline pipeline,
            @Nonnull final Configuration configuration,
            @Nonnull ClassLoader userClassloader)
            throws MalformedURLException {
        /** 非空检查 */
        checkNotNull(pipeline);
        checkNotNull(configuration);
        /**
         * 从 configuration 中创建了一个 ExecutionConfigAccessor 对象，这个对象提供对执行配置（如并行度、JAR 文件列表等）的访问。
         */
        final ExecutionConfigAccessor executionConfigAccessor =
                ExecutionConfigAccessor.fromConfiguration(configuration);
        /**
         *使用 FlinkPipelineTranslationUtil 的 getJobGraph 方法将 pipeline 转换为 JobGraph。
         * 转换过程中还使用了 userClassloader、configuration 和从 executionConfigAccessor 获取的并行度。
         */
        final JobGraph jobGraph =
                FlinkPipelineTranslationUtil.getJobGraph(
                        userClassloader,
                        pipeline,
                        configuration,
                        executionConfigAccessor.getParallelism());
        /**
         * 如果配置中指定了固定的 JobID，则将其设置为 jobGraph 的 JobID。
         */
        configuration
                .getOptional(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID)
                .ifPresent(strJobID -> jobGraph.setJobID(JobID.fromHexString(strJobID)));
        /**
         * 如果配置中启用了附着模式并且设置为在附着时关闭，则设置 jobGraph 的初始客户端心跳超时时间。
         */
        if (configuration.get(DeploymentOptions.ATTACHED)
                && configuration.get(DeploymentOptions.SHUTDOWN_IF_ATTACHED)) {
            jobGraph.setInitialClientHeartbeatTimeout(
                    configuration.get(ClientOptions.CLIENT_HEARTBEAT_TIMEOUT));
        }
        /**
         * 添加 JAR 文件和类路径
         * 将 executionConfigAccessor 中的 JAR 文件和类路径添加到 jobGraph 中。
         */
        jobGraph.addJars(executionConfigAccessor.getJars());
        jobGraph.setClasspaths(executionConfigAccessor.getClasspaths());
        jobGraph.setSavepointRestoreSettings(executionConfigAccessor.getSavepointRestoreSettings());
        /** 返回转换后的 JobGraph 对象 */
        return jobGraph;
    }
}
