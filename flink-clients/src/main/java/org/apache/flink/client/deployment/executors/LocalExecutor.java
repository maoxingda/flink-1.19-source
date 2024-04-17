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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.program.PerJobMiniClusterFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;

import java.net.MalformedURLException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** An {@link PipelineExecutor} for executing a {@link Pipeline} locally. */
@Internal
public class LocalExecutor implements PipelineExecutor {

    public static final String NAME = "local";

    private final Configuration configuration;
    private final Function<MiniClusterConfiguration, MiniCluster> miniClusterFactory;

    public static LocalExecutor create(Configuration configuration) {
        return new LocalExecutor(configuration, MiniCluster::new);
    }

    public static LocalExecutor createWithFactory(
            Configuration configuration,
            Function<MiniClusterConfiguration, MiniCluster> miniClusterFactory) {
        return new LocalExecutor(configuration, miniClusterFactory);
    }

    private LocalExecutor(
            Configuration configuration,
            Function<MiniClusterConfiguration, MiniCluster> miniClusterFactory) {
        this.configuration = configuration;
        this.miniClusterFactory = miniClusterFactory;
    }

    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * execute的公共方法，它接受一个Pipeline对象、一个Configuration对象和一个ClassLoader对象作为参数，
     * 并返回一个CompletableFuture<JobClient>对象。这个方法的目的是执行给定的Pipeline，并异步地返回一个JobClient，
     * 该客户端可用于管理和查询作业的状态。
      */
    @Override
    public CompletableFuture<JobClient> execute(
            Pipeline pipeline, Configuration configuration, ClassLoader userCodeClassloader)
            throws Exception {
        /** 参数检查 */
        checkNotNull(pipeline);
        checkNotNull(configuration);
        /**
         * 创建了一个新的`Configuration`对象`effectiveConfig`，
         * 并将当前对象的`configuration`和传入的`configuration`参数合并到这个新的配置对象中。
         */
        Configuration effectiveConfig = new Configuration();
        effectiveConfig.addAll(this.configuration);
        effectiveConfig.addAll(configuration);

        // we only support attached execution with the local executor.
        /** 只有local才会校验*/
        checkState(configuration.get(DeploymentOptions.ATTACHED));
        /**
         * 调用`getJobGraph`方法来将`pipeline`转换为`JobGraph`对象。
         */
        final JobGraph jobGraph = getJobGraph(pipeline, effectiveConfig, userCodeClassloader);
        /**
         * submitJob提交作业并返回JobClient
         */
        return PerJobMiniClusterFactory.createWithFactory(effectiveConfig, miniClusterFactory)
                .submitJob(jobGraph, userCodeClassloader);
    }

    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * 定义了一个名为 getJobGraph 的私有方法，它的主要目的是将传入的 Pipeline 对象转换为 JobGraph 对象
      */
    private JobGraph getJobGraph(
            Pipeline pipeline, Configuration configuration, ClassLoader userCodeClassloader)
            throws MalformedURLException {
        // This is a quirk in how LocalEnvironment used to work. It sets the default parallelism
        // to <num taskmanagers> * <num task slots>. Might be questionable but we keep the behaviour
        // for now.
        /**
         * pipeline 是否是 Plan 类型的实例。如果是，则将其转换为 Plan 类型，并存储在变量 plan 中。
         */
        if (pipeline instanceof Plan) {
            Plan plan = (Plan) pipeline;
            final int slotsPerTaskManager =
                    configuration.get(
                            TaskManagerOptions.NUM_TASK_SLOTS, plan.getMaximumParallelism());
            final int numTaskManagers =
                    configuration.get(TaskManagerOptions.MINI_CLUSTER_NUM_TASK_MANAGERS);
            /** 设置并行度 */
            plan.setDefaultParallelism(slotsPerTaskManager * numTaskManagers);
        }
        /**
         * 调用 PipelineExecutorUtils 的 getJobGraph 方法，传入原始的 pipeline、configuration 和 userCodeClassloader，以完成 Pipeline 到 JobGraph 的转换，
         * 并返回转换后的 JobGraph 对象。
         */
        return PipelineExecutorUtils.getJobGraph(pipeline, configuration, userCodeClassloader);
    }
}
