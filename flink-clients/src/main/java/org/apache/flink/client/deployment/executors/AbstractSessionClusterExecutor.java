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
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientJobClientAdapter;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.CacheSupportedPipelineExecutor;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.function.FunctionUtils;

import javax.annotation.Nonnull;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An abstract {@link PipelineExecutor} used to execute {@link Pipeline pipelines} on an existing
 * (session) cluster.
 *
 * @param <ClusterID> the type of the id of the cluster.
 * @param <ClientFactory> the type of the {@link ClusterClientFactory} used to create/retrieve a
 *     client to the target cluster.
 */
@Internal
public class AbstractSessionClusterExecutor<
                ClusterID, ClientFactory extends ClusterClientFactory<ClusterID>>
        implements CacheSupportedPipelineExecutor {

    private final ClientFactory clusterClientFactory;

    public AbstractSessionClusterExecutor(@Nonnull final ClientFactory clusterClientFactory) {
        this.clusterClientFactory = checkNotNull(clusterClientFactory);
    }

    @Override
    public CompletableFuture<JobClient> execute(
            @Nonnull final Pipeline pipeline,
            @Nonnull final Configuration configuration,
            @Nonnull final ClassLoader userCodeClassloader)
            throws Exception {
        /**
         *  根据Pipeline参数获取JobGraph
         */
        final JobGraph jobGraph =
                PipelineExecutorUtils.getJobGraph(pipeline, configuration, userCodeClassloader);
        /**
         * ClusterDescriptor,用于集群通信的客户端的描述符,内部最核心的就是像集群提交任务
         * deploySessionCluster、deployApplicationCluster、deployJobCluster
         * ClusterDescriptor是基于配置构建的并不是说去和集群通信
         */
        try (final ClusterDescriptor<ClusterID> clusterDescriptor =
                clusterClientFactory.createClusterDescriptor(configuration)) {
            /**
             * 获取ClusterID
             */
            final ClusterID clusterID = clusterClientFactory.getClusterId(configuration);
            checkState(clusterID != null);
            /**
             * 检索现有集群通过clusterId
             * ClusterClients的工厂,用来获取ClusterClients
             */
            final ClusterClientProvider<ClusterID> clusterClientProvider =
                    clusterDescriptor.retrieve(clusterID);
            /**
             * 通过ClusterClientProvider实例获取ClusterClient集群客户端。用于提交任务
             */
            ClusterClient<ClusterID> clusterClient = clusterClientProvider.getClusterClient();
            return clusterClient
                    /** 使用clusterClient的submitJob方法提交一个jobGraph到集群 */
                    .submitJob(jobGraph)
                    /** 作业提交后，使用thenApplyAsync方法异步地等待作业初始化完成。 */
                    .thenApplyAsync(
                            FunctionUtils.uncheckedFunction(
                                    jobId -> {
                                        /** 此方法将阻塞，直到作业状态不再为INITIALIZING。 */
                                        ClientUtils.waitUntilJobInitializationFinished(
                                                () -> clusterClient.getJobStatus(jobId).get(),
                                                () -> clusterClient.requestJobResult(jobId).get(),
                                                userCodeClassloader);
                                        return jobId;
                                    }))
                    .thenApplyAsync(
                            jobID ->
                                    /** 一旦作业初始化完成，该代码段会创建一个ClusterClientJobClientAdapter实例(JobClient) */
                                    (JobClient)
                                            new ClusterClientJobClientAdapter<>(
                                                    clusterClientProvider,
                                                    jobID,
                                                    userCodeClassloader))
                    /**  关闭集群客户端 */
                    .whenCompleteAsync((ignored1, ignored2) -> clusterClient.close());
        }
    }

    @Override
    public CompletableFuture<Set<AbstractID>> listCompletedClusterDatasetIds(
            Configuration configuration, ClassLoader userCodeClassloader) throws Exception {

        try (final ClusterDescriptor<ClusterID> clusterDescriptor =
                clusterClientFactory.createClusterDescriptor(configuration)) {
            final ClusterID clusterID = clusterClientFactory.getClusterId(configuration);
            checkState(clusterID != null);

            final ClusterClientProvider<ClusterID> clusterClientProvider =
                    clusterDescriptor.retrieve(clusterID);

            final ClusterClient<ClusterID> clusterClient = clusterClientProvider.getClusterClient();
            return clusterClient.listCompletedClusterDatasetIds();
        }
    }

    /**
      * @授课老师(V): yi_locus
      * 将集群中的数据集失效
     * clusterClient.invalidateClusterDataset方法是Task任务结果的数据集失效
     * IntermediateDataSet：表示 JobVertex 的输出，即经过 operator 处理产生的数据集。producer 是 JobVertex ，consumer 是 JobEdge。
      */
    @Override
    public CompletableFuture<Void> invalidateClusterDataset(
            AbstractID clusterDatasetId,
            Configuration configuration,
            ClassLoader userCodeClassloader)
            throws Exception {
        /**
         * 部署集群（例如Yarnm、k8s、Standalone并返回用于集群通信的客户端的描述符。最核心的触发集群部署
         */
        try (final ClusterDescriptor<ClusterID> clusterDescriptor =
                clusterClientFactory.createClusterDescriptor(configuration)) {
            /**
             * 获取集群的ClusterID
             */
            final ClusterID clusterID = clusterClientFactory.getClusterId(configuration);
            checkState(clusterID != null);
            /**
             * 获取得到ClusterDescriptor构建出来ClusterClientProvider类
             */
            final ClusterClientProvider<ClusterID> clusterClientProvider =
                    clusterDescriptor.retrieve(clusterID);
            /**
             * 基于ClusterClientProvider.getClusterClient()方法得到ClusterClient
             * ClusterClient提供了将程序提交到远程集群所需的功能。
             */
            final ClusterClient<ClusterID> clusterClient = clusterClientProvider.getClusterClient();
            /**
             * 通过clusterClient.invalidateClusterDataset方法向远程集群通信(参数类型为IntermediateDataSetID)
             * IntermediateDataSet：表示 JobVertex 的输出，即经过 operator 处理产生的数据集。producer 是 JobVertex ，consumer 是 JobEdge。
             */
            return clusterClient
                    .invalidateClusterDataset(new IntermediateDataSetID(clusterDatasetId))
                    .thenApply(acknowledge -> null);
        }
    }
}
