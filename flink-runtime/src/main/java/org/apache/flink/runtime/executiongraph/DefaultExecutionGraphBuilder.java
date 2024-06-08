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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.checkpoint.hooks.MasterHooks;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.executiongraph.failover.partitionrelease.PartitionGroupReleaseStrategy;
import org.apache.flink.runtime.executiongraph.failover.partitionrelease.PartitionGroupReleaseStrategyFactoryLoader;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageLoader;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static org.apache.flink.configuration.StateChangelogOptions.STATE_CHANGE_LOG_STORAGE;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class to encapsulate the logic of building an {@link DefaultExecutionGraph} from a {@link
 * JobGraph}.
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 封装从 JobGraph 构建 DefaultExecutionGraph 的逻辑的实用程序类。
*/
public class DefaultExecutionGraphBuilder {

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 基于JobGraph构建出来DefaultExecutionGraph
    */
    public static DefaultExecutionGraph buildGraph(
            JobGraph jobGraph,
            Configuration jobManagerConfig,
            ScheduledExecutorService futureExecutor,
            Executor ioExecutor,
            ClassLoader classLoader,
            CompletedCheckpointStore completedCheckpointStore,
            CheckpointsCleaner checkpointsCleaner,
            CheckpointIDCounter checkpointIdCounter,
            Time rpcTimeout,
            BlobWriter blobWriter,
            Logger log,
            ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker partitionTracker,
            TaskDeploymentDescriptorFactory.PartitionLocationConstraint partitionLocationConstraint,
            ExecutionDeploymentListener executionDeploymentListener,
            ExecutionStateUpdateListener executionStateUpdateListener,
            long initializationTimestamp,
            VertexAttemptNumberStore vertexAttemptNumberStore,
            VertexParallelismStore vertexParallelismStore,
            Supplier<CheckpointStatsTracker> checkpointStatsTrackerFactory,
            boolean isDynamicGraph,
            ExecutionJobVertex.Factory executionJobVertexFactory,
            MarkPartitionFinishedStrategy markPartitionFinishedStrategy,
            boolean nonFinishedHybridPartitionShouldBeUnknown,
            JobManagerJobMetricGroup jobManagerJobMetricGroup)
            throws JobExecutionException, JobException {
        /** 校验是否为null ,否则抛出异常*/
        checkNotNull(jobGraph, "job graph cannot be null");
        /** 获取Jobname */
        final String jobName = jobGraph.getName();
        /** 获取jobId */
        final JobID jobId = jobGraph.getJobID();
        /** 获取JobInformation */
        final JobInformation jobInformation =
                new JobInformation(
                        jobId,
                        jobName,
                        jobGraph.getSerializedExecutionConfig(),
                        jobGraph.getJobConfiguration(),
                        jobGraph.getUserJarBlobKeys(),
                        jobGraph.getClasspaths());

        /**
         * 置在Flink中JobManager保留多少个已完成的job execution尝试的历史记录。
         * jobmanager.execution.attempts-history-size 默认16 历史中保留的历史执行尝试的最大次数
         */
        final int executionHistorySizeLimit =
                jobManagerConfig.get(JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE);
        /**
         * 分区组策略
         * 用于决定何时发布 ConsumerdPartitionGroup的策略的接口。
         */
        final PartitionGroupReleaseStrategy.Factory partitionGroupReleaseStrategyFactory =
                PartitionGroupReleaseStrategyFactoryLoader.loadPartitionGroupReleaseStrategyFactory(
                        jobManagerConfig);
        /**
         *
         * jobmanager.task-deployment.offload-shuffle-descriptors-to-blob-server.threshold-num
         * 这是一个专家选项，我们不想在文档中公开。默认值几乎适用于所有情况
         * blob服务器的阈值,默认64M
         */
        /**
         * 将shuffle描述符卸载到blob Server的阈值。
         * 一旦shuffle描述符的数量超过此值，把shuffle描述符卸载到blob Server。
         * 此默认值意味着JobManager需要序列化并传输2048个shuffle描述符（几乎32KB）给2048个消费者（总共64MB）。
         */
        final int offloadShuffleDescriptorsThreshold =
                jobManagerConfig.get(
                        TaskDeploymentDescriptorFactory.OFFLOAD_SHUFFLE_DESCRIPTORS_THRESHOLD);
        /**
         * TaskDeploymentDescriptor的工厂
         * TaskDeploymentDescriptor用于从Execution部署到Task
         * 后面详细讲解
         */
        final TaskDeploymentDescriptorFactory taskDeploymentDescriptorFactory;
        try {
            taskDeploymentDescriptorFactory =
                    new TaskDeploymentDescriptorFactory(
                            BlobWriter.serializeAndTryOffload(jobInformation, jobId, blobWriter),
                            jobId,
                            partitionLocationConstraint,
                            blobWriter,
                            nonFinishedHybridPartitionShouldBeUnknown,
                            offloadShuffleDescriptorsThreshold);
        } catch (IOException e) {
            throw new JobException("Could not create the TaskDeploymentDescriptorFactory.", e);
        }

        // create a new execution graph, if none exists so far
        /**
         * 创建一个新的执行图
         */
        final DefaultExecutionGraph executionGraph =
                new DefaultExecutionGraph(
                        jobInformation,
                        futureExecutor,
                        ioExecutor,
                        rpcTimeout,
                        executionHistorySizeLimit,
                        classLoader,
                        blobWriter,
                        partitionGroupReleaseStrategyFactory,
                        shuffleMaster,
                        partitionTracker,
                        executionDeploymentListener,
                        executionStateUpdateListener,
                        initializationTimestamp,
                        vertexAttemptNumberStore,
                        vertexParallelismStore,
                        isDynamicGraph,
                        executionJobVertexFactory,
                        jobGraph.getJobStatusHooks(),
                        markPartitionFinishedStrategy,
                        taskDeploymentDescriptorFactory);

        // set the basic properties
        /** 设置公共属性  */
        try {
            /** 将来JobGrap转换为json格式设置到executionGraph jsonPlan字段中*/
            executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));
        } catch (Throwable t) {
            /** 打印日志抛出异常 */
            log.warn("Cannot create JSON plan for job", t);
            // give the graph an empty plan
            executionGraph.setJsonPlan("{}");
        }

        // initialize the vertices that have a master initialization hook
        // file output formats create directories here, input formats create splits
        /**
         * 初始化具有主初始化钩子文件的顶点输出格式在此处创建目录，输入格式创建拆分
         */
        /** 获取系统纳秒时间 */
        final long initMasterStart = System.nanoTime();
        log.info("Running initialization on master for job {} ({}).", jobName, jobId);
        /**
         * 循环JobVertext
         * 判断executableClass是否存在
         */
        for (JobVertex vertex : jobGraph.getVertices()) {
            /** 获取InvokableClass org.apache.flink.streaming.runtime.tasks.OneInputStreamTask */
            String executableClass = vertex.getInvokableClassName();
            /** 如果executableClass不存在 则抛出异常 */
            if (executableClass == null || executableClass.isEmpty()) {
                throw new JobSubmissionException(
                        jobId,
                        "The vertex "
                                + vertex.getID()
                                + " ("
                                + vertex.getName()
                                + ") has no invokable class.");
            }

            try {
                /**
                 * 设置数据源切分、输出并行度，在JobMaster启动前调用
                 * 初始化Master环境
                 */
                vertex.initializeOnMaster(
                        new SimpleInitializeOnMasterContext(
                                classLoader,
                                vertexParallelismStore
                                        .getParallelismInfo(vertex.getID())
                                        .getParallelism()));
            } catch (Throwable t) {
                throw new JobExecutionException(
                        jobId,
                        "Cannot initialize task '" + vertex.getName() + "': " + t.getMessage(),
                        t);
            }
        }
        /** 打印日志 */
        log.info(
                "Successfully ran initialization on master in {} ms.",
                (System.nanoTime() - initMasterStart) / 1_000_000);

        // topologically sort the job vertices and attach the graph to the existing one
        /**
         * 对作业顶点进行拓扑排序，并将图形附加到现有图形
         * id从小达到，source map sink
         */
        List<JobVertex> sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources();
        /** 打印日志 */
        if (log.isDebugEnabled()) {
            log.debug(
                    "Adding {} vertices from job graph {} ({}).",
                    sortedTopology.size(),
                    jobName,
                    jobId);
        }
        /** 进行转换 */
        executionGraph.attachJobGraph(sortedTopology, jobManagerJobMetricGroup);

        if (log.isDebugEnabled()) {
            log.debug(
                    "Successfully created execution graph from job graph {} ({}).", jobName, jobId);
        }

        // configure the state checkpointing
        /** 配置状态检查点 */
        if (isDynamicGraph) {
            // dynamic graph does not support checkpointing so we skip it
            /** 动态图不支持检查点 */
            log.warn("Skip setting up checkpointing for a job with dynamic graph.");
        } else if (isCheckpointingEnabled(jobGraph)) {
            /**
             * 如果开启了检查点配置
             * 则从JobGraph获取检查点配置
             */
            JobCheckpointingSettings snapshotSettings = jobGraph.getCheckpointingSettings();

            // load the state backend from the application settings
            /** 从应用程序设置加载状态后端 */
            final StateBackend applicationConfiguredBackend;
            /**
             * 获取默认的状态后端
             */
            final SerializedValue<StateBackend> serializedAppConfigured =
                    snapshotSettings.getDefaultStateBackend();
            // 检查 serializedAppConfigured 是否为空
            if (serializedAppConfigured == null) {
                // 如果为空，则设置 applicationConfiguredBackend 为 null
                applicationConfiguredBackend = null;
            } else {
                try {
                    // 否则，尝试使用 classLoader 反序列化 serializedAppConfigured
                    applicationConfiguredBackend =
                            serializedAppConfigured.deserializeValue(classLoader);
                } catch (IOException | ClassNotFoundException e) {
                    //抛出异常
                    throw new JobExecutionException(
                            jobId, "Could not deserialize application-defined state backend.", e);
                }
            }
            // 定义一个 StateBackend 类型的变量 rootBackend
            final StateBackend rootBackend;
            try {
                // 尝试从 applicationConfiguredBackend、jobGraph 的配置、jobManagerConfig、classLoader 和日志中
                // 加载或配置 StateBackend，如果都没有指定，则使用默认配置
                rootBackend =
                        StateBackendLoader.fromApplicationOrConfigOrDefault(
                                applicationConfiguredBackend,
                                jobGraph.getJobConfiguration(),
                                jobManagerConfig,
                                classLoader,
                                log);
            } catch (IllegalConfigurationException | IOException | DynamicCodeLoadingException e) {
                // 如果在加载或配置 StateBackend 的过程中发生异常
                // 则抛出一个 JobExecutionException 异常，并带有详细的错误信息和原始异常 e
                throw new JobExecutionException(
                        jobId, "Could not instantiate configured state backend", e);
            }

            // load the checkpoint storage from the application settings
            // 从应用程序设置中加载检查点存储
           // 定义一个 CheckpointStorage 类型的变量 applicationConfiguredStorage
            final CheckpointStorage applicationConfiguredStorage;
            // 获取默认的 CheckpointStorage 的序列化值
            final SerializedValue<CheckpointStorage> serializedAppConfiguredStorage =
                    snapshotSettings.getDefaultCheckpointStorage();

            if (serializedAppConfiguredStorage == null) {
                // 如果 serializedAppConfiguredStorage 为空，则设置 applicationConfiguredStorage 为 null
                applicationConfiguredStorage = null;
            } else {
                try {
                    // 尝试从 serializedAppConfiguredStorage 反序列化应用程序定义的检查点存储
                    applicationConfiguredStorage =
                            serializedAppConfiguredStorage.deserializeValue(classLoader);
                } catch (IOException | ClassNotFoundException e) {
                    //抛出异常
                    throw new JobExecutionException(
                            jobId,
                            "Could not deserialize application-defined checkpoint storage.",
                            e);
                }
            }
            // 定义一个 CheckpointStorage 类型的变量 rootStorage
            final CheckpointStorage rootStorage;
            try {
                // 尝试从 applicationConfiguredStorage、rootBackend、jobGraph 的配置、jobManagerConfig、classLoader 和日志中
                // 加载或配置 CheckpointStorage
                rootStorage =
                        CheckpointStorageLoader.load(
                                applicationConfiguredStorage,
                                rootBackend,
                                jobGraph.getJobConfiguration(),
                                jobManagerConfig,
                                classLoader,
                                log);
            } catch (IllegalConfigurationException | DynamicCodeLoadingException e) {
                //抛出异常
                throw new JobExecutionException(
                        jobId, "Could not instantiate configured checkpoint storage", e);
            }

            // instantiate the user-defined checkpoint hooks
            // 从快照设置中获取用户定义的MasterTriggerRestoreHook的序列化工厂数组
            final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks =
                    snapshotSettings.getMasterHooks();
            // 用于存储反序列化后的MasterTriggerRestoreHook对象的列表
            final List<MasterTriggerRestoreHook<?>> hooks;
            // 检查serializedHooks是否为空
            if (serializedHooks == null) {
                // 如果为空，则hooks列表为一个空的列表
                hooks = Collections.emptyList();
            } else {
                // 如果serializedHooks不为空，则尝试反序列化它
                final MasterTriggerRestoreHook.Factory[] hookFactories;
                try {
                    // 使用给定的类加载器反序列化serializedHooks
                    hookFactories = serializedHooks.deserializeValue(classLoader);
                } catch (IOException | ClassNotFoundException e) {
                    // 如果在反序列化过程中发生异常，则抛出JobExecutionException异常
                    throw new JobExecutionException(
                            jobId, "Could not instantiate user-defined checkpoint hooks", e);
                }
                // 保存当前线程的上下文类加载器，以便稍后恢复
                final Thread thread = Thread.currentThread();
                final ClassLoader originalClassLoader = thread.getContextClassLoader();
                // 临时设置当前线程的上下文类加载器为指定的类加载器
                thread.setContextClassLoader(classLoader);

                try {
                    // 创建一个新的ArrayList来存储MasterTriggerRestoreHook对象
                    hooks = new ArrayList<>(hookFactories.length);
                    // 遍历反序列化后的钩子工厂数组
                    for (MasterTriggerRestoreHook.Factory factory : hookFactories) {
                        // 使用工厂创建钩子，并使用wrapHook方法包装，然后添加到hooks列表中
                        hooks.add(MasterHooks.wrapHook(factory.create(), classLoader));
                    }
                } finally {
                    // 恢复线程的原始上下文类加载器
                    thread.setContextClassLoader(originalClassLoader);
                }
            }
            // 从快照设置中获取检查点协调器的配置
            final CheckpointCoordinatorConfiguration chkConfig =
                    snapshotSettings.getCheckpointCoordinatorConfiguration();
            // 启用作业图的检查点机制
            executionGraph.enableCheckpointing(
                    chkConfig,
                    hooks,
                    checkpointIdCounter,// 检查点ID计数器
                    completedCheckpointStore,// 已完成检查点的存储
                    rootBackend,// 后端存储
                    rootStorage,// 检查点存储
                    checkpointStatsTrackerFactory.get(),// 检查点统计追踪器工厂
                    checkpointsCleaner,
                    jobManagerConfig.get(STATE_CHANGE_LOG_STORAGE)); // 状态更改日志存储配置
        }
        // 返回配置好的executionGraph
        return executionGraph;
    }

    public static boolean isCheckpointingEnabled(JobGraph jobGraph) {
        return jobGraph.getCheckpointingSettings() != null;
    }

    // ------------------------------------------------------------------------

    /** This class is not supposed to be instantiated. */
    private DefaultExecutionGraphBuilder() {}
}
