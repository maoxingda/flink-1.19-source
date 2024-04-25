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
         * jobmanager.execution.attempts-history-size 默认16 历史中保留的历史执行尝试的最大次数
         */
        final int executionHistorySizeLimit =
                jobManagerConfig.get(JobManagerOptions.MAX_ATTEMPTS_HISTORY_SIZE);
        /**
         * 分区组策略
         *
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
        final int offloadShuffleDescriptorsThreshold =
                jobManagerConfig.get(
                        TaskDeploymentDescriptorFactory.OFFLOAD_SHUFFLE_DESCRIPTORS_THRESHOLD);
        /**
         * TaskDeploymentDescriptor的工厂
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
            /**
             * 设置json格式
             */
            executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));
        } catch (Throwable t) {
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
         *
         */
        for (JobVertex vertex : jobGraph.getVertices()) {
            /** 获取InvokableClass */
            String executableClass = vertex.getInvokableClassName();
            /** 如果executableClass 则抛出异常 */
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
        if (isDynamicGraph) {
            // dynamic graph does not support checkpointing so we skip it
            log.warn("Skip setting up checkpointing for a job with dynamic graph.");
        } else if (isCheckpointingEnabled(jobGraph)) {
            JobCheckpointingSettings snapshotSettings = jobGraph.getCheckpointingSettings();

            // load the state backend from the application settings
            final StateBackend applicationConfiguredBackend;
            final SerializedValue<StateBackend> serializedAppConfigured =
                    snapshotSettings.getDefaultStateBackend();

            if (serializedAppConfigured == null) {
                applicationConfiguredBackend = null;
            } else {
                try {
                    applicationConfiguredBackend =
                            serializedAppConfigured.deserializeValue(classLoader);
                } catch (IOException | ClassNotFoundException e) {
                    throw new JobExecutionException(
                            jobId, "Could not deserialize application-defined state backend.", e);
                }
            }

            final StateBackend rootBackend;
            try {
                rootBackend =
                        StateBackendLoader.fromApplicationOrConfigOrDefault(
                                applicationConfiguredBackend,
                                jobGraph.getJobConfiguration(),
                                jobManagerConfig,
                                classLoader,
                                log);
            } catch (IllegalConfigurationException | IOException | DynamicCodeLoadingException e) {
                throw new JobExecutionException(
                        jobId, "Could not instantiate configured state backend", e);
            }

            // load the checkpoint storage from the application settings
            final CheckpointStorage applicationConfiguredStorage;
            final SerializedValue<CheckpointStorage> serializedAppConfiguredStorage =
                    snapshotSettings.getDefaultCheckpointStorage();

            if (serializedAppConfiguredStorage == null) {
                applicationConfiguredStorage = null;
            } else {
                try {
                    applicationConfiguredStorage =
                            serializedAppConfiguredStorage.deserializeValue(classLoader);
                } catch (IOException | ClassNotFoundException e) {
                    throw new JobExecutionException(
                            jobId,
                            "Could not deserialize application-defined checkpoint storage.",
                            e);
                }
            }

            final CheckpointStorage rootStorage;
            try {
                rootStorage =
                        CheckpointStorageLoader.load(
                                applicationConfiguredStorage,
                                rootBackend,
                                jobGraph.getJobConfiguration(),
                                jobManagerConfig,
                                classLoader,
                                log);
            } catch (IllegalConfigurationException | DynamicCodeLoadingException e) {
                throw new JobExecutionException(
                        jobId, "Could not instantiate configured checkpoint storage", e);
            }

            // instantiate the user-defined checkpoint hooks

            final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks =
                    snapshotSettings.getMasterHooks();
            final List<MasterTriggerRestoreHook<?>> hooks;

            if (serializedHooks == null) {
                hooks = Collections.emptyList();
            } else {
                final MasterTriggerRestoreHook.Factory[] hookFactories;
                try {
                    hookFactories = serializedHooks.deserializeValue(classLoader);
                } catch (IOException | ClassNotFoundException e) {
                    throw new JobExecutionException(
                            jobId, "Could not instantiate user-defined checkpoint hooks", e);
                }

                final Thread thread = Thread.currentThread();
                final ClassLoader originalClassLoader = thread.getContextClassLoader();
                thread.setContextClassLoader(classLoader);

                try {
                    hooks = new ArrayList<>(hookFactories.length);
                    for (MasterTriggerRestoreHook.Factory factory : hookFactories) {
                        hooks.add(MasterHooks.wrapHook(factory.create(), classLoader));
                    }
                } finally {
                    thread.setContextClassLoader(originalClassLoader);
                }
            }

            final CheckpointCoordinatorConfiguration chkConfig =
                    snapshotSettings.getCheckpointCoordinatorConfiguration();

            executionGraph.enableCheckpointing(
                    chkConfig,
                    hooks,
                    checkpointIdCounter,
                    completedCheckpointStore,
                    rootBackend,
                    rootStorage,
                    checkpointStatsTrackerFactory.get(),
                    checkpointsCleaner,
                    jobManagerConfig.get(STATE_CHANGE_LOG_STORAGE));
        }

        return executionGraph;
    }

    public static boolean isCheckpointingEnabled(JobGraph jobGraph) {
        return jobGraph.getCheckpointingSettings() != null;
    }

    // ------------------------------------------------------------------------

    /** This class is not supposed to be instantiated. */
    private DefaultExecutionGraphBuilder() {}
}
