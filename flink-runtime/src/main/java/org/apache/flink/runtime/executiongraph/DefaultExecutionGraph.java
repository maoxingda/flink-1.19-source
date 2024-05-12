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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobStatusHook;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointFailureManager;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointPlanCalculator;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.DefaultCheckpointPlanCalculator;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.checkpoint.OperatorCoordinatorCheckpointContext;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.entrypoint.ClusterEntryPointExceptionUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.ResultPartitionAvailabilityChecker;
import org.apache.flink.runtime.executiongraph.failover.partitionrelease.PartitionGroupReleaseStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertex.FinalizeOnMasterContext;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.CoordinatorStoreImpl;
import org.apache.flink.runtime.query.KvStateLocationRegistry;
import org.apache.flink.runtime.scheduler.InternalFailuresListener;
import org.apache.flink.runtime.scheduler.SsgNetworkMemoryCalculationUtils;
import org.apache.flink.runtime.scheduler.VertexParallelismInformation;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.runtime.scheduler.adapter.DefaultExecutionTopology;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TernaryBoolean;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Default implementation of the {@link ExecutionGraph}. */
public class DefaultExecutionGraph implements ExecutionGraph, InternalExecutionGraphAccessor {

    /** The log object used for debugging. */
    static final Logger LOG = LoggerFactory.getLogger(ExecutionGraph.class);

    // --------------------------------------------------------------------------------------------

    /**
     * The unique id of an execution graph. It is different from JobID, because there can be
     * multiple execution graphs created from one job graph, in cases like job re-submission, job
     * master failover and job rescaling.
     */
    private final ExecutionGraphID executionGraphId;

    /** Job specific information like the job id, job name, job configuration, etc. */
    private final JobInformation jobInformation;

    /** The executor which is used to execute futures. */
    private final ScheduledExecutorService futureExecutor;

    /** The executor which is used to execute blocking io operations. */
    private final Executor ioExecutor;

    /** {@link CoordinatorStore} shared across all operator coordinators within this execution. */
    private final CoordinatorStore coordinatorStore = new CoordinatorStoreImpl();

    /** Executor that runs tasks in the job manager's main thread. */
    /** 在作业管理器的主线程中运行任务的执行器。 */
    @Nonnull private ComponentMainThreadExecutor jobMasterMainThreadExecutor;

    /** {@code true} if all source tasks are stoppable. */
    private boolean isStoppable = true;

    /** All job vertices that are part of this graph. */
    /** 所有节点 存放map 方便基于id进行获取 */
    private final Map<JobVertexID, ExecutionJobVertex> tasks;

    /** All vertices, in the order in which they were created. * */
    /** 所有顶点，按创建顺序排列 */
    private final List<ExecutionJobVertex> verticesInCreationOrder;

    /** All intermediate results that are part of this graph. */
    private final Map<IntermediateDataSetID, IntermediateResult> intermediateResults;

    /** The currently executed tasks, for callbacks. */
    /** 当前执行的任务，用于回调。 */
    private final Map<ExecutionAttemptID, Execution> currentExecutions;

    /**
     * Listeners that receive messages when the entire job switches it status (such as from RUNNING
     * to FINISHED).
     */
    private final List<JobStatusListener> jobStatusListeners;

    /**
     * Timestamps (in milliseconds as returned by {@code System.currentTimeMillis()} when the
     * execution graph transitioned into a certain state. The index into this array is the ordinal
     * of the enum value, i.e. the timestamp when the graph went into state "RUNNING" is at {@code
     * stateTimestamps[RUNNING.ordinal()]}.
     */
    private final long[] stateTimestamps;

    /** The timeout for all messages that require a response/acknowledgement. */
    private final Time rpcTimeout;

    /** The classloader for the user code. Needed for calls into user code classes. */
    private final ClassLoader userClassLoader;

    /** Registered KvState instances reported by the TaskManagers. */
    private final KvStateLocationRegistry kvStateLocationRegistry;

    /** Blob writer used to offload RPC messages. */
    private final BlobWriter blobWriter;

    /** Number of total job vertices. */
    /** 作业顶点总数 */
    private int numJobVerticesTotal;

    private final PartitionGroupReleaseStrategy.Factory partitionGroupReleaseStrategyFactory;

    private PartitionGroupReleaseStrategy partitionGroupReleaseStrategy;

    private DefaultExecutionTopology executionTopology;

    @Nullable private InternalFailuresListener internalTaskFailuresListener;

    /** Counts all restarts. Used by other Gauges/Meters and does not register to metric group. */
    private final Counter numberOfRestartsCounter = new SimpleCounter();

    // ------ Configuration of the Execution -------

    /** The maximum number of historical execution attempts kept in history. */
    private final int executionHistorySizeLimit;

    // ------ Execution status and progress. These values are volatile, and accessed under the lock
    // -------

    /** Number of finished job vertices. */
    private int numFinishedJobVertices;

    /** Current status of the job execution. */
    private volatile JobStatus state = JobStatus.CREATED;

    /** A future that completes once the job has reached a terminal state. */
    private final CompletableFuture<JobStatus> terminationFuture = new CompletableFuture<>();

    /**
     * The exception that caused the job to fail. This is set to the first root exception that was
     * not recoverable and triggered job failure.
     */
    private Throwable failureCause;

    /**
     * The extended failure cause information for the job. This exists in addition to
     * 'failureCause', to let 'failureCause' be a strong reference to the exception, while this info
     * holds no strong reference to any user-defined classes.
     */
    private ErrorInfo failureInfo;

    private final JobMasterPartitionTracker partitionTracker;

    private final ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker;

    /** Future for an ongoing or completed scheduling action. */
    @Nullable private CompletableFuture<Void> schedulingFuture;

    private final VertexAttemptNumberStore initialAttemptCounts;

    private final VertexParallelismStore parallelismStore;

    // ------ Fields that are relevant to the execution and need to be cleared before archiving
    // -------

    @Nullable private CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration;

    /** The coordinator for checkpoints, if snapshot checkpoints are enabled. */
    @Nullable private CheckpointCoordinator checkpointCoordinator;

    /** TODO, replace it with main thread executor. */
    @Nullable private ScheduledExecutorService checkpointCoordinatorTimer;

    /**
     * Checkpoint stats tracker separate from the coordinator in order to be available after
     * archiving.
     */
    @Nullable private CheckpointStatsTracker checkpointStatsTracker;

    // ------ Fields that are only relevant for archived execution graphs ------------
    @Nullable private String stateBackendName;

    @Nullable private String checkpointStorageName;

    @Nullable private String changelogStorageName;

    @Nullable private TernaryBoolean stateChangelogEnabled;

    private String jsonPlan;

    /** Shuffle master to register partitions for task deployment. */
    private final ShuffleMaster<?> shuffleMaster;

    private final ExecutionDeploymentListener executionDeploymentListener;
    private final ExecutionStateUpdateListener executionStateUpdateListener;

    private final EdgeManager edgeManager;

    private final Map<ExecutionVertexID, ExecutionVertex> executionVerticesById;
    private final Map<IntermediateResultPartitionID, IntermediateResultPartition>
            resultPartitionsById;

    private final VertexInputInfoStore vertexInputInfoStore;
    private final boolean isDynamic;

    private final ExecutionJobVertex.Factory executionJobVertexFactory;

    private final List<JobStatusHook> jobStatusHooks;

    private final MarkPartitionFinishedStrategy markPartitionFinishedStrategy;

    private final TaskDeploymentDescriptorFactory taskDeploymentDescriptorFactory;

    // --------------------------------------------------------------------------------------------
    //   Constructors
    // --------------------------------------------------------------------------------------------

    public DefaultExecutionGraph(
            JobInformation jobInformation,
            ScheduledExecutorService futureExecutor,
            Executor ioExecutor,
            Time rpcTimeout,
            int executionHistorySizeLimit,
            ClassLoader userClassLoader,
            BlobWriter blobWriter,
            PartitionGroupReleaseStrategy.Factory partitionGroupReleaseStrategyFactory,
            ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker partitionTracker,
            ExecutionDeploymentListener executionDeploymentListener,
            ExecutionStateUpdateListener executionStateUpdateListener,
            long initializationTimestamp,
            VertexAttemptNumberStore initialAttemptCounts,
            VertexParallelismStore vertexParallelismStore,
            boolean isDynamic,
            ExecutionJobVertex.Factory executionJobVertexFactory,
            List<JobStatusHook> jobStatusHooks,
            MarkPartitionFinishedStrategy markPartitionFinishedStrategy,
            TaskDeploymentDescriptorFactory taskDeploymentDescriptorFactory) {

        this.executionGraphId = new ExecutionGraphID();

        this.jobInformation = checkNotNull(jobInformation);

        this.blobWriter = checkNotNull(blobWriter);

        this.futureExecutor = checkNotNull(futureExecutor);
        this.ioExecutor = checkNotNull(ioExecutor);

        this.userClassLoader = checkNotNull(userClassLoader, "userClassLoader");

        this.tasks = CollectionUtil.newHashMapWithExpectedSize(16);
        this.intermediateResults = CollectionUtil.newHashMapWithExpectedSize(16);
        this.verticesInCreationOrder = new ArrayList<>(16);
        this.currentExecutions = CollectionUtil.newHashMapWithExpectedSize(16);

        this.jobStatusListeners = new ArrayList<>();

        this.stateTimestamps = new long[JobStatus.values().length];
        this.stateTimestamps[JobStatus.INITIALIZING.ordinal()] = initializationTimestamp;
        this.stateTimestamps[JobStatus.CREATED.ordinal()] = System.currentTimeMillis();

        this.rpcTimeout = checkNotNull(rpcTimeout);

        this.partitionGroupReleaseStrategyFactory =
                checkNotNull(partitionGroupReleaseStrategyFactory);

        this.kvStateLocationRegistry =
                new KvStateLocationRegistry(jobInformation.getJobId(), getAllVertices());

        this.executionHistorySizeLimit = executionHistorySizeLimit;

        this.schedulingFuture = null;
        this.jobMasterMainThreadExecutor =
                new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor(
                        "ExecutionGraph is not initialized with proper main thread executor. "
                                + "Call to ExecutionGraph.start(...) required.");

        this.shuffleMaster = checkNotNull(shuffleMaster);

        this.partitionTracker = checkNotNull(partitionTracker);

        this.resultPartitionAvailabilityChecker =
                new ExecutionGraphResultPartitionAvailabilityChecker(
                        this::createResultPartitionId, partitionTracker);

        this.executionDeploymentListener = executionDeploymentListener;
        this.executionStateUpdateListener = executionStateUpdateListener;

        this.initialAttemptCounts = initialAttemptCounts;

        this.parallelismStore = vertexParallelismStore;

        this.edgeManager = new EdgeManager();
        this.executionVerticesById = new HashMap<>();
        this.resultPartitionsById = new HashMap<>();
        this.vertexInputInfoStore = new VertexInputInfoStore();

        this.isDynamic = isDynamic;

        this.executionJobVertexFactory = checkNotNull(executionJobVertexFactory);

        this.jobStatusHooks = checkNotNull(jobStatusHooks);

        this.markPartitionFinishedStrategy = markPartitionFinishedStrategy;

        this.taskDeploymentDescriptorFactory = checkNotNull(taskDeploymentDescriptorFactory);

        LOG.info(
                "Created execution graph {} for job {}.",
                executionGraphId,
                jobInformation.getJobId());
        // Trigger hook onCreated
        notifyJobStatusHooks(state, null);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 设置主线程中运行任务的执行器。
    */
    @Override
    public void start(@Nonnull ComponentMainThreadExecutor jobMasterMainThreadExecutor) {
        this.jobMasterMainThreadExecutor = jobMasterMainThreadExecutor;
    }

    // --------------------------------------------------------------------------------------------
    //  Configuration of Data-flow wide execution settings
    // --------------------------------------------------------------------------------------------

    @Override
    public SchedulingTopology getSchedulingTopology() {
        return executionTopology;
    }

    @Override
    @Nonnull
    public ComponentMainThreadExecutor getJobMasterMainThreadExecutor() {
        return jobMasterMainThreadExecutor;
    }

    @Override
    public TernaryBoolean isChangelogStateBackendEnabled() {
        return stateChangelogEnabled;
    }

    @Override
    public Optional<String> getStateBackendName() {
        return Optional.ofNullable(stateBackendName);
    }

    @Override
    public Optional<String> getCheckpointStorageName() {
        return Optional.ofNullable(checkpointStorageName);
    }

    @Override
    public Optional<String> getChangelogStorageName() {
        return Optional.ofNullable(changelogStorageName);
    }

    @Override
    public void enableCheckpointing(
            CheckpointCoordinatorConfiguration chkConfig,
            List<MasterTriggerRestoreHook<?>> masterHooks,
            CheckpointIDCounter checkpointIDCounter,
            CompletedCheckpointStore checkpointStore,
            StateBackend checkpointStateBackend,
            CheckpointStorage checkpointStorage,
            CheckpointStatsTracker statsTracker,
            CheckpointsCleaner checkpointsCleaner,
            String changelogStorageName) {

        checkState(state == JobStatus.CREATED, "Job must be in CREATED state");
        checkState(checkpointCoordinator == null, "checkpointing already enabled");

        final Collection<OperatorCoordinatorCheckpointContext> operatorCoordinators =
                buildOpCoordinatorCheckpointContexts();

        checkpointStatsTracker = checkNotNull(statsTracker, "CheckpointStatsTracker");
        checkpointCoordinatorConfiguration =
                checkNotNull(chkConfig, "CheckpointCoordinatorConfiguration");

        CheckpointFailureManager failureManager =
                new CheckpointFailureManager(
                        chkConfig.getTolerableCheckpointFailureNumber(),
                        new CheckpointFailureManager.FailJobCallback() {
                            @Override
                            public void failJob(Throwable cause) {
                                getJobMasterMainThreadExecutor().execute(() -> failGlobal(cause));
                            }

                            @Override
                            public void failJobDueToTaskFailure(
                                    Throwable cause, ExecutionAttemptID failingTask) {
                                getJobMasterMainThreadExecutor()
                                        .execute(
                                                () ->
                                                        failGlobalIfExecutionIsStillRunning(
                                                                cause, failingTask));
                            }
                        });

        checkState(checkpointCoordinatorTimer == null);

        checkpointCoordinatorTimer =
                Executors.newSingleThreadScheduledExecutor(
                        new DispatcherThreadFactory(
                                Thread.currentThread().getThreadGroup(), "Checkpoint Timer"));

        // create the coordinator that triggers and commits checkpoints and holds the state
        checkpointCoordinator =
                new CheckpointCoordinator(
                        jobInformation.getJobId(),
                        chkConfig,
                        operatorCoordinators,
                        checkpointIDCounter,
                        checkpointStore,
                        checkpointStorage,
                        ioExecutor,
                        checkpointsCleaner,
                        new ScheduledExecutorServiceAdapter(checkpointCoordinatorTimer),
                        failureManager,
                        createCheckpointPlanCalculator(
                                chkConfig.isEnableCheckpointsAfterTasksFinish()),
                        checkpointStatsTracker);

        // register the master hooks on the checkpoint coordinator
        for (MasterTriggerRestoreHook<?> hook : masterHooks) {
            if (!checkpointCoordinator.addMasterHook(hook)) {
                LOG.warn(
                        "Trying to register multiple checkpoint hooks with the name: {}",
                        hook.getIdentifier());
            }
        }

        if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
            // the periodic checkpoint scheduler is activated and deactivated as a result of
            // job status changes (running -> on, all other states -> off)
            registerJobStatusListener(checkpointCoordinator.createActivatorDeactivator());
        }

        this.stateBackendName = checkpointStateBackend.getName();
        this.stateChangelogEnabled =
                TernaryBoolean.fromBoolean(
                        StateBackendLoader.isChangelogStateBackend(checkpointStateBackend));

        this.checkpointStorageName = checkpointStorage.getClass().getSimpleName();
        this.changelogStorageName = changelogStorageName;
    }

    private CheckpointPlanCalculator createCheckpointPlanCalculator(
            boolean enableCheckpointsAfterTasksFinish) {
        return new DefaultCheckpointPlanCalculator(
                getJobID(),
                new ExecutionGraphCheckpointPlanCalculatorContext(this),
                getVerticesTopologically(),
                enableCheckpointsAfterTasksFinish);
    }

    @Override
    @Nullable
    public CheckpointCoordinator getCheckpointCoordinator() {
        return checkpointCoordinator;
    }

    @Nullable
    @Override
    public CheckpointStatsTracker getCheckpointStatsTracker() {
        return checkpointStatsTracker;
    }

    @Override
    public KvStateLocationRegistry getKvStateLocationRegistry() {
        return kvStateLocationRegistry;
    }

    @Override
    public CheckpointCoordinatorConfiguration getCheckpointCoordinatorConfiguration() {
        if (checkpointCoordinatorConfiguration != null) {
            return checkpointCoordinatorConfiguration;
        } else {
            return null;
        }
    }

    @Override
    public CheckpointStatsSnapshot getCheckpointStatsSnapshot() {
        if (checkpointStatsTracker != null) {
            return checkpointStatsTracker.createSnapshot();
        } else {
            return null;
        }
    }

    private Collection<OperatorCoordinatorCheckpointContext>
            buildOpCoordinatorCheckpointContexts() {
        final ArrayList<OperatorCoordinatorCheckpointContext> contexts = new ArrayList<>();
        for (final ExecutionJobVertex vertex : verticesInCreationOrder) {
            contexts.addAll(vertex.getOperatorCoordinators());
        }
        contexts.trimToSize();
        return contexts;
    }

    // --------------------------------------------------------------------------------------------
    //  Properties and Status of the Execution Graph
    // --------------------------------------------------------------------------------------------

    @Override
    public void setJsonPlan(String jsonPlan) {
        this.jsonPlan = jsonPlan;
    }

    @Override
    public String getJsonPlan() {
        return jsonPlan;
    }

    @Override
    public JobID getJobID() {
        return jobInformation.getJobId();
    }

    @Override
    public String getJobName() {
        return jobInformation.getJobName();
    }

    @Override
    public boolean isStoppable() {
        return this.isStoppable;
    }

    @Override
    public Configuration getJobConfiguration() {
        return jobInformation.getJobConfiguration();
    }

    @Override
    public ClassLoader getUserClassLoader() {
        return this.userClassLoader;
    }

    @Override
    public JobStatus getState() {
        return state;
    }

    @Override
    public Throwable getFailureCause() {
        return failureCause;
    }

    public ErrorInfo getFailureInfo() {
        return failureInfo;
    }

    @Override
    public long getNumberOfRestarts() {
        return numberOfRestartsCounter.getCount();
    }

    @Override
    public int getNumFinishedVertices() {
        return IterableUtils.toStream(getVerticesTopologically())
                .map(ExecutionJobVertex::getNumExecutionVertexFinished)
                .mapToInt(Integer::intValue)
                .sum();
    }

    @Override
    public ExecutionJobVertex getJobVertex(JobVertexID id) {
        return this.tasks.get(id);
    }

    @Override
    public Map<JobVertexID, ExecutionJobVertex> getAllVertices() {
        return Collections.unmodifiableMap(this.tasks);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 获取ExecutionJobVertex顶点迭代器
     * 里面存放的就是ExecutionJobVertex对象
    */
    @Override
    public Iterable<ExecutionJobVertex> getVerticesTopologically() {
        // we return a specific iterator that does not fail with concurrent modifications
        // the list is append only, so it is safe for that
        /** 获取ExecutionGraph中ExecutionJobVertex数量 3 */
        final int numElements = this.verticesInCreationOrder.size();
        /** 返回一个新的 Iterable 对象 */
        return new Iterable<ExecutionJobVertex>() {
            @Override
            public Iterator<ExecutionJobVertex> iterator() {
                return new Iterator<ExecutionJobVertex>() {
                    /** 私有变量 `pos` 代表起始位置 */
                    private int pos = 0;
                    /** 这个变量用于跟踪当前迭代的位置。 */
                    /**
                     * 这个方法检查是否还有更多的元素可以迭代。
                     * 它比较 `pos` 和 `numElements` 的值，
                     * 如果 `pos` 小于 `numElements`，则返回 `true`，否则返回 `false`。
                     * @return
                     */
                    @Override
                    public boolean hasNext() {
                        return pos < numElements;
                    }

                    /** 返回迭代中的下一个元素 */
                    @Override
                    public ExecutionJobVertex next() {
                        /** 它检查是否还有更多的元素（通过调用 `hasNext()`） */
                        /**
                         *它检查是否还有更多的元素（通过调用 `hasNext()`）。
                         */
                        if (hasNext()) {
                            /** 它就从 `verticesInCreationOrder` 列表中获取当前位置的元素 */
                            return verticesInCreationOrder.get(pos++);
                        } else {
                            /** 如果没有更多的元素，则抛出一个 `NoSuchElementException` */
                            throw new NoSuchElementException();
                        }
                    }

                    /**
                     * 删除操作是不支持的，所以方法直接抛出一个 `UnsupportedOperationException`。
                     */
                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    @Override
    public Map<IntermediateDataSetID, IntermediateResult> getAllIntermediateResults() {
        return Collections.unmodifiableMap(this.intermediateResults);
    }

    @Override
    public Iterable<ExecutionVertex> getAllExecutionVertices() {
        return () -> new AllVerticesIterator<>(getVerticesTopologically().iterator());
    }

    @Override
    public EdgeManager getEdgeManager() {
        return edgeManager;
    }

    @Override
    public ExecutionVertex getExecutionVertexOrThrow(ExecutionVertexID id) {
        return checkNotNull(executionVerticesById.get(id));
    }

    @Override
    public IntermediateResultPartition getResultPartitionOrThrow(
            final IntermediateResultPartitionID id) {
        return checkNotNull(resultPartitionsById.get(id));
    }

    @Override
    public long getStatusTimestamp(JobStatus status) {
        return this.stateTimestamps[status.ordinal()];
    }

    @Override
    public final BlobWriter getBlobWriter() {
        return blobWriter;
    }

    @Override
    public Executor getFutureExecutor() {
        return futureExecutor;
    }

    @Override
    public Map<String, OptionalFailure<Accumulator<?, ?>>> aggregateUserAccumulators() {

        Map<String, OptionalFailure<Accumulator<?, ?>>> userAccumulators = new HashMap<>();

        for (ExecutionVertex vertex : getAllExecutionVertices()) {
            Map<String, Accumulator<?, ?>> next =
                    vertex.getCurrentExecutionAttempt().getUserAccumulators();
            if (next != null) {
                AccumulatorHelper.mergeInto(userAccumulators, next);
            }
        }

        return userAccumulators;
    }

    /**
     * Gets a serialized accumulator map.
     *
     * @return The accumulator map with serialized accumulator values.
     */
    @Override
    public Map<String, SerializedValue<OptionalFailure<Object>>> getAccumulatorsSerialized() {
        return aggregateUserAccumulators().entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> serializeAccumulator(entry.getKey(), entry.getValue())));
    }

    private static SerializedValue<OptionalFailure<Object>> serializeAccumulator(
            String name, OptionalFailure<Accumulator<?, ?>> accumulator) {
        try {
            if (accumulator.isFailure()) {
                return new SerializedValue<>(
                        OptionalFailure.ofFailure(accumulator.getFailureCause()));
            }
            return new SerializedValue<>(
                    OptionalFailure.of(accumulator.getUnchecked().getLocalValue()));
        } catch (IOException ioe) {
            LOG.error("Could not serialize accumulator " + name + '.', ioe);
            try {
                return new SerializedValue<>(OptionalFailure.ofFailure(ioe));
            } catch (IOException e) {
                throw new RuntimeException(
                        "It should never happen that we cannot serialize the accumulator serialization exception.",
                        e);
            }
        }
    }

    /**
     * Returns the a stringified version of the user-defined accumulators.
     *
     * @return an Array containing the StringifiedAccumulatorResult objects
     */
    @Override
    public StringifiedAccumulatorResult[] getAccumulatorResultsStringified() {
        Map<String, OptionalFailure<Accumulator<?, ?>>> accumulatorMap =
                aggregateUserAccumulators();
        return StringifiedAccumulatorResult.stringifyAccumulatorResults(accumulatorMap);
    }

    @Override
    public void setInternalTaskFailuresListener(
            final InternalFailuresListener internalTaskFailuresListener) {
        checkNotNull(internalTaskFailuresListener);
        checkState(
                this.internalTaskFailuresListener == null,
                "internalTaskFailuresListener can be only set once");
        this.internalTaskFailuresListener = internalTaskFailuresListener;
    }

    // --------------------------------------------------------------------------------------------
    //  Actions
    // --------------------------------------------------------------------------------------------

    @Override
    public void notifyNewlyInitializedJobVertices(List<ExecutionJobVertex> vertices) {
        executionTopology.notifyExecutionGraphUpdated(this, vertices);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 转换初始化ExecutionGraph
    */
    @Override
    public void attachJobGraph(
            List<JobVertex> verticesToAttach, JobManagerJobMetricGroup jobManagerJobMetricGroup)
            throws JobException {
        /** 检查当前线程是否是作业主节点（Job Master）的主线程 */
        assertRunningInJobMasterMainThread();
        /** 打印日志 */
        LOG.debug(
                "Attaching {} topologically sorted vertices to existing job graph with {} "
                        + "vertices and {} intermediate results.",
                verticesToAttach.size(),
                tasks.size(),
                intermediateResults.size());
        /** 转换附加作业顶点 */
        attachJobVertices(verticesToAttach, jobManagerJobMetricGroup);
        /**如果非动态则初始化JobVertices*/
        if (!isDynamic) {
            /** 初始化JobVertices */
            initializeJobVertices(verticesToAttach);
        }

        // the topology assigning should happen before notifying new vertices to failoverStrategy
        /** 根据一个 DefaultExecutionGraph 对象来创建一个 DefaultExecutionTopology 对象 */
        executionTopology = DefaultExecutionTopology.fromExecutionGraph(this);

        /**
         * 释放与 链接相关的阻塞中间结果分区SchedulelingPipelinedRegion只要区域的执行顶点完成。
         */
        partitionGroupReleaseStrategy =
                partitionGroupReleaseStrategyFactory.createInstance(getSchedulingTopology());
    }

    /** Attach job vertices without initializing them. */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 附加作业顶点
    */
    private void attachJobVertices(
            List<JobVertex> topologicallySorted, JobManagerJobMetricGroup jobManagerJobMetricGroup)
            throws JobException {
        /**
         * 循环JobVertex
         */
        for (JobVertex jobVertex : topologicallySorted) {
            /**
             * 检查是input是否为空 source input为空
             * 检查jobVertex.isStoppable是否为true
             */
            if (jobVertex.isInputVertex() && !jobVertex.isStoppable()) {
                this.isStoppable = false;
            }
            /** 获取顶点并行度相关的信息 最大并行度、最小并行度、当前并行度*/
            VertexParallelismInformation parallelismInfo =
                    parallelismStore.getParallelismInfo(jobVertex.getID());

            // create the execution job vertex and attach it to the graph
            /** 创建执行作业顶点 */
            ExecutionJobVertex ejv =
                    executionJobVertexFactory.createExecutionJobVertex(
                            this,
                            jobVertex,
                            parallelismInfo,
                            coordinatorStore,
                            jobManagerJobMetricGroup);
            /**
             * 将ExecutionJobVertex 放入map结构 key=id,value=ExecutionJobVertex对象
             */
            ExecutionJobVertex previousTask = this.tasks.putIfAbsent(jobVertex.getID(), ejv);
            /**
             * 如果存在相同的则抛出异常
             */
            if (previousTask != null) {
                throw new JobException(
                        String.format(
                                "Encountered two job vertices with ID %s : previous=[%s] / new=[%s]",
                                jobVertex.getID(), ejv, previousTask));
            }
            /**
             * 按照创建顺序放入List<ExecutionJobVertex> verticesInCreationOrder集合
             */
            this.verticesInCreationOrder.add(ejv);
            /** 作业顶点总数*/
            this.numJobVerticesTotal++;
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 初始化JobVertices，内部构建ExecutionVertices
    */
    private void initializeJobVertices(List<JobVertex> topologicallySorted) throws JobException {
        /** 获取当前时间 */
        final long createTimestamp = System.currentTimeMillis();
        /** 循环便利Job Vertex */
        for (JobVertex jobVertex : topologicallySorted) {
            /** 根据ID获取 ExecutionJobVertex */
            final ExecutionJobVertex ejv = tasks.get(jobVertex.getID());
            /** 初始化 ExecutionJobVertex */
            initializeJobVertex(ejv, createTimestamp);
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 初始化给定的执行作业顶点，主要包括根据并行度创建执行顶点，以及连接到前置任务。
     */
    @Override
    public void initializeJobVertex(
            ExecutionJobVertex ejv,
            long createTimestamp,
            Map<IntermediateDataSetID, JobVertexInputInfo> jobVertexInputInfos)
            throws JobException {
        /** 检查是否为 null */
        checkNotNull(ejv);
        checkNotNull(jobVertexInputInfos);
        /**
         * Map<JobVertexID, Map<IntermediateDataSetID, JobVertexInputInfo>>
         * 遍历 jobVertexInputInfos 映射，并将每个输入信息存储到 vertexInputInfoStore 中，
         * 键是 ejv 的作业顶点 ID 和 resultId。
         */
        jobVertexInputInfos.forEach(
                (resultId, info) ->
                        this.vertexInputInfoStore.put(ejv.getJobVertexId(), resultId, info));
        /**
         * 构建ExecutionVertex、以及中间结果
         */
        ejv.initialize(
                executionHistorySizeLimit,
                rpcTimeout,
                createTimestamp,
                this.initialAttemptCounts.getAttemptCounts(ejv.getJobVertexId()));
        /**
         * 设置顶点链接
          */
        ejv.connectToPredecessors(this.intermediateResults);
        /**
         * 遍历ejv产生的所有中间结果集，并尝试将它们添加到 intermediateResults 映射中。
         * 如果某个 ID 的结果集已经存在，则抛出一个异常。
         */
        for (IntermediateResult res : ejv.getProducedDataSets()) {
            IntermediateResult previousDataSet =
                    this.intermediateResults.putIfAbsent(res.getId(), res);
            if (previousDataSet != null) {
                throw new JobException(
                        String.format(
                                "Encountered two intermediate data set with ID %s : previous=[%s] / new=[%s]",
                                res.getId(), res, previousDataSet));
            }
        }
        /**
         * Map<ExecutionVertexID, ExecutionVertex> executionVerticesById;
         * Map<IntermediateResultPartitionID, IntermediateResultPartition>
         *  注册执行顶点和结果分区
         */
        registerExecutionVerticesAndResultPartitionsFor(ejv);

        // enrich network memory.
        /** 获取Slot共享组*/
        SlotSharingGroup slotSharingGroup = ejv.getSlotSharingGroup();
        /**
         * 构建ResourceProfile
         */
        if (areJobVerticesAllInitialized(slotSharingGroup)) {
            SsgNetworkMemoryCalculationUtils.enrichNetworkMemory(
                    slotSharingGroup, this::getJobVertex, shuffleMaster);
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 判断SlotSharingGroup组下的JobVertex是否初始化
    */
    private boolean areJobVerticesAllInitialized(final SlotSharingGroup group) {
        /** 调用 group.getJobVertexIds() 方法，获取槽共享组中所有作业顶点的 ID，并遍历这些 ID。 */
        for (JobVertexID jobVertexId : group.getJobVertexIds()) {
            /** 通过jobVertexId 获得ExecutionJobVertex对象*/
            final ExecutionJobVertex jobVertex = getJobVertex(jobVertexId);
            /** 检查是否为null */
            checkNotNull(jobVertex, "Unknown job vertex %s", jobVertexId);
            /** 检查作业顶点是否初始化 */
            if (!jobVertex.isInitialized()) {
                return false;
            }
        }
        return true;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 状态转换
     */
    @Override
    public void transitionToRunning() {
        /** 将状态从CREATE 转换为RUNNING 如果失败则抛出异常 */
        if (!transitionState(JobStatus.CREATED, JobStatus.RUNNING)) {
            throw new IllegalStateException(
                    "Job may only be scheduled from state " + JobStatus.CREATED);
        }
    }

    @Override
    public void cancel() {

        assertRunningInJobMasterMainThread();

        while (true) {
            JobStatus current = state;

            if (current == JobStatus.RUNNING
                    || current == JobStatus.CREATED
                    || current == JobStatus.RESTARTING) {
                if (transitionState(current, JobStatus.CANCELLING)) {
                    resetExecutionGraph(ExecutionJobVertex::cancelWithFuture)
                            .whenComplete(
                                    (Void value, Throwable throwable) -> {
                                        if (throwable != null) {
                                            transitionState(
                                                    JobStatus.CANCELLING,
                                                    JobStatus.FAILED,
                                                    new FlinkException(
                                                            "Could not cancel job "
                                                                    + getJobName()
                                                                    + " because not all execution job vertices could be cancelled.",
                                                            throwable));
                                        } else {
                                            // cancellations may currently be overridden by failures
                                            // which trigger restarts, so we need to pass a proper
                                            // restart global version here
                                            allVerticesInTerminalState();
                                        }
                                    });

                    return;
                }
            }
            // Executions are being canceled. Go into cancelling and wait for
            // all vertices to be in their final state.
            else if (current == JobStatus.FAILING) {
                if (transitionState(current, JobStatus.CANCELLING)) {
                    return;
                }
            } else {
                // no need to treat other states
                return;
            }
        }
    }

    private CompletableFuture<Void> resetExecutionGraph(
            Function<ExecutionJobVertex, CompletableFuture<Void>> perVertexOperationAsync) {
        assertRunningInJobMasterMainThread();

        incrementRestarts();

        // cancel ongoing scheduling action
        if (schedulingFuture != null) {
            schedulingFuture.cancel(false);
        }

        return applyToVertexAsync(perVertexOperationAsync);
    }

    private CompletableFuture<Void> applyToVertexAsync(
            Function<ExecutionJobVertex, CompletableFuture<Void>> perVertexOperationAsync) {
        return FutureUtils.waitForAll(
                verticesInCreationOrder.stream()
                        .map(perVertexOperationAsync)
                        .collect(Collectors.toList()));
    }

    @Override
    public void suspend(Throwable suspensionCause) {

        assertRunningInJobMasterMainThread();

        if (state.isTerminalState()) {
            // stay in a terminal state
            return;
        } else if (transitionState(state, JobStatus.SUSPENDED, suspensionCause)) {
            initFailureCause(suspensionCause, System.currentTimeMillis());

            final CompletableFuture<Void> jobVerticesTerminationFuture =
                    resetExecutionGraph(ExecutionJobVertex::suspend);

            checkState(jobVerticesTerminationFuture.isDone(), "Suspend needs to happen atomically");

            jobVerticesTerminationFuture.whenComplete(
                    (Void ignored, Throwable throwable) -> {
                        if (throwable != null) {
                            LOG.debug("Could not properly suspend the execution graph.", throwable);
                        }

                        onTerminalState(state);
                        LOG.info("Job {} has been suspended.", getJobID());
                    });
        } else {
            throw new IllegalStateException(
                    String.format(
                            "Could not suspend because transition from %s to %s failed.",
                            state, JobStatus.SUSPENDED));
        }
    }

    void failGlobalIfExecutionIsStillRunning(Throwable cause, ExecutionAttemptID failingAttempt) {
        final Execution failedExecution = currentExecutions.get(failingAttempt);
        if (failedExecution != null
                && (failedExecution.getState() == ExecutionState.RUNNING
                        || failedExecution.getState() == ExecutionState.INITIALIZING)) {
            failGlobal(cause);
        } else {
            LOG.debug(
                    "The failing attempt {} belongs to an already not"
                            + " running task thus won't fail the job",
                    failingAttempt);
        }
    }

    @Override
    public void failGlobal(Throwable t) {
        checkState(internalTaskFailuresListener != null);
        internalTaskFailuresListener.notifyGlobalFailure(t);
    }

    /**
     * Returns the serializable {@link ArchivedExecutionConfig}.
     *
     * @return ArchivedExecutionConfig which may be null in case of errors
     */
    @Override
    public ArchivedExecutionConfig getArchivedExecutionConfig() {
        // create a summary of all relevant data accessed in the web interface's JobConfigHandler
        try {
            ExecutionConfig executionConfig =
                    jobInformation.getSerializedExecutionConfig().deserializeValue(userClassLoader);
            if (executionConfig != null) {
                return executionConfig.archive();
            }
        } catch (IOException | ClassNotFoundException e) {
            LOG.error("Couldn't create ArchivedExecutionConfig for job {} ", getJobID(), e);
        }
        return null;
    }

    @Override
    public CompletableFuture<JobStatus> getTerminationFuture() {
        return terminationFuture;
    }

    @Override
    @VisibleForTesting
    public JobStatus waitUntilTerminal() throws InterruptedException {
        try {
            return terminationFuture.get();
        } catch (ExecutionException e) {
            // this should never happen
            // it would be a bug, so we  don't expect this to be handled and throw
            // an unchecked exception here
            throw new RuntimeException(e);
        }
    }

    // ------------------------------------------------------------------------
    //  State Transitions
    // ------------------------------------------------------------------------
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 状态转换
    */
    @Override
    public boolean transitionState(JobStatus current, JobStatus newState) {
        /** 状态转换 */
        return transitionState(current, newState, null);
    }

    private void transitionState(JobStatus newState, Throwable error) {
        transitionState(state, newState, error);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 状态转换
     * 用于将作业的状态从当前状态（current）转换到新的状态（newState）
    */
    private boolean transitionState(JobStatus current, JobStatus newState, Throwable error) {
        assertRunningInJobMasterMainThread();
        // consistency check
        /** 如果是终结状态则抛出异常 */
        if (current.isTerminalState()) {
            String message = "Job is trying to leave terminal state " + current;
            LOG.error(message);
            throw new IllegalStateException(message);
        }

        // now do the actual state transition
        /** 现在进行实际的状态转换 */
        if (state == current) {
            state = newState;
            LOG.info(
                    "Job {} ({}) switched from state {} to {}.",
                    getJobName(),
                    getJobID(),
                    current,
                    newState,
                    error);
            /**
             * 设置RUNNING的时间为当前时间
             */
            stateTimestamps[newState.ordinal()] = System.currentTimeMillis();
            /** 触发状态修改 */
            notifyJobStatusChange(newState);
            /** 触发状态对应的钩子*/
            notifyJobStatusHooks(newState, error);
            /** 转换成功返回 true */
            return true;
        } else {
            /** 转换成功返回 false */
            return false;
        }
    }

    @Override
    public void incrementRestarts() {
        numberOfRestartsCounter.inc();
    }

    @Override
    public void initFailureCause(Throwable t, long timestamp) {
        this.failureCause = t;
        this.failureInfo = new ErrorInfo(t, timestamp);
    }

    // ------------------------------------------------------------------------
    //  Job Status Progress
    // ------------------------------------------------------------------------

    /**
     * Called whenever a job vertex reaches state FINISHED (completed successfully). Once all job
     * vertices are in the FINISHED state, the program is successfully done.
     */
    @Override
    public void jobVertexFinished() {
        assertRunningInJobMasterMainThread();
        final int numFinished = ++numFinishedJobVertices;
        if (numFinished == numJobVerticesTotal) {
            FutureUtils.assertNoException(
                    waitForAllExecutionsTermination().thenAccept(ignored -> jobFinished()));
        }
    }

    private CompletableFuture<?> waitForAllExecutionsTermination() {
        final List<CompletableFuture<?>> terminationFutures =
                verticesInCreationOrder.stream()
                        .flatMap(ejv -> Arrays.stream(ejv.getTaskVertices()))
                        .map(ExecutionVertex::getTerminationFuture)
                        .collect(Collectors.toList());

        return FutureUtils.waitForAll(terminationFutures);
    }

    private void jobFinished() {
        assertRunningInJobMasterMainThread();

        // check whether we are still in "RUNNING" and trigger the final cleanup
        if (state == JobStatus.RUNNING) {
            // we do the final cleanup in the I/O executor, because it may involve
            // some heavier work

            try {
                for (ExecutionJobVertex ejv : verticesInCreationOrder) {
                    final Map<Integer, Integer> subtaskToFinishedAttempt =
                            Arrays.stream(ejv.getTaskVertices())
                                    .map(ExecutionVertex::getCurrentExecutionAttempt)
                                    .collect(
                                            Collectors.toMap(
                                                    Execution::getParallelSubtaskIndex,
                                                    Execution::getAttemptNumber));
                    ejv.getJobVertex()
                            .finalizeOnMaster(
                                    new FinalizeOnMasterContext() {
                                        @Override
                                        public ClassLoader getClassLoader() {
                                            return getUserClassLoader();
                                        }

                                        @Override
                                        public int getExecutionParallelism() {
                                            return ejv.getParallelism();
                                        }

                                        @Override
                                        public int getFinishedAttempt(int subtaskIndex) {
                                            final Integer attemptNumber =
                                                    subtaskToFinishedAttempt.get(subtaskIndex);
                                            if (attemptNumber == null) {
                                                throw new IllegalArgumentException(
                                                        "Invalid subtaskIndex "
                                                                + subtaskIndex
                                                                + " provided");
                                            }
                                            return attemptNumber;
                                        }
                                    });
                }
            } catch (Throwable t) {
                ExceptionUtils.rethrowIfFatalError(t);
                ClusterEntryPointExceptionUtils.tryEnrichClusterEntryPointError(t);
                failGlobal(new Exception("Failed to finalize execution on master", t));
                return;
            }

            // if we do not make this state transition, then a concurrent
            // cancellation or failure happened
            if (transitionState(JobStatus.RUNNING, JobStatus.FINISHED)) {
                onTerminalState(JobStatus.FINISHED);
            }
        }
    }

    @Override
    public void jobVertexUnFinished() {
        assertRunningInJobMasterMainThread();
        numFinishedJobVertices--;
    }

    /**
     * This method is a callback during cancellation/failover and called when all tasks have reached
     * a terminal state (cancelled/failed/finished).
     */
    private void allVerticesInTerminalState() {

        assertRunningInJobMasterMainThread();

        // we are done, transition to the final state
        JobStatus current;
        while (true) {
            current = this.state;

            if (current == JobStatus.RUNNING) {
                failGlobal(
                        new Exception(
                                "ExecutionGraph went into allVerticesInTerminalState() from RUNNING"));
            } else if (current == JobStatus.CANCELLING) {
                if (transitionState(current, JobStatus.CANCELED)) {
                    onTerminalState(JobStatus.CANCELED);
                    break;
                }
            } else if (current == JobStatus.FAILING) {
                break;
            } else if (current.isGloballyTerminalState()) {
                LOG.warn(
                        "Job has entered globally terminal state without waiting for all "
                                + "job vertices to reach final state.");
                break;
            } else {
                failGlobal(
                        new Exception(
                                "ExecutionGraph went into final state from state " + current));
                break;
            }
        }
        // done transitioning the state
    }

    @Override
    public void failJob(Throwable cause, long timestamp) {
        if (state == JobStatus.FAILING || state.isTerminalState()) {
            return;
        }

        transitionState(JobStatus.FAILING, cause);
        initFailureCause(cause, timestamp);

        FutureUtils.assertNoException(
                applyToVertexAsync(ExecutionJobVertex::cancelWithFuture)
                        .whenComplete(
                                (aVoid, throwable) -> {
                                    if (transitionState(
                                            JobStatus.FAILING, JobStatus.FAILED, cause)) {
                                        onTerminalState(JobStatus.FAILED);
                                    } else if (state == JobStatus.CANCELLING) {
                                        transitionState(JobStatus.CANCELLING, JobStatus.CANCELED);
                                        onTerminalState(JobStatus.CANCELED);
                                    } else if (!state.isTerminalState()) {
                                        throw new IllegalStateException(
                                                "Cannot complete job failing from an unexpected state: "
                                                        + state);
                                    }
                                }));
    }

    private void onTerminalState(JobStatus status) {
        LOG.debug("ExecutionGraph {} reached terminal state {}.", getJobID(), status);

        try {
            CheckpointCoordinator coord = this.checkpointCoordinator;
            this.checkpointCoordinator = null;
            if (coord != null) {
                coord.shutdown();
            }
            if (checkpointCoordinatorTimer != null) {
                checkpointCoordinatorTimer.shutdownNow();
                checkpointCoordinatorTimer = null;
            }
        } catch (Exception e) {
            LOG.error("Error while cleaning up after execution", e);
        } finally {
            terminationFuture.complete(status);
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Callbacks and Callback Utilities
    // --------------------------------------------------------------------------------------------

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 更新与给定状态转换相关的任务执行状态。
     * @param state 包含任务执行状态转换信息的对象
     * @return 如果成功更新任务执行状态，则返回true；否则返回false
    */
    @Override
    public boolean updateState(TaskExecutionStateTransition state) {
        // 这是为了确保状态更新操作在正确的线程环境中执行
        assertRunningInJobMasterMainThread();
        // 从currentExecutions映射中获取与给定状态转换的ID关联的Execution对象
        final Execution attempt = currentExecutions.get(state.getID());

        if (attempt != null) {
            try {
                // 尝试更新Execution对象的状态
                final boolean stateUpdated = updateStateInternal(state, attempt);
                //可能需要释放与Execution对象关联的分区组
                maybeReleasePartitionGroupsFor(attempt);
                // 如果状态成功更新，则返回true
                return stateUpdated;
            } catch (Throwable t) {
                //处理异常
                ExceptionUtils.rethrowIfFatalErrorOrOOM(t);

                // failures during updates leave the ExecutionGraph inconsistent
                failGlobal(t);
                // 返回false表示状态更新失败
                return false;
            }
        } else {
            // 返回false表示状态更新失败
            return false;
        }
    }

    private boolean updateStateInternal(
            final TaskExecutionStateTransition state, final Execution attempt) {
        Map<String, Accumulator<?, ?>> accumulators;

        switch (state.getExecutionState()) {
            case INITIALIZING:
                return attempt.switchToInitializing();

            case RUNNING:
                return attempt.switchToRunning();

            case FINISHED:
                // this deserialization is exception-free
                accumulators = deserializeAccumulators(state);
                attempt.markFinished(accumulators, state.getIOMetrics());
                return true;

            case CANCELED:
                // this deserialization is exception-free
                accumulators = deserializeAccumulators(state);
                attempt.completeCancelling(accumulators, state.getIOMetrics(), false);
                return true;

            case FAILED:
                // this deserialization is exception-free
                accumulators = deserializeAccumulators(state);
                attempt.markFailed(
                        state.getError(userClassLoader),
                        state.getCancelTask(),
                        accumulators,
                        state.getIOMetrics(),
                        state.getReleasePartitions(),
                        true);
                return true;

            default:
                // we mark as failed and return false, which triggers the TaskManager
                // to remove the task
                attempt.fail(
                        new Exception(
                                "TaskManager sent illegal state update: "
                                        + state.getExecutionState()));
                return false;
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 根据Execution的状态，释放与其相关的分区组。
     * @param attempt 已完成的Execution对象
    */
    private void maybeReleasePartitionGroupsFor(final Execution attempt) {
        // 获取Execution对应的ExecutionVertex的ID
        final ExecutionVertexID finishedExecutionVertex = attempt.getVertex().getID();
        // 检查Execution的状态
        if (attempt.getState() == ExecutionState.FINISHED) {
            // 如果Execution状态为已完成（FINISHED）
            // 调用partitionGroupReleaseStrategy的vertexFinished方法来获取可以释放的分区组列表
            final List<ConsumedPartitionGroup> releasablePartitionGroups =
                    partitionGroupReleaseStrategy.vertexFinished(finishedExecutionVertex);
            // 释放这些分区组
            releasePartitionGroups(releasablePartitionGroups);
        } else {
            // 调用partitionGroupReleaseStrategy的vertexUnfinished方法，更新分区组的状态或进行其他相关处理
            partitionGroupReleaseStrategy.vertexUnfinished(finishedExecutionVertex);
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 释放指定的分区组。
     * @param releasablePartitionGroups 准备释放的分区组列表
    */
    private void releasePartitionGroups(
            final List<ConsumedPartitionGroup> releasablePartitionGroups) {
        // 如果存在可以释放的分区组
        if (releasablePartitionGroups.size() > 0) {
            // 创建一个用于存储可释放分区ID的列表
            final List<ResultPartitionID> releasablePartitionIds = new ArrayList<>();

            // Remove the cache of ShuffleDescriptors when ConsumedPartitionGroups are released
            // 遍历所有准备释放的分区组
            for (ConsumedPartitionGroup releasablePartitionGroup : releasablePartitionGroups) {
                // 获取与分区组关联的IntermediateResult
                IntermediateResult totalResult =
                        checkNotNull(
                                intermediateResults.get(
                                        releasablePartitionGroup.getIntermediateDataSetID()));
                // 遍历分区组中的每个分区ID
                for (IntermediateResultPartitionID partitionId : releasablePartitionGroup) {
                    // 获取对应的IntermediateResultPartition
                    IntermediateResultPartition partition =
                            totalResult.getPartitionById(partitionId);
                    // 标记该分区组为可释放状态
                    partition.markPartitionGroupReleasable(releasablePartitionGroup);
                    // 检查该分区是否可以被释放
                    if (partition.canBeReleased()) {
                        // 如果可以，创建ResultPartitionID并添加到可释放的分区ID列表中
                        releasablePartitionIds.add(createResultPartitionId(partitionId));
                    }
                }
                // 清除与分区组相关的缓存信息
                totalResult.clearCachedInformationForPartitionGroup(releasablePartitionGroup);
            }
            // 停止跟踪并释放所有可释放的分区
            partitionTracker.stopTrackingAndReleasePartitions(releasablePartitionIds);
        }
    }

    ResultPartitionID createResultPartitionId(
            final IntermediateResultPartitionID resultPartitionId) {
        final SchedulingResultPartition schedulingResultPartition =
                getSchedulingTopology().getResultPartition(resultPartitionId);
        final SchedulingExecutionVertex producer = schedulingResultPartition.getProducer();
        final ExecutionVertexID producerId = producer.getId();
        final JobVertexID jobVertexId = producerId.getJobVertexId();
        final ExecutionJobVertex jobVertex = getJobVertex(jobVertexId);
        checkNotNull(jobVertex, "Unknown job vertex %s", jobVertexId);

        final ExecutionVertex[] taskVertices = jobVertex.getTaskVertices();
        final int subtaskIndex = producerId.getSubtaskIndex();
        checkState(
                subtaskIndex < taskVertices.length,
                "Invalid subtask index %d for job vertex %s",
                subtaskIndex,
                jobVertexId);

        final ExecutionVertex taskVertex = taskVertices[subtaskIndex];
        final Execution execution = taskVertex.getCurrentExecutionAttempt();
        return new ResultPartitionID(resultPartitionId, execution.getAttemptId());
    }

    /**
     * Deserializes accumulators from a task state update.
     *
     * <p>This method never throws an exception!
     *
     * @param state The task execution state from which to deserialize the accumulators.
     * @return The deserialized accumulators, of null, if there are no accumulators or an error
     *     occurred.
     */
    private Map<String, Accumulator<?, ?>> deserializeAccumulators(
            TaskExecutionStateTransition state) {
        AccumulatorSnapshot serializedAccumulators = state.getAccumulators();

        if (serializedAccumulators != null) {
            try {
                return serializedAccumulators.deserializeUserAccumulators(userClassLoader);
            } catch (Throwable t) {
                // we catch Throwable here to include all form of linking errors that may
                // occur if user classes are missing in the classpath
                LOG.error("Failed to deserialize final accumulator results.", t);
            }
        }
        return null;
    }

    @Override
    public Map<ExecutionAttemptID, Execution> getRegisteredExecutions() {
        return Collections.unmodifiableMap(currentExecutions);
    }

    @Override
    public void registerExecution(Execution exec) {
        assertRunningInJobMasterMainThread();
        Execution previous = currentExecutions.putIfAbsent(exec.getAttemptId(), exec);
        if (previous != null) {
            failGlobal(
                    new Exception(
                            "Trying to register execution "
                                    + exec
                                    + " for already used ID "
                                    + exec.getAttemptId()));
        }
    }

    @Override
    public void deregisterExecution(Execution exec) {
        assertRunningInJobMasterMainThread();
        Execution contained = currentExecutions.remove(exec.getAttemptId());

        if (contained != null && contained != exec) {
            failGlobal(
                    new Exception(
                            "De-registering execution "
                                    + exec
                                    + " failed. Found for same ID execution "
                                    + contained));
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 维护Map结构方便后面执行的时候使用
    */
    private void registerExecutionVerticesAndResultPartitionsFor(
            ExecutionJobVertex executionJobVertex) {
        for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
            executionVerticesById.put(executionVertex.getID(), executionVertex);
            resultPartitionsById.putAll(executionVertex.getProducedPartitions());
        }
    }

    @Override
    public void updateAccumulators(AccumulatorSnapshot accumulatorSnapshot) {
        Map<String, Accumulator<?, ?>> userAccumulators;
        try {
            userAccumulators = accumulatorSnapshot.deserializeUserAccumulators(userClassLoader);

            ExecutionAttemptID execID = accumulatorSnapshot.getExecutionAttemptID();
            Execution execution = currentExecutions.get(execID);
            if (execution != null) {
                execution.setAccumulators(userAccumulators);
            } else {
                LOG.debug("Received accumulator result for unknown execution {}.", execID);
            }
        } catch (Exception e) {
            LOG.error("Cannot update accumulators for job {}.", getJobID(), e);
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Listeners & Observers
    // --------------------------------------------------------------------------------------------

    @Override
    public void registerJobStatusListener(JobStatusListener listener) {
        if (listener != null) {
            jobStatusListeners.add(listener);
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *触发JobStatus状态的修改
    */
    private void notifyJobStatusChange(JobStatus newState) {
        /** 判断状态监听器的大小 */
        if (jobStatusListeners.size() > 0) {
            /** 获取当前时间 */
            final long timestamp = System.currentTimeMillis();
            /** 循环状态监听器 JobStatusListener*/
            for (JobStatusListener listener : jobStatusListeners) {
                try {
                    /** 只有JobStatus改变的时候才会触发 jobStatusChanges*/
                    listener.jobStatusChanges(getJobID(), newState, timestamp);
                } catch (Throwable t) {
                    LOG.warn("Error while notifying JobStatusListener", t);
                }
            }
        }
    }

    private void notifyJobStatusHooks(JobStatus newState, @Nullable Throwable cause) {
        JobID jobID = jobInformation.getJobId();
        for (JobStatusHook hook : jobStatusHooks) {
            try {
                switch (newState) {
                    case CREATED:
                        hook.onCreated(jobID);
                        break;
                    case CANCELED:
                        hook.onCanceled(jobID);
                        break;
                    case FAILED:
                        hook.onFailed(jobID, cause);
                        break;
                    case FINISHED:
                        hook.onFinished(jobID);
                        break;
                }
            } catch (Throwable e) {
                throw new RuntimeException(
                        "Error while notifying JobStatusHook[" + hook.getClass() + "]", e);
            }
        }
    }

    @Override
    public void notifyExecutionChange(
            final Execution execution,
            ExecutionState previousState,
            final ExecutionState newExecutionState) {
        executionStateUpdateListener.onStateUpdate(
                execution.getAttemptId(), previousState, newExecutionState);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 检查当前线程是否是作业主节点（Job Master）的主线程
    */
    private void assertRunningInJobMasterMainThread() {
        if (!(jobMasterMainThreadExecutor
                instanceof ComponentMainThreadExecutor.DummyComponentMainThreadExecutor)) {
            jobMasterMainThreadExecutor.assertRunningInMainThread();
        }
    }

    @Override
    public void notifySchedulerNgAboutInternalTaskFailure(
            final ExecutionAttemptID attemptId,
            final Throwable t,
            final boolean cancelTask,
            final boolean releasePartitions) {
        checkState(internalTaskFailuresListener != null);
        internalTaskFailuresListener.notifyTaskFailure(attemptId, t, cancelTask, releasePartitions);
    }

    @Override
    public void deleteBlobs(List<PermanentBlobKey> blobKeys) {
        CompletableFuture.runAsync(
                () -> {
                    for (PermanentBlobKey blobKey : blobKeys) {
                        blobWriter.deletePermanent(getJobID(), blobKey);
                    }
                },
                ioExecutor);
    }

    @Override
    public ShuffleMaster<?> getShuffleMaster() {
        return shuffleMaster;
    }

    @Override
    public JobMasterPartitionTracker getPartitionTracker() {
        return partitionTracker;
    }

    @Override
    public ResultPartitionAvailabilityChecker getResultPartitionAvailabilityChecker() {
        return resultPartitionAvailabilityChecker;
    }

    @Override
    public PartitionGroupReleaseStrategy getPartitionGroupReleaseStrategy() {
        return partitionGroupReleaseStrategy;
    }

    @Override
    public ExecutionDeploymentListener getExecutionDeploymentListener() {
        return executionDeploymentListener;
    }

    @Override
    public boolean isDynamic() {
        return isDynamic;
    }

    @Override
    public Optional<String> findVertexWithAttempt(ExecutionAttemptID attemptId) {
        return Optional.ofNullable(currentExecutions.get(attemptId))
                .map(Execution::getVertexWithAttempt);
    }

    @Override
    public Optional<AccessExecution> findExecution(ExecutionAttemptID attemptId) {
        return Optional.ofNullable(currentExecutions.get(attemptId));
    }

    @Override
    public ExecutionGraphID getExecutionGraphID() {
        return executionGraphId;
    }

    @Override
    public List<ShuffleDescriptor> getClusterPartitionShuffleDescriptors(
            IntermediateDataSetID intermediateDataSetID) {
        return partitionTracker.getClusterPartitionShuffleDescriptors(intermediateDataSetID);
    }

    @Override
    public MarkPartitionFinishedStrategy getMarkPartitionFinishedStrategy() {
        return markPartitionFinishedStrategy;
    }

    @Override
    public JobVertexInputInfo getJobVertexInputInfo(
            JobVertexID jobVertexId, IntermediateDataSetID resultId) {
        return vertexInputInfoStore.get(jobVertexId, resultId);
    }

    @Override
    public TaskDeploymentDescriptorFactory getTaskDeploymentDescriptorFactory() {
        return taskDeploymentDescriptorFactory;
    }
}
