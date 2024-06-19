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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.FinishedTaskStateProvider.PartialFinishingNotSupportedByStateException;
import org.apache.flink.runtime.checkpoint.hooks.MasterHooks;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorInfo;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageCoordinatorView;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;
import static org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT;
import static org.apache.flink.runtime.checkpoint.CheckpointType.FULL_CHECKPOINT;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The checkpoint coordinator coordinates the distributed snapshots of operators and state. It
 * triggers the checkpoint by sending the messages to the relevant tasks and collects the checkpoint
 * acknowledgements. It also collects and maintains the overview of the state handles reported by
 * the tasks that acknowledge the checkpoint.
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 检查点协调器协调操作员和状态的分布式快照。它通过向相关任务发送消息来触发检查点，并收集检查点确认。它还收集并维护由确认检查点的任务报告的状态句柄的概述。
*/
public class CheckpointCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

    /** The number of recent checkpoints whose IDs are remembered. */
    private static final int NUM_GHOST_CHECKPOINT_IDS = 16;

    // ------------------------------------------------------------------------

    /** Coordinator-wide lock to safeguard the checkpoint updates. */
    private final Object lock = new Object();

    /** The job whose checkpoint this coordinator coordinates. */
    private final JobID job;

    /** Default checkpoint properties. */
    private final CheckpointProperties checkpointProperties;

    /** The executor used for asynchronous calls, like potentially blocking I/O. */
    private final Executor executor;

    private final CheckpointsCleaner checkpointsCleaner;

    /** The operator coordinators that need to be checkpointed. */
    private final Collection<OperatorCoordinatorCheckpointContext> coordinatorsToCheckpoint;

    /** Map from checkpoint ID to the pending checkpoint. */
    @GuardedBy("lock")
    private final Map<Long, PendingCheckpoint> pendingCheckpoints;

    /**
     * Completed checkpoints. Implementations can be blocking. Make sure calls to methods accessing
     * this don't block the job manager actor and run asynchronously.
     */
    private final CompletedCheckpointStore completedCheckpointStore;

    /**
     * The root checkpoint state backend, which is responsible for initializing the checkpoint,
     * storing the metadata, and cleaning up the checkpoint.
     */
    private final CheckpointStorageCoordinatorView checkpointStorageView;

    /** A list of recent expired checkpoint IDs, to identify late messages (vs invalid ones). */
    private final ArrayDeque<Long> recentExpiredCheckpoints;

    /**
     * Checkpoint ID counter to ensure ascending IDs. In case of job manager failures, these need to
     * be ascending across job managers.
     */
    private final CheckpointIDCounter checkpointIdCounter;

    /**
     * The checkpoint interval when there is no source reporting isProcessingBacklog=true. Actual
     * trigger time may be affected by the max concurrent checkpoints, minimum-pause values and
     * checkpoint interval during backlog.
     */
    private final long baseInterval;

    /**
     * The checkpoint interval when any source reports isProcessingBacklog=true. Actual trigger time
     * may be affected by the max concurrent checkpoints and minimum-pause values.
     */
    private final long baseIntervalDuringBacklog;

    /** The max time (in ms) that a checkpoint may take. */
    private final long checkpointTimeout;

    /**
     * The min time(in ms) to delay after a checkpoint could be triggered. Allows to enforce minimum
     * processing time between checkpoint attempts
     */
    private final long minPauseBetweenCheckpoints;

    /**
     * The timer that handles the checkpoint timeouts and triggers periodic checkpoints. It must be
     * single-threaded. Eventually it will be replaced by main thread executor.
     */
    private final ScheduledExecutor timer;

    /** The master checkpoint hooks executed by this checkpoint coordinator. */
    private final HashMap<String, MasterTriggerRestoreHook<?>> masterHooks;

    private final boolean unalignedCheckpointsEnabled;

    private final long alignedCheckpointTimeout;

    /** Actor that receives status updates from the execution graph this coordinator works for. */
    private JobStatusListener jobStatusListener;

    /**
     * The current periodic trigger. Used to deduplicate concurrently scheduled checkpoints if any.
     */
    @GuardedBy("lock")
    private ScheduledTrigger currentPeriodicTrigger;

    /** A handle to the current periodic trigger, to cancel it when necessary. */
    @GuardedBy("lock")
    private ScheduledFuture<?> currentPeriodicTriggerFuture;

    /**
     * The timestamp (via {@link Clock#relativeTimeMillis()}) when the next checkpoint will be
     * triggered.
     *
     * <p>If it's value is {@link Long#MAX_VALUE}, it means there is not a next checkpoint
     * scheduled.
     */
    @GuardedBy("lock")
    private long nextCheckpointTriggeringRelativeTime;

    /**
     * The timestamp (via {@link Clock#relativeTimeMillis()}) when the last checkpoint completed.
     */
    private long lastCheckpointCompletionRelativeTime;

    /**
     * Flag whether a triggered checkpoint should immediately schedule the next checkpoint.
     * Non-volatile, because only accessed in synchronized scope
     */
    private boolean periodicScheduling;

    /** Flag marking the coordinator as shut down (not accepting any messages any more). */
    private volatile boolean shutdown;

    /** Optional tracker for checkpoint statistics. */
    private final CheckpointStatsTracker statsTracker;

    private final BiFunction<
                    Set<ExecutionJobVertex>,
                    Map<OperatorID, OperatorState>,
                    VertexFinishedStateChecker>
            vertexFinishedStateCheckerFactory;

    /** Id of checkpoint for which in-flight data should be ignored on recovery. */
    private final long checkpointIdOfIgnoredInFlightData;

    private final CheckpointFailureManager failureManager;

    private final Clock clock;

    private final boolean isExactlyOnceMode;

    /** Flag represents there is an in-flight trigger request. */
    private boolean isTriggering = false;

    private final CheckpointRequestDecider requestDecider;

    private final CheckpointPlanCalculator checkpointPlanCalculator;

    /** IDs of the source operators that are currently processing backlog. */
    @GuardedBy("lock")
    private final Set<OperatorID> backlogOperators = new HashSet<>();

    private boolean baseLocationsForCheckpointInitialized = false;

    private boolean forceFullSnapshot;

    // --------------------------------------------------------------------------------------------

    public CheckpointCoordinator(
            JobID job,
            CheckpointCoordinatorConfiguration chkConfig,
            Collection<OperatorCoordinatorCheckpointContext> coordinatorsToCheckpoint,
            CheckpointIDCounter checkpointIDCounter,
            CompletedCheckpointStore completedCheckpointStore,
            CheckpointStorage checkpointStorage,
            Executor executor,
            CheckpointsCleaner checkpointsCleaner,
            ScheduledExecutor timer,
            CheckpointFailureManager failureManager,
            CheckpointPlanCalculator checkpointPlanCalculator,
            CheckpointStatsTracker statsTracker) {

        this(
                job,
                chkConfig,
                coordinatorsToCheckpoint,
                checkpointIDCounter,
                completedCheckpointStore,
                checkpointStorage,
                executor,
                checkpointsCleaner,
                timer,
                failureManager,
                checkpointPlanCalculator,
                SystemClock.getInstance(),
                statsTracker,
                VertexFinishedStateChecker::new);
    }

    @VisibleForTesting
    public CheckpointCoordinator(
            JobID job,
            CheckpointCoordinatorConfiguration chkConfig,
            Collection<OperatorCoordinatorCheckpointContext> coordinatorsToCheckpoint,
            CheckpointIDCounter checkpointIDCounter,
            CompletedCheckpointStore completedCheckpointStore,
            CheckpointStorage checkpointStorage,
            Executor executor,
            CheckpointsCleaner checkpointsCleaner,
            ScheduledExecutor timer,
            CheckpointFailureManager failureManager,
            CheckpointPlanCalculator checkpointPlanCalculator,
            Clock clock,
            CheckpointStatsTracker statsTracker,
            BiFunction<
                            Set<ExecutionJobVertex>,
                            Map<OperatorID, OperatorState>,
                            VertexFinishedStateChecker>
                    vertexFinishedStateCheckerFactory) {

        // sanity checks
        checkNotNull(checkpointStorage);

        // max "in between duration" can be one year - this is to prevent numeric overflows
        long minPauseBetweenCheckpoints = chkConfig.getMinPauseBetweenCheckpoints();
        if (minPauseBetweenCheckpoints > 365L * 24 * 60 * 60 * 1_000) {
            minPauseBetweenCheckpoints = 365L * 24 * 60 * 60 * 1_000;
        }

        // it does not make sense to schedule checkpoints more often then the desired
        // time between checkpoints
        long baseInterval = chkConfig.getCheckpointInterval();
        if (baseInterval < minPauseBetweenCheckpoints) {
            baseInterval = minPauseBetweenCheckpoints;
        }

        this.job = checkNotNull(job);
        this.baseInterval = baseInterval;
        this.baseIntervalDuringBacklog = chkConfig.getCheckpointIntervalDuringBacklog();
        this.nextCheckpointTriggeringRelativeTime = Long.MAX_VALUE;
        this.checkpointTimeout = chkConfig.getCheckpointTimeout();
        this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
        this.coordinatorsToCheckpoint =
                Collections.unmodifiableCollection(coordinatorsToCheckpoint);
        this.pendingCheckpoints = new LinkedHashMap<>();
        this.checkpointIdCounter = checkNotNull(checkpointIDCounter);
        this.completedCheckpointStore = checkNotNull(completedCheckpointStore);
        this.executor = checkNotNull(executor);
        this.checkpointsCleaner = checkNotNull(checkpointsCleaner);
        this.failureManager = checkNotNull(failureManager);
        this.checkpointPlanCalculator = checkNotNull(checkpointPlanCalculator);
        this.clock = checkNotNull(clock);
        this.isExactlyOnceMode = chkConfig.isExactlyOnce();
        this.unalignedCheckpointsEnabled = chkConfig.isUnalignedCheckpointsEnabled();
        this.alignedCheckpointTimeout = chkConfig.getAlignedCheckpointTimeout();
        this.checkpointIdOfIgnoredInFlightData = chkConfig.getCheckpointIdOfIgnoredInFlightData();

        this.recentExpiredCheckpoints = new ArrayDeque<>(NUM_GHOST_CHECKPOINT_IDS);
        this.masterHooks = new HashMap<>();

        this.timer = timer;

        this.checkpointProperties =
                CheckpointProperties.forCheckpoint(chkConfig.getCheckpointRetentionPolicy());

        try {
            this.checkpointStorageView = checkpointStorage.createCheckpointStorage(job);

            if (isPeriodicCheckpointingConfigured()) {
                checkpointStorageView.initializeBaseLocationsForCheckpoint();
                baseLocationsForCheckpointInitialized = true;
            }
        } catch (IOException e) {
            throw new FlinkRuntimeException(
                    "Failed to create checkpoint storage at checkpoint coordinator side.", e);
        }

        try {
            // Make sure the checkpoint ID enumerator is running. Possibly
            // issues a blocking call to ZooKeeper.
            checkpointIDCounter.start();
        } catch (Throwable t) {
            throw new RuntimeException(
                    "Failed to start checkpoint ID counter: " + t.getMessage(), t);
        }
        this.requestDecider =
                new CheckpointRequestDecider(
                        chkConfig.getMaxConcurrentCheckpoints(),
                        this::rescheduleTrigger,
                        this.clock,
                        this.minPauseBetweenCheckpoints,
                        this.pendingCheckpoints::size,
                        this.checkpointsCleaner::getNumberOfCheckpointsToClean);
        this.statsTracker = checkNotNull(statsTracker, "Statistic tracker can not be null");
        this.vertexFinishedStateCheckerFactory = checkNotNull(vertexFinishedStateCheckerFactory);
    }

    // --------------------------------------------------------------------------------------------
    //  Configuration
    // --------------------------------------------------------------------------------------------

    /**
     * Adds the given master hook to the checkpoint coordinator. This method does nothing, if the
     * checkpoint coordinator already contained a hook with the same ID (as defined via {@link
     * MasterTriggerRestoreHook#getIdentifier()}).
     *
     * @param hook The hook to add.
     * @return True, if the hook was added, false if the checkpoint coordinator already contained a
     *     hook with the same ID.
     */
    public boolean addMasterHook(MasterTriggerRestoreHook<?> hook) {
        checkNotNull(hook);

        final String id = hook.getIdentifier();
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(id), "The hook has a null or empty id");

        synchronized (lock) {
            if (!masterHooks.containsKey(id)) {
                masterHooks.put(id, hook);
                return true;
            } else {
                return false;
            }
        }
    }

    /** Gets the number of currently register master hooks. */
    public int getNumberOfRegisteredMasterHooks() {
        synchronized (lock) {
            return masterHooks.size();
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Clean shutdown
    // --------------------------------------------------------------------------------------------

    /**
     * Shuts down the checkpoint coordinator.
     *
     * <p>After this method has been called, the coordinator does not accept and further messages
     * and cannot trigger any further checkpoints.
     */
    public void shutdown() throws Exception {
        synchronized (lock) {
            if (!shutdown) {
                shutdown = true;
                LOG.info("Stopping checkpoint coordinator for job {}.", job);

                periodicScheduling = false;

                // shut down the hooks
                MasterHooks.close(masterHooks.values(), LOG);
                masterHooks.clear();

                final CheckpointException reason =
                        new CheckpointException(
                                CheckpointFailureReason.CHECKPOINT_COORDINATOR_SHUTDOWN);
                // clear queued requests and in-flight checkpoints
                abortPendingAndQueuedCheckpoints(reason);
            }
        }
    }

    public boolean isShutdown() {
        return shutdown;
    }

    /**
     * Reports whether a source operator is currently processing backlog.
     *
     * <p>If any source operator is processing backlog, the checkpoint interval would be decided by
     * {@code execution.checkpointing.interval-during-backlog} instead of {@code
     * execution.checkpointing.interval}.
     *
     * <p>If a source has not invoked this method, the source is considered to have
     * isProcessingBacklog=false. If a source operator has invoked this method multiple times, the
     * last reported value is used.
     *
     * @param operatorID the operator ID of the source operator.
     * @param isProcessingBacklog whether the source operator is processing backlog.
     */
    public void setIsProcessingBacklog(OperatorID operatorID, boolean isProcessingBacklog) {
        synchronized (lock) {
            if (isProcessingBacklog) {
                backlogOperators.add(operatorID);
            } else {
                backlogOperators.remove(operatorID);
            }

            long currentCheckpointInterval = getCurrentCheckpointInterval();
            if (currentCheckpointInterval
                    != CheckpointCoordinatorConfiguration.DISABLED_CHECKPOINT_INTERVAL) {
                long currentRelativeTime = clock.relativeTimeMillis();
                if (currentRelativeTime + currentCheckpointInterval
                        < nextCheckpointTriggeringRelativeTime) {
                    rescheduleTrigger(currentRelativeTime, currentCheckpointInterval);
                }
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Triggering Checkpoints and Savepoints
    // --------------------------------------------------------------------------------------------

    /**
     * Triggers a savepoint with the given savepoint directory as a target.
     *
     * @param targetLocation Target location for the savepoint, optional. If null, the state
     *     backend's configured default will be used.
     * @return A future to the completed checkpoint
     * @throws IllegalStateException If no savepoint directory has been specified and no default
     *     savepoint directory has been configured
     */
    public CompletableFuture<CompletedCheckpoint> triggerSavepoint(
            @Nullable final String targetLocation, final SavepointFormatType formatType) {
        final CheckpointProperties properties =
                CheckpointProperties.forSavepoint(!unalignedCheckpointsEnabled, formatType);
        return triggerSavepointInternal(properties, targetLocation);
    }

    /**
     * Triggers a synchronous savepoint with the given savepoint directory as a target.
     *
     * @param terminate flag indicating if the job should terminate or just suspend
     * @param targetLocation Target location for the savepoint, optional. If null, the state
     *     backend's configured default will be used.
     * @return A future to the completed checkpoint
     * @throws IllegalStateException If no savepoint directory has been specified and no default
     *     savepoint directory has been configured
     */
    public CompletableFuture<CompletedCheckpoint> triggerSynchronousSavepoint(
            final boolean terminate,
            @Nullable final String targetLocation,
            SavepointFormatType formatType) {

        final CheckpointProperties properties =
                CheckpointProperties.forSyncSavepoint(
                        !unalignedCheckpointsEnabled, terminate, formatType);

        return triggerSavepointInternal(properties, targetLocation);
    }

    private CompletableFuture<CompletedCheckpoint> triggerSavepointInternal(
            final CheckpointProperties checkpointProperties,
            @Nullable final String targetLocation) {

        checkNotNull(checkpointProperties);

        return triggerCheckpointFromCheckpointThread(checkpointProperties, targetLocation, false);
    }

    private CompletableFuture<CompletedCheckpoint> triggerCheckpointFromCheckpointThread(
            CheckpointProperties checkpointProperties, String targetLocation, boolean isPeriodic) {
        // TODO, call triggerCheckpoint directly after removing timer thread
        // for now, execute the trigger in timer thread to avoid competition
        final CompletableFuture<CompletedCheckpoint> resultFuture = new CompletableFuture<>();
        timer.execute(
                () ->
                        triggerCheckpoint(checkpointProperties, targetLocation, isPeriodic)
                                .whenComplete(
                                        (completedCheckpoint, throwable) -> {
                                            if (throwable == null) {
                                                resultFuture.complete(completedCheckpoint);
                                            } else {
                                                resultFuture.completeExceptionally(throwable);
                                            }
                                        }));
        return resultFuture;
    }

    /**
     * Triggers a new standard checkpoint and uses the given timestamp as the checkpoint timestamp.
     * The return value is a future. It completes when the checkpoint triggered finishes or an error
     * occurred.
     *
     * @param isPeriodic Flag indicating whether this triggered checkpoint is periodic.
     * @return a future to the completed checkpoint.
     */
    public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(boolean isPeriodic) {
        return triggerCheckpointFromCheckpointThread(checkpointProperties, null, isPeriodic);
    }

    /**
     * Triggers one new checkpoint with the given checkpointType. The returned future completes when
     * the triggered checkpoint finishes or an error occurred.
     *
     * @param checkpointType specifies the backup type of the checkpoint to trigger.
     * @return a future to the completed checkpoint.
     */
    public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(CheckpointType checkpointType) {

        if (checkpointType == null) {
            throw new IllegalArgumentException("checkpointType cannot be null");
        }

        final SnapshotType snapshotType;
        switch (checkpointType) {
            case CONFIGURED:
                snapshotType = checkpointProperties.getCheckpointType();
                break;
            case FULL:
                snapshotType = FULL_CHECKPOINT;
                break;
            case INCREMENTAL:
                snapshotType = CHECKPOINT;
                break;
            default:
                throw new IllegalArgumentException("unknown checkpointType: " + checkpointType);
        }

        final CheckpointProperties properties =
                new CheckpointProperties(
                        checkpointProperties.forceCheckpoint(),
                        snapshotType,
                        checkpointProperties.discardOnSubsumed(),
                        checkpointProperties.discardOnJobFinished(),
                        checkpointProperties.discardOnJobCancelled(),
                        checkpointProperties.discardOnJobFailed(),
                        checkpointProperties.discardOnJobSuspended(),
                        checkpointProperties.isUnclaimed());
        return triggerCheckpointFromCheckpointThread(properties, null, false);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 触发检查点并返回一个表示检查点完成状态的CompletableFuture对象
     * @param props 检查点的属性配置
     * @param externalSavepointLocation 可选的外部保存点位置，如果为空则不使用外部保存点
     * @param isPeriodic 是否是周期性触发的检查点
    */
    @VisibleForTesting
    CompletableFuture<CompletedCheckpoint> triggerCheckpoint(
            CheckpointProperties props,
            @Nullable String externalSavepointLocation,
            boolean isPeriodic) {
        // 创建一个包含检查点触发请求所需信息的CheckpointTriggerRequest对象
        CheckpointTriggerRequest request =
                new CheckpointTriggerRequest(props, externalSavepointLocation, isPeriodic);
        // 选择要执行的请求，如果找到有效的请求，则执行startTriggeringCheckpoint方法
        chooseRequestToExecute(request).ifPresent(this::startTriggeringCheckpoint);
        // 返回与CheckpointTriggerRequest关联的CompletableFuture对象
        // 这个对象将在检查点完成时完成，并携带CompletedCheckpoint对象作为结果
        // 或者，如果在检查点触发或执行过程中发生异常，则会携带异常完成
        return request.onCompletionPromise;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 真正触发CheckPoint的方法
    */
    private void startTriggeringCheckpoint(CheckpointTriggerRequest request) {
        try {
            // 使用同步块来确保全局状态检查是线程安全的
            synchronized (lock) {
                // 在触发检查点之前，对全局状态进行预检查，检查是否满足触发周期性检查点的条件
                preCheckGlobalState(request.isPeriodic);
            }

            // we will actually trigger this checkpoint!
            // 检查当前是否正在触发检查点，如果不是，则设置状态为正在触发
            // 使用Preconditions来确保状态检查，如果不满足条件则抛出异常
            Preconditions.checkState(!isTriggering);
            isTriggering = true;

            // 记录触发检查点的时间戳
            final long timestamp = System.currentTimeMillis();

            // 异步计算检查点计划
            // 返回一个CompletableFuture，该Future在计划计算完成后将包含CheckpointPlan对象
            CompletableFuture<CheckpointPlan> checkpointPlanFuture =
                    checkpointPlanCalculator.calculateCheckpointPlan();
             // 判断是否需要初始化检查点的基础位置信息
            // 如果尚未初始化，则进行初始化，并更新状态
            boolean initializeBaseLocations = !baseLocationsForCheckpointInitialized;
            baseLocationsForCheckpointInitialized = true;

            // 创建一个新的CompletableFuture，用于表示主触发操作的完成
            CompletableFuture<Void> masterTriggerCompletionPromise = new CompletableFuture<>();
            // 异步处理checkpointPlanFuture的结果
            final CompletableFuture<PendingCheckpoint> pendingCheckpointCompletableFuture =
                    checkpointPlanFuture
                            .thenApplyAsync(
                                    plan -> {
                                        try {
                                            // this must happen outside the coordinator-wide lock,
                                            // because it communicates with external services
                                            // (in HA mode) and may block for a while.
                                            // 获取一个新的、递增的检查点ID
                                            long checkpointID =
                                                    checkpointIdCounter.getAndIncrement();
                                            // 创建一个PendingCheckpoint对象，该对象将用于表示尚未完成的检查点
                                            return new Tuple2<>(plan, checkpointID);
                                        } catch (Throwable e) {
                                            // 如果在获取检查点ID或创建PendingCheckpoint时发生异常，
                                            throw new CompletionException(e);
                                        }
                                    },
                                    executor)
                            .thenApplyAsync(
                                    // checkpointInfo是一个Tuple2对象，包含CheckpointPlan和checkpointID
                                    (checkpointInfo) ->
                                            // 这里调用createPendingCheckpoint方法来完成这个操作
                                            createPendingCheckpoint(
                                                    timestamp,// 触发检查点的时间戳
                                                    request.props,// 请求的属性
                                                    checkpointInfo.f0,// CheckpointPlan
                                                    request.isPeriodic,// 是否是周期性检查点
                                                    checkpointInfo.f1,// 检查点ID
                                                    request.getOnCompletionFuture(),// 检查点完成时的回调Future
                                                    masterTriggerCompletionPromise),// 主触发操作的完成Promise
                                    timer);

            final CompletableFuture<?> coordinatorCheckpointsComplete =
                    pendingCheckpointCompletableFuture
                            // 当pendingCheckpointCompletableFuture完成时，异步地应用给定的函数来初始化检查点位置
                            .thenApplyAsync(
                                    pendingCheckpoint -> {
                                        try {
                                            // 初始化检查点的存储位置
                                            // 使用pendingCheckpoint的checkpointID、请求的属性、外部保存点位置等参数
                                            CheckpointStorageLocation checkpointStorageLocation =
                                                    initializeCheckpointLocation(
                                                            pendingCheckpoint.getCheckpointID(),
                                                            request.props,
                                                            request.externalSavepointLocation,
                                                            initializeBaseLocations);
                                            // 将pendingCheckpoint和checkpointStorageLocation封装成一个Tuple2对象返回
                                            return Tuple2.of(
                                                    pendingCheckpoint, checkpointStorageLocation);
                                        } catch (Throwable e) {
                                            //抛出异常
                                            throw new CompletionException(e);
                                        }
                                    },
                                    executor)
                            .thenComposeAsync(
                                    // 从Tuple2中提取PendingCheckpoint
                                    (checkpointInfo) -> {
                                        PendingCheckpoint pendingCheckpoint = checkpointInfo.f0;
                                        // 如果PendingCheckpoint已经被释放（disposed）
                                        // 则稍后会处理这个已释放的检查点，跳过协调器状态的快照操作
                                        if (pendingCheckpoint.isDisposed()) {
                                            // The disposed checkpoint will be handled later,
                                            // skip snapshotting the coordinator states.
                                            // 这里返回null表示没有后续操作需要等待
                                            return null;
                                        }
                                        synchronized (lock) {
                                            // 设置pendingCheckpoint的目标存储位置为checkpointInfo中的第二个元素（即checkpointStorageLocation）
                                            pendingCheckpoint.setCheckpointTargetLocation(
                                                    checkpointInfo.f1);
                                        }
                                        // 不是真正的触发 触发并确认所有协调器的检查点（checkpoint）操作，并返回一个CompletableFuture来表示该操作的完成
                                        // 参数包括：
                                        // - coordinatorsToCheckpoint：需要触发检查点的协调器列表
                                        // - pendingCheckpoint：当前待处理的检查点
                                        // - timer：可能用于检查点操作的计时器或时间服务
                                        return OperatorCoordinatorCheckpoints
                                                .triggerAndAcknowledgeAllCoordinatorCheckpointsWithCompletion(
                                                        coordinatorsToCheckpoint,
                                                        pendingCheckpoint,
                                                        timer);
                                    },
                                    timer);

            // We have to take the snapshot of the master hooks after the coordinator checkpoints
            // has completed.
            // This is to ensure the tasks are checkpointed after the OperatorCoordinators in case
            // ExternallyInducedSource is used.
            // 创建一个新的CompletableFuture，它将在coordinatorCheckpointsComplete完成后异步地执行后续操作
            final CompletableFuture<?> masterStatesComplete =
                    coordinatorCheckpointsComplete.thenComposeAsync(
                            ignored -> {
                                // If the code reaches here, the pending checkpoint is guaranteed to
                                // be not null.
                                // We use FutureUtils.getWithoutException() to make compiler happy
                                // with checked
                                // exceptions in the signature.
                                // 如果代码执行到这里，说明之前的检查点（pending checkpoint）一定不是null
                                // 使用FutureUtils.getWithoutException()来避免编译器对检查异常的不满
                                PendingCheckpoint checkpoint =
                                        FutureUtils.getWithoutException(
                                                pendingCheckpointCompletableFuture);
                                if (checkpoint == null || checkpoint.isDisposed()) {
                                    // The disposed checkpoint will be handled later,
                                    // skip snapshotting the master states.
                                    // 如果检查点已经被处理，则跳过主状态的快照操作
                                    // 后续会处理已处理的检查点，此处直接返回null
                                    return null;
                                }
                                // 对主状态进行快照操作，并返回此操作的CompletableFuture
                                return snapshotMasterState(checkpoint);
                            },
                            timer);

            // 使用FutureUtils.forward来将多个CompletableFuture的完成情况转发给masterTriggerCompletionPromise
            // 这样，当masterStatesComplete和coordinatorCheckpointsComplete都完成时，masterTriggerCompletionPromise也会被完成
            // 这允许外部监听masterTriggerCompletionPromise以得知整个操作何时完成
            FutureUtils.forward(
                    CompletableFuture.allOf(masterStatesComplete, coordinatorCheckpointsComplete),
                    masterTriggerCompletionPromise);
            // 使用FutureUtils.assertNoException来确保masterTriggerCompletionPromise的完成没有异常
            // 如果有异常，它将直接抛出
            FutureUtils.assertNoException(

                    // 当masterTriggerCompletionPromise完成时，异步处理其结果和可能的异常
                    masterTriggerCompletionPromise
                            .handleAsync(
                                    (ignored, throwable) -> {
                                        // 获取pendingCheckpointCompletableFuture的结果，即待处理的检查点
                                        final PendingCheckpoint checkpoint =
                                                FutureUtils.getWithoutException(
                                                        pendingCheckpointCompletableFuture);
                                        // 检查状态，确保待处理的检查点存在或者已经有一个异常发生
                                        Preconditions.checkState(
                                                checkpoint != null || throwable != null,
                                                "Either the pending checkpoint needs to be created or an error must have occurred.");

                                        if (throwable != null) {
                                            // the initialization might not be finished yet
                                            // 如果有异常发生
                                            // 如果检查点还未被创建（可能初始化未完成）
                                            if (checkpoint == null) {
                                                // 使用请求和异常调用onTriggerFailure方法
                                                onTriggerFailure(request, throwable);
                                            } else {
                                                // 使用检查点和异常调用onTriggerFailure方法
                                                onTriggerFailure(checkpoint, throwable);
                                            }
                                        } else {
                                            // 如果没有异常，则触发检查点请求
                                            triggerCheckpointRequest(
                                                    request, timestamp, checkpoint);
                                        }
                                        return null;
                                    },
                                    timer)
                            .exceptionally(
                                    //异常处理 抛出异常
                                    error -> {
                                        if (!isShutdown()) {
                                            //抛出异常
                                            throw new CompletionException(error);
                                        } else if (findThrowable(
                                                        error, RejectedExecutionException.class)
                                                .isPresent()) {
                                            LOG.debug("Execution rejected during shutdown");
                                        } else {
                                            LOG.warn("Error encountered during shutdown", error);
                                        }
                                        return null;
                                    }));
        } catch (Throwable throwable) {
            //异常处理
            onTriggerFailure(request, throwable);
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 触发检查点请求的方法
     *
     * @param request        检查点触发请求对象
     * @param timestamp      当前时间戳
     * @param checkpoint     待处理的检查点对象
    */
    private void triggerCheckpointRequest(
            CheckpointTriggerRequest request, long timestamp, PendingCheckpoint checkpoint) {
        // 检查检查点对象是否已经被处理或销毁
        if (checkpoint.isDisposed()) {
            // 如果检查点已经被处理或销毁，则触发失败处理
            onTriggerFailure(
                    checkpoint,
                    new CheckpointException(
                            CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE,
                            checkpoint.getFailureCause()));
        } else {
            // 触发检查点相关任务，返回一个CompletableFuture对象
            // 该对象表示异步计算的结果，或者表示计算过程中的异常
            triggerTasks(request, timestamp, checkpoint)
                    .exceptionally(
                            failure -> {
                                // 当触发检查点相关任务发生异常时，执行此处的lambda表达式
                                LOG.info(
                                        "Triggering Checkpoint {} for job {} failed due to {}",
                                        checkpoint.getCheckpointID(),
                                        job,
                                        failure);
                                // 根据异常类型，创建或获取CheckpointException对象
                                final CheckpointException cause;
                                if (failure instanceof CheckpointException) {
                                    cause = (CheckpointException) failure;
                                } else {
                                    cause =
                                            new CheckpointException(
                                                    CheckpointFailureReason
                                                            .TRIGGER_CHECKPOINT_FAILURE,
                                                    failure);
                                }
                                // 使用定时器异步执行检查点中止操作，防止在持有锁的情况下进行长时间操作
                                timer.execute(
                                        () -> {
                                            synchronized (lock) {
                                                abortPendingCheckpoint(checkpoint, cause);
                                            }
                                        });
                                // 返回null，因为exceptionally方法处理的是异常情况，不需要返回正常结果
                                return null;
                            });

            // It is possible that the tasks has finished
            // checkpointing at this point.
            // So we need to complete this pending checkpoint.
            // 在此处检查检查点是否可能已经完成
            // 这可能是因为在触发任务的同时，检查点任务已经完成
            // 所以我们需要完成这个待处理的检查点
            if (maybeCompleteCheckpoint(checkpoint)) {
                // 如果检查点完成，则触发成功处理
                onTriggerSuccess();
            }
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 触发任务的检查点或保存点。
     *
     * @param request        检查点触发请求对象
     * @param timestamp      当前时间戳
     * @param checkpoint     待处理的检查点对象
     * @return 一个CompletableFuture，表示所有任务检查点的触发操作是否完成
    */
    private CompletableFuture<Void> triggerTasks(
            CheckpointTriggerRequest request, long timestamp, PendingCheckpoint checkpoint) {
        // no exception, no discarding, everything is OK
        // 获取检查点ID
        // 如果没有异常，没有丢弃，一切正常
        final long checkpointId = checkpoint.getCheckpointID();
        // 根据配置和请求属性确定检查点类型
        final SnapshotType type;
        if (this.forceFullSnapshot && !request.props.isSavepoint()) {
            // 如果强制要求完整快照且不是保存点请求，则使用FULL_CHECKPOINT
            type = FULL_CHECKPOINT;
        } else {
            // 否则，使用请求中指定的检查点类型
            type = request.props.getCheckpointType();
        }
        // 创建检查点选项对象，该对象包含检查点配置信息
        final CheckpointOptions checkpointOptions =
                CheckpointOptions.forConfig(
                        type,
                        checkpoint.getCheckpointStorageLocation().getLocationReference(),
                        isExactlyOnceMode,// 是否是恰好一次模式
                        unalignedCheckpointsEnabled,// 是否启用非对齐检查点
                        alignedCheckpointTimeout);// 对齐检查点超时时间

        // send messages to the tasks to trigger their checkpoints
        // 创建一个列表来存储每个任务触发检查点后的确认响应的CompletableFuture
        // 向任务发送消息以触发它们的检查点
        List<CompletableFuture<Acknowledge>> acks = new ArrayList<>();
        for (Execution execution : checkpoint.getCheckpointPlan().getTasksToTrigger()) {
            if (request.props.isSynchronous()) {
                // 如果请求是同步的，则触发同步保存点
                acks.add(
                        execution.triggerSynchronousSavepoint(
                                checkpointId, timestamp, checkpointOptions));
            } else {
                // 如果请求是异步的，则触发异步检查点
                acks.add(execution.triggerCheckpoint(checkpointId, timestamp, checkpointOptions));
            }
        }
        // 等待所有任务的检查点触发响应完成
        // 返回一个CompletableFuture，表示所有任务检查点的触发操作是否完成
        return FutureUtils.waitForAll(acks);
    }

    /**
     * Initialize the checkpoint location asynchronously. It will be expected to be executed in io
     * thread due to it might be time-consuming.
     *
     * @param checkpointID checkpoint id
     * @param props checkpoint properties
     * @param externalSavepointLocation the external savepoint location, it might be null
     * @return the checkpoint location
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 异步初始化检查点位置。由于该操作可能会消耗大量时间，因此预期在I/O线程中执行。
     *
     * @param checkpointID 检查点ID
     * @param props 检查点属性
     * @param externalSavepointLocation 外部保存点位置，可能为null
     * @param initializeBaseLocations 是否初始化基础位置
     * @return 初始化后的检查点存储位置
     * @throws Exception 初始化过程中可能抛出的异常
    */
    private CheckpointStorageLocation initializeCheckpointLocation(
            long checkpointID,
            CheckpointProperties props,
            @Nullable String externalSavepointLocation,
            boolean initializeBaseLocations)
            throws Exception {
        // 声明用于存储初始化后的检查点存储位置的变量
        final CheckpointStorageLocation checkpointStorageLocation;
        // 如果当前属性表示这是一个保存点
        if (props.isSavepoint()) {
            // 调用checkpointStorageView的initializeLocationForSavepoint方法来初始化保存点的位置
            // 并传入检查点ID和外部保存点位置作为参数
            checkpointStorageLocation =
                    checkpointStorageView.initializeLocationForSavepoint(
                            checkpointID, externalSavepointLocation);
        } else {
            // 如果不是保存点，并且需要初始化基础位置
            if (initializeBaseLocations) {
                // 调用checkpointStorageView的initializeBaseLocationsForCheckpoint方法来初始化基础位置
                checkpointStorageView.initializeBaseLocationsForCheckpoint();
            }
            // 调用checkpointStorageView的initializeLocationForCheckpoint方法来初始化检查点的位置
            // 并传入检查点ID作为参数
            checkpointStorageLocation =
                    checkpointStorageView.initializeLocationForCheckpoint(checkpointID);
        }
        // 返回初始化后的检查点存储位置
        return checkpointStorageLocation;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建PendingCheckpoint
     * @param timestamp 检查点的时间戳
     * @param props 检查点的属性
     * @param checkpointPlan 检查点的计划
     * @param isPeriodic 是否是周期性检查点
     * @param checkpointID 检查点的ID
    */
    private PendingCheckpoint createPendingCheckpoint(
            long timestamp,
            CheckpointProperties props,
            CheckpointPlan checkpointPlan,
            boolean isPeriodic,
            long checkpointID,
            CompletableFuture<CompletedCheckpoint> onCompletionPromise,
            CompletableFuture<Void> masterTriggerCompletionPromise) {
        // 使用锁来保证线程安全
        synchronized (lock) {
            try {
                // since we haven't created the PendingCheckpoint yet, we need to check the
                // global state here.
                // 在创建PendingCheckpoint之前，我们需要检查全局状态
                // 因为我们还没有创建PendingCheckpoint，所以在这里检查全局状态
                preCheckGlobalState(isPeriodic);
            } catch (Throwable t) {
                throw new CompletionException(t);
            }
        }
        // 跟踪待处理的检查点统计信息
        PendingCheckpointStats pendingCheckpointStats =
                trackPendingCheckpointStats(checkpointID, checkpointPlan, props, timestamp);

        // 创建一个新的PendingCheckpoint实例
        final PendingCheckpoint checkpoint =
                new PendingCheckpoint(
                        job,// 相关的作业
                        checkpointID,// 检查点的ID
                        timestamp,// 检查点的时间戳
                        checkpointPlan,// 检查点的计划
                        OperatorInfo.getIds(coordinatorsToCheckpoint), // 需要进行检查点的协调器ID集合
                        masterHooks.keySet(),// 主协调器钩子的键集合
                        props,// 检查点的属性
                        onCompletionPromise,// 检查点完成时的Promise
                        pendingCheckpointStats,// 待处理的检查点统计信息
                        masterTriggerCompletionPromise);// 触发检查点完成时的Promise

        synchronized (lock) {
            // 将新的PendingCheckpoint添加到pendingCheckpoints映射中
            pendingCheckpoints.put(checkpointID, checkpoint);

            // 使用定时器计划在检查点超时后取消检查点
            ScheduledFuture<?> cancellerHandle =
                    timer.schedule(
                            new CheckpointCanceller(checkpoint),// 检查点取消器
                            checkpointTimeout,// 检查点超时时间
                            TimeUnit.MILLISECONDS); // 时间单位：毫秒
            // 尝试设置检查点的取消器句柄
            if (!checkpoint.setCancellerHandle(cancellerHandle)) {
                // checkpoint is already disposed!
                // 返回创建的PendingCheckpoint实例
                cancellerHandle.cancel(false);
            }
        }

        LOG.info(
                "Triggering checkpoint {} (type={}) @ {} for job {}.",
                checkpointID,
                checkpoint.getProps().getCheckpointType(),
                timestamp,
                job);
        return checkpoint;
    }

    /**
     * Snapshot master hook states asynchronously.
     *
     * @param checkpoint the pending checkpoint
     * @return the future represents master hook states are finished or not
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 异步捕获主节点钩子（hooks）的状态快照。
     *
     * @param checkpoint 等待中的检查点
     * @return 一个表示主节点钩子状态是否完成的 CompletableFuture
    */
    private CompletableFuture<Void> snapshotMasterState(PendingCheckpoint checkpoint) {
        // 如果主节点钩子列表为空，则直接返回一个已经完成的 CompletableFuture
        if (masterHooks.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        // 获取检查点的ID和时间戳
        final long checkpointID = checkpoint.getCheckpointID();
        // 创建一个新的 CompletableFuture 来表示主节点钩子状态捕获的完成情况
        final long timestamp = checkpoint.getCheckpointTimestamp();
        // 遍历主节点钩子列表
        final CompletableFuture<Void> masterStateCompletableFuture = new CompletableFuture<>();
        for (MasterTriggerRestoreHook<?> masterHook : masterHooks.values()) {
            MasterHooks.triggerHook(masterHook, checkpointID, timestamp, executor)
                    .whenCompleteAsync(
                            (masterState, throwable) -> {
                                try {
                                    // 使用锁来确保线程安全
                                    synchronized (lock) {
                                        // 如果 masterStateCompletableFuture 已经完成（无论是正常还是异常），则直接返回
                                        if (masterStateCompletableFuture.isDone()) {
                                            return;
                                        }
                                        // 检查检查点是否已经被丢弃
                                        if (checkpoint.isDisposed()) {
                                            throw new IllegalStateException(
                                                    "Checkpoint "
                                                            + checkpointID
                                                            + " has been discarded");
                                        }
                                        // 如果没有发生异常（throwable 为 null）
                                        if (throwable == null) {
                                            // 通知检查点主节点状态已经被确认
                                            checkpoint.acknowledgeMasterState(
                                                    masterHook.getIdentifier(), masterState);
                                            // 检查是否所有主节点状态都已经被确认
                                            if (checkpoint.areMasterStatesFullyAcknowledged()) {
                                                // 如果所有状态都已确认，则完成 masterStateCompletableFuture
                                                masterStateCompletableFuture.complete(null);
                                            }
                                            // 如果发生异常
                                        } else {
                                            // 使用异常完成 masterStateCompletableFuture
                                            masterStateCompletableFuture.completeExceptionally(
                                                    throwable);
                                        }
                                    }
                                } catch (Throwable t) {
                                    // 如果在处理钩子结果时发生异常，则使用异常完成 masterStateCompletableFuture
                                    masterStateCompletableFuture.completeExceptionally(t);
                                }
                            },
                            timer);
        }
        // 返回 masterStateCompletableFuture，它将在所有主节点钩子处理完或出现异常时完成
        return masterStateCompletableFuture;
    }

    /** Trigger request is successful. NOTE, it must be invoked if trigger request is successful. */
    private void onTriggerSuccess() {
        isTriggering = false;
        executeQueuedRequest();
    }

    /**
     * The trigger request is failed prematurely without a proper initialization. There is no
     * resource to release, but the completion promise needs to fail manually here.
     *
     * @param onCompletionPromise the completion promise of the checkpoint/savepoint
     * @param throwable the reason of trigger failure
     */
    private void onTriggerFailure(
            CheckpointTriggerRequest onCompletionPromise, Throwable throwable) {
        final CheckpointException checkpointException =
                getCheckpointException(
                        CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE, throwable);
        onCompletionPromise.completeExceptionally(checkpointException);
        onTriggerFailure((PendingCheckpoint) null, onCompletionPromise.props, checkpointException);
    }

    private void onTriggerFailure(PendingCheckpoint checkpoint, Throwable throwable) {
        checkArgument(checkpoint != null, "Pending checkpoint can not be null.");

        onTriggerFailure(checkpoint, checkpoint.getProps(), throwable);
    }

    /**
     * The trigger request is failed. NOTE, it must be invoked if trigger request is failed.
     *
     * @param checkpoint the pending checkpoint which is failed. It could be null if it's failed
     *     prematurely without a proper initialization.
     * @param throwable the reason of trigger failure
     */
    private void onTriggerFailure(
            @Nullable PendingCheckpoint checkpoint,
            CheckpointProperties checkpointProperties,
            Throwable throwable) {
        // beautify the stack trace a bit
        throwable = ExceptionUtils.stripCompletionException(throwable);

        try {
            coordinatorsToCheckpoint.forEach(
                    OperatorCoordinatorCheckpointContext::abortCurrentTriggering);

            final CheckpointException cause =
                    getCheckpointException(
                            CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE, throwable);

            if (checkpoint != null && !checkpoint.isDisposed()) {
                synchronized (lock) {
                    abortPendingCheckpoint(checkpoint, cause);
                }
            } else {
                failureManager.handleCheckpointException(
                        checkpoint, checkpointProperties, cause, null, job, null, statsTracker);
            }
        } finally {
            isTriggering = false;
            executeQueuedRequest();
        }
    }

    private void executeQueuedRequest() {
        chooseQueuedRequestToExecute().ifPresent(this::startTriggeringCheckpoint);
    }

    private Optional<CheckpointTriggerRequest> chooseQueuedRequestToExecute() {
        synchronized (lock) {
            return requestDecider.chooseQueuedRequestToExecute(
                    isTriggering, lastCheckpointCompletionRelativeTime);
        }
    }

    private Optional<CheckpointTriggerRequest> chooseRequestToExecute(
            CheckpointTriggerRequest request) {
        synchronized (lock) {
            Optional<CheckpointTriggerRequest> checkpointTriggerRequest =
                    requestDecider.chooseRequestToExecute(
                            request, isTriggering, lastCheckpointCompletionRelativeTime);
            return checkpointTriggerRequest;
        }
    }

    // Returns true if the checkpoint is successfully completed, false otherwise.
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 尝试完成检查点，如果成功完成则返回true，否则返回false
    */
    private boolean maybeCompleteCheckpoint(PendingCheckpoint checkpoint) {
        // 使用锁来保证对检查点状态的同步访问
        synchronized (lock) {
            // 检查检查点是否已经被所有子任务确认
            if (checkpoint.isFullyAcknowledged()) {
                try {
                    // we need to check inside the lock for being shutdown as well,
                    // otherwise we get races and invalid error log messages.
                    // 在锁内部再次检查是否已关闭，以避免竞态条件和无效的错误日志消息
                    // 如果在检查点完成前系统被关闭，则不执行完成操作
                    if (shutdown) {
                        // 如果系统已关闭，则不完成检查点，直接返回false
                        return false;
                    }
                    // 调用方法来实际完成待处理的检查点
                    completePendingCheckpoint(checkpoint);
                    // 如果完成操作成功，则跳出try块，继续执行下面的return语句
                } catch (CheckpointException ce) {
                    // 如果在完成检查点过程中发生异常，则调用失败处理函数
                    onTriggerFailure(checkpoint, ce);
                    //返回false
                    return false;
                }
            }
        }
        return true;
    }

    // --------------------------------------------------------------------------------------------
    //  Handling checkpoints and messages
    // --------------------------------------------------------------------------------------------

    /**
     * Receives a {@link DeclineCheckpoint} message for a pending checkpoint.
     *
     * @param message Checkpoint decline from the task manager
     * @param taskManagerLocationInfo The location info of the decline checkpoint message's sender
     */
    public void receiveDeclineMessage(DeclineCheckpoint message, String taskManagerLocationInfo) {
        if (shutdown || message == null) {
            return;
        }

        if (!job.equals(message.getJob())) {
            throw new IllegalArgumentException(
                    "Received DeclineCheckpoint message for job "
                            + message.getJob()
                            + " from "
                            + taskManagerLocationInfo
                            + " while this coordinator handles job "
                            + job);
        }

        final long checkpointId = message.getCheckpointId();
        final CheckpointException checkpointException =
                message.getSerializedCheckpointException().unwrap();
        final String reason = checkpointException.getMessage();

        PendingCheckpoint checkpoint;

        synchronized (lock) {
            // we need to check inside the lock for being shutdown as well, otherwise we
            // get races and invalid error log messages
            if (shutdown) {
                return;
            }

            checkpoint = pendingCheckpoints.get(checkpointId);

            if (checkpoint != null) {
                Preconditions.checkState(
                        !checkpoint.isDisposed(),
                        "Received message for discarded but non-removed checkpoint "
                                + checkpointId);
                LOG.info(
                        "Decline checkpoint {} by task {} of job {} at {}.",
                        checkpointId,
                        message.getTaskExecutionId(),
                        job,
                        taskManagerLocationInfo,
                        checkpointException.getCause());
                abortPendingCheckpoint(
                        checkpoint, checkpointException, message.getTaskExecutionId());
            } else if (LOG.isDebugEnabled()) {
                if (recentExpiredCheckpoints.contains(checkpointId)) {
                    // message is for an expired checkpoint
                    LOG.debug(
                            "Received another decline message for now expired checkpoint attempt {} from task {} of job {} at {} : {}",
                            checkpointId,
                            message.getTaskExecutionId(),
                            job,
                            taskManagerLocationInfo,
                            reason);
                } else {
                    // message is for an unknown checkpoint. might be so old that we don't even
                    // remember it any more
                    LOG.debug(
                            "Received decline message for unknown (too old?) checkpoint attempt {} from task {} of job {} at {} : {}",
                            checkpointId,
                            message.getTaskExecutionId(),
                            job,
                            taskManagerLocationInfo,
                            reason);
                }
            }
        }
    }

    /**
     * Receives an AcknowledgeCheckpoint message and returns whether the message was associated with
     * a pending checkpoint.
     *
     * @param message Checkpoint ack from the task manager
     * @param taskManagerLocationInfo The location of the acknowledge checkpoint message's sender
     * @return Flag indicating whether the ack'd checkpoint was associated with a pending
     *     checkpoint.
     * @throws CheckpointException If the checkpoint cannot be added to the completed checkpoint
     *     store.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 接收来自任务管理器的AcknowledgeCheckpoint消息，并返回该消息是否与待处理的检查点相关联。
     *
     * @param message 从任务管理器接收到的检查点确认消息
     * @param taskManagerLocationInfo 确认检查点消息的发送者的位置信息
     * @return 一个标志，指示被确认的检查点是否与待处理的检查点相关联
     * @throws CheckpointException 如果无法将检查点添加到已完成的检查点存储中
    */
    public boolean receiveAcknowledgeMessage(
            AcknowledgeCheckpoint message, String taskManagerLocationInfo)
            throws CheckpointException {
        // 如果作业已经关闭或传入的消息为空，则直接返回false
        if (shutdown || message == null) {
            return false;
        }
        // 如果传入的消息所属的作业与当前作业不匹配，则记录错误日志并返回false
        if (!job.equals(message.getJob())) {
            LOG.error(
                    "Received wrong AcknowledgeCheckpoint message for job {} from {} : {}",
                    job,
                    taskManagerLocationInfo,
                    message);
            return false;
        }
        // 获取消息中的检查点ID
        final long checkpointId = message.getCheckpointId();
        // 使用锁来确保操作的原子性，避免并发问题
        synchronized (lock) {
            // we need to check inside the lock for being shutdown as well, otherwise we
            // get races and invalid error log messages
            // 再次检查作业是否已关闭，因为可能在同步块外部关闭
            if (shutdown) {
                return false;
            }
            // 从待处理的检查点集合中查找与传入ID匹配的待处理检查点
            final PendingCheckpoint checkpoint = pendingCheckpoints.get(checkpointId);

            // 检查消息中是否包含了子任务的状态
            if (message.getSubtaskState() != null) {
                // Register shared state regardless of checkpoint state and task ACK state.
                // This way, shared state is
                // 1. kept if the message is late or state will be used by the task otherwise
                // 2. removed eventually upon checkpoint subsumption (or job cancellation)
                // Do not register savepoints' shared state, as Flink is not in charge of
                // savepoints' lifecycle
                // 无论检查点的状态或任务确认状态如何，都注册共享状态。
                // 这样做的目的是：
                // 1. 如果消息是延迟的或者状态将被任务以其他方式使用，则保持共享状态。
                // 2. 当检查点被包含（或作业被取消）时，最终会移除共享状态。

                // 不要注册保存点的共享状态，因为 Flink 不负责管理保存点的生命周期
                // 这里，我们检查是否找到了与消息中的检查点ID对应的PendingCheckpoint对象
                // 并且该检查点不是保存点（即它是一个常规的检查点）
                if (checkpoint == null || !checkpoint.getProps().isSavepoint()) {
                    // 调用消息中的子任务状态对象的registerSharedStates方法
                    // 将其共享状态注册到completedCheckpointStore的共享状态注册表中
                    // 使用当前检查点的ID作为关联标识
                    // 这样，共享的状态可以在需要时被检索或管理
                    message.getSubtaskState()
                            .registerSharedStates(
                                    completedCheckpointStore.getSharedStateRegistry(),
                                    checkpointId);
                }
            }

            // 确保当前有一个有效的检查点对象且它没有被销毁
            if (checkpoint != null && !checkpoint.isDisposed()) {

                // 尝试确认任务的检查点
                // 此方法会处理与给定任务的检查点确认相关的所有逻辑
                // 包括检查任务ID、状态以及可能的度量指标
                switch (checkpoint.acknowledgeTask(
                        message.getTaskExecutionId(),
                        message.getSubtaskState(),
                        message.getCheckpointMetrics())) {
                    case SUCCESS:
                        // 如果确认成功  打印日志
                        LOG.debug(
                                "Received acknowledge message for checkpoint {} from task {} of job {} at {}.",
                                checkpointId,
                                message.getTaskExecutionId(),
                                message.getJob(),
                                taskManagerLocationInfo);
                        // 如果检查点已经被所有相关任务确认
                        if (checkpoint.isFullyAcknowledged()) {
                            // 完成挂起的检查点（例如，将其标记为已完成，并可能触发清理逻辑）
                            completePendingCheckpoint(checkpoint);
                        }
                        break;
                    case DUPLICATE:
                        // 如果确认消息是重复的打印日志
                        LOG.debug(
                                "Received a duplicate acknowledge message for checkpoint {}, task {}, job {}, location {}.",
                                message.getCheckpointId(),
                                message.getTaskExecutionId(),
                                message.getJob(),
                                taskManagerLocationInfo);
                        break;
                    case UNKNOWN:
                        // 如果无法确认检查点（因为任务的执行尝试ID未知）
                        LOG.warn(
                                "Could not acknowledge the checkpoint {} for task {} of job {} at {}, "
                                        + "because the task's execution attempt id was unknown. Discarding "
                                        + "the state handle to avoid lingering state.",
                                message.getCheckpointId(),
                                message.getTaskExecutionId(),
                                message.getJob(),
                                taskManagerLocationInfo);
                        // 丢弃与该消息关联的子任务状态，避免状态滞留
                        discardSubtaskState(
                                message.getJob(),
                                message.getTaskExecutionId(),
                                message.getCheckpointId(),
                                message.getSubtaskState());

                        break;
                    // 如果挂起的检查点已被丢弃  打印日志
                    case DISCARDED:
                        LOG.warn(
                                "Could not acknowledge the checkpoint {} for task {} of job {} at {}, "
                                        + "because the pending checkpoint had been discarded. Discarding the "
                                        + "state handle tp avoid lingering state.",
                                message.getCheckpointId(),
                                message.getTaskExecutionId(),
                                message.getJob(),
                                taskManagerLocationInfo);
                        // 丢弃与该消息关联的子任务状态，避免状态滞留
                        discardSubtaskState(
                                message.getJob(),
                                message.getTaskExecutionId(),
                                message.getCheckpointId(),
                                message.getSubtaskState());
                }

                return true;
                // 如果之前的检查点确认逻辑未能成功处理，且检查点对象本身不为空，正常不会触发
            } else if (checkpoint != null) {
                // this should not happen
                //抛出异常
                throw new IllegalStateException(
                        "Received message for discarded but non-removed checkpoint "
                                + checkpointId);
            } else {
                // 如果检查点对象为空（可能已经被处理或销毁），则继续处理其他逻辑
                // 报告与检查点相关的度量指标（例如，统计信息、性能数据等）
                reportCheckpointMetrics(
                        message.getCheckpointId(),
                        message.getTaskExecutionId(),
                        message.getCheckpointMetrics());
                // 标记变量，用于表示检查点是否之前处于挂起状态
                boolean wasPendingCheckpoint;

                // message is for an unknown checkpoint, or comes too late (checkpoint disposed)
                // 消息是针对一个已经过期或未知的检查点，或者消息来得太晚（检查点已经被销毁）
                // 检查检查点ID是否存在于最近过期的检查点列表中
                if (recentExpiredCheckpoints.contains(checkpointId)) {
                    // 如果是，则表示该检查点之前处于挂起状态
                    wasPendingCheckpoint = true;
                    // 记录警告日志，说明收到了一个已过期检查点的消息
                    LOG.warn(
                            "Received late message for now expired checkpoint attempt {} from task "
                                    + "{} of job {} at {}.",
                            checkpointId,
                            message.getTaskExecutionId(),
                            message.getJob(),
                            taskManagerLocationInfo);
                } else {
                    // 如果不在过期列表中，则说明这是一个未知的检查点
                    // 记录调试日志，说明收到了一个未知检查点的消息
                    LOG.debug(
                            "Received message for an unknown checkpoint {} from task {} of job {} at {}.",
                            checkpointId,
                            message.getTaskExecutionId(),
                            message.getJob(),
                            taskManagerLocationInfo);
                    wasPendingCheckpoint = false;
                }

                // try to discard the state so that we don't have lingering state lying around
                // 尝试丢弃与该消息关联的子任务状态，以避免状态滞留
                discardSubtaskState(
                        message.getJob(),
                        message.getTaskExecutionId(),
                        message.getCheckpointId(),
                        message.getSubtaskState());
                // 返回 wasPendingCheckpoint 的值，以便调用者知道是否之前存在挂起的检查点
                return wasPendingCheckpoint;
            }
        }
    }

    /**
     * Try to complete the given pending checkpoint.
     *
     * <p>Important: This method should only be called in the checkpoint lock scope.
     *
     * @param pendingCheckpoint to complete
     * @throws CheckpointException if the completion failed
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 尝试完成给定的待处理检查点。
     *
     * <p>重要提示：该方法应仅在检查点锁的范围内调用。
     *
     * @param pendingCheckpoint 要完成的待处理检查点
     * @throws CheckpointException 如果完成失败，则抛出此异常
    */
    private void completePendingCheckpoint(PendingCheckpoint pendingCheckpoint)
            throws CheckpointException {
        // 获取待处理检查点的ID
        final long checkpointId = pendingCheckpoint.getCheckpointID();
        // 初始化一个用于存储完成后的检查点的变量
        final CompletedCheckpoint completedCheckpoint;
        // 初始化一个用于存储被最新检查点替代的旧检查点的变量
        final CompletedCheckpoint lastSubsumed;
        // 获取检查点的属性
        final CheckpointProperties props = pendingCheckpoint.getProps();

        // 通知共享状态注册表该检查点已完成
        completedCheckpointStore.getSharedStateRegistry().checkpointCompleted(checkpointId);

        try {
            // 对待处理检查点进行最后的处理，如保存状态等，并返回完成后的检查点
            completedCheckpoint = finalizeCheckpoint(pendingCheckpoint);

            // the pending checkpoint must be discarded after the finalization
            // 在完成处理后，待处理检查点必须被丢弃，并且completedCheckpoint不能为空
            // 使用Preconditions进行状态检查
            Preconditions.checkState(pendingCheckpoint.isDisposed() && completedCheckpoint != null);

            // 如果不是保存点（Savepoint）
            if (!props.isSavepoint()) {
                // 将完成的检查点添加到存储中，并替代最旧的检查点
                lastSubsumed =
                        addCompletedCheckpointToStoreAndSubsumeOldest(
                                checkpointId, completedCheckpoint, pendingCheckpoint);
            } else {
                // 如果是保存点，则不需要替代旧的检查点
                lastSubsumed = null;
            }
            // 通知等待完成的Future，检查点已完成，并传入完成的检查点对象
            pendingCheckpoint.getCompletionFuture().complete(completedCheckpoint);
            // 报告完成的检查点
            reportCompletedCheckpoint(completedCheckpoint);
        } catch (Exception exception) {
            // For robustness reasons, we need catch exception and try marking the checkpoint
            // completed.
            // 使用completeExceptionally方法将异常传递给等待完成的Future
            pendingCheckpoint.getCompletionFuture().completeExceptionally(exception);
            //抛出异常
            throw exception;
        } finally {
            // 无论是否发生异常，都要从待处理检查点列表中移除该检查点
            pendingCheckpoints.remove(checkpointId);
            // 调度下一次检查点触发请求
            scheduleTriggerRequest();
        }

        cleanupAfterCompletedCheckpoint(
                pendingCheckpoint, checkpointId, completedCheckpoint, lastSubsumed, props);
    }

    private void reportCompletedCheckpoint(CompletedCheckpoint completedCheckpoint) {
        failureManager.handleCheckpointSuccess(completedCheckpoint.getCheckpointID());
        CompletedCheckpointStats completedCheckpointStats = completedCheckpoint.getStatistic();
        if (completedCheckpointStats != null) {
            LOG.trace(
                    "Checkpoint {} size: {}Kb, duration: {}ms",
                    completedCheckpoint.getCheckpointID(),
                    completedCheckpointStats.getStateSize() == 0
                            ? 0
                            : completedCheckpointStats.getStateSize() / 1024,
                    completedCheckpointStats.getEndToEndDuration());
            // Finalize the statsCallback and give the completed checkpoint a
            // callback for discards.
            statsTracker.reportCompletedCheckpoint(completedCheckpointStats);
        }
    }

    private void cleanupAfterCompletedCheckpoint(
            PendingCheckpoint pendingCheckpoint,
            long checkpointId,
            CompletedCheckpoint completedCheckpoint,
            CompletedCheckpoint lastSubsumed,
            CheckpointProperties props) {

        // record the time when this was completed, to calculate
        // the 'min delay between checkpoints'
        lastCheckpointCompletionRelativeTime = clock.relativeTimeMillis();

        logCheckpointInfo(completedCheckpoint);

        if (!props.isSavepoint() || props.isSynchronous()) {
            // drop those pending checkpoints that are at prior to the completed one
            dropSubsumedCheckpoints(checkpointId);

            // send the "notify complete" call to all vertices, coordinators, etc.
            sendAcknowledgeMessages(
                    pendingCheckpoint.getCheckpointPlan().getTasksToCommitTo(),
                    checkpointId,
                    completedCheckpoint.getTimestamp(),
                    extractIdIfDiscardedOnSubsumed(lastSubsumed));
        }
    }

    private void logCheckpointInfo(CompletedCheckpoint completedCheckpoint) {
        LOG.info(
                "Completed checkpoint {} for job {} ({} bytes, checkpointDuration={} ms, finalizationTime={} ms).",
                completedCheckpoint.getCheckpointID(),
                job,
                completedCheckpoint.getStateSize(),
                completedCheckpoint.getCompletionTimestamp() - completedCheckpoint.getTimestamp(),
                System.currentTimeMillis() - completedCheckpoint.getCompletionTimestamp());

        if (LOG.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder();
            builder.append("Checkpoint state: ");
            for (OperatorState state : completedCheckpoint.getOperatorStates().values()) {
                builder.append(state);
                builder.append(", ");
            }
            // Remove last two chars ", "
            builder.setLength(builder.length() - 2);

            LOG.debug(builder.toString());
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 尝试对给定的待处理检查点进行最终化处理，包括保存状态等操作。
     *
     * @param pendingCheckpoint 待处理的检查点
     * @return 完成的检查点
     * @throws CheckpointException 如果最终化处理失败，则抛出此异常
    */
    private CompletedCheckpoint finalizeCheckpoint(PendingCheckpoint pendingCheckpoint)
            throws CheckpointException {
        try {
            // 调用待处理检查点的finalizeCheckpoint方法，进行最终化处理
            // 该方法可能涉及状态保存、资源清理等操作
            // 传入检查点清理器、调度触发请求的方法引用和执行器
            final CompletedCheckpoint completedCheckpoint =
                    pendingCheckpoint.finalizeCheckpoint(
                            checkpointsCleaner, this::scheduleTriggerRequest, executor);
            // 返回完成后的检查点对象
            return completedCheckpoint;
        } catch (Exception e1) {
            // abort the current pending checkpoint if we fails to finalize the pending
            // checkpoint.
            // 如果在最终化处理过程中发生异常，则终止当前的待处理检查点
            // 终止原因根据异常类型决定
            final CheckpointFailureReason failureReason =
                    e1 instanceof PartialFinishingNotSupportedByStateException
                            ? CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_CLOSING
                            : CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE;
            // 如果待处理检查点还未被丢弃（即尚未被外部清理）
            if (!pendingCheckpoint.isDisposed()) {
                // 终止待处理检查点，并传入异常信息
                abortPendingCheckpoint(
                        pendingCheckpoint, new CheckpointException(failureReason, e1));
            }
            // 抛出新的异常，包含失败的检查点ID和失败原因
            throw new CheckpointException(
                    "Could not finalize the pending checkpoint "
                            + pendingCheckpoint.getCheckpointID()
                            + '.',
                    failureReason,
                    e1);
        }
    }

    private long extractIdIfDiscardedOnSubsumed(CompletedCheckpoint lastSubsumed) {
        final long lastSubsumedCheckpointId;
        if (lastSubsumed != null && lastSubsumed.getProperties().discardOnSubsumed()) {
            lastSubsumedCheckpointId = lastSubsumed.getCheckpointID();
        } else {
            lastSubsumedCheckpointId = CheckpointStoreUtil.INVALID_CHECKPOINT_ID;
        }
        return lastSubsumedCheckpointId;
    }

    private CompletedCheckpoint addCompletedCheckpointToStoreAndSubsumeOldest(
            long checkpointId,
            CompletedCheckpoint completedCheckpoint,
            PendingCheckpoint pendingCheckpoint)
            throws CheckpointException {
        List<ExecutionVertex> tasksToAbort =
                pendingCheckpoint.getCheckpointPlan().getTasksToCommitTo();
        try {
            final CompletedCheckpoint subsumedCheckpoint =
                    completedCheckpointStore.addCheckpointAndSubsumeOldestOne(
                            completedCheckpoint, checkpointsCleaner, this::scheduleTriggerRequest);
            // reset the force full snapshot flag, we should've completed at least one full
            // snapshot by now
            this.forceFullSnapshot = false;
            return subsumedCheckpoint;
        } catch (Exception exception) {
            pendingCheckpoint.getCompletionFuture().completeExceptionally(exception);
            if (exception instanceof PossibleInconsistentStateException) {
                LOG.warn(
                        "An error occurred while writing checkpoint {} to the underlying metadata"
                                + " store. Flink was not able to determine whether the metadata was"
                                + " successfully persisted. The corresponding state located at '{}'"
                                + " won't be discarded and needs to be cleaned up manually.",
                        completedCheckpoint.getCheckpointID(),
                        completedCheckpoint.getExternalPointer());
            } else {
                // we failed to store the completed checkpoint. Let's clean up
                checkpointsCleaner.cleanCheckpointOnFailedStoring(completedCheckpoint, executor);
            }

            final CheckpointException checkpointException =
                    new CheckpointException(
                            "Could not complete the pending checkpoint " + checkpointId + '.',
                            CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE,
                            exception);
            reportFailedCheckpoint(pendingCheckpoint, checkpointException);
            sendAbortedMessages(tasksToAbort, checkpointId, completedCheckpoint.getTimestamp());
            throw checkpointException;
        }
    }

    private void reportFailedCheckpoint(
            PendingCheckpoint pendingCheckpoint, CheckpointException exception) {

        failureManager.handleCheckpointException(
                pendingCheckpoint,
                pendingCheckpoint.getProps(),
                exception,
                null,
                job,
                getStatsCallback(pendingCheckpoint),
                statsTracker);
    }

    void scheduleTriggerRequest() {
        synchronized (lock) {
            if (isShutdown()) {
                LOG.debug(
                        "Skip scheduling trigger request because the CheckpointCoordinator is shut down");
            } else {
                timer.execute(this::executeQueuedRequest);
            }
        }
    }

    @VisibleForTesting
    void sendAcknowledgeMessages(
            List<ExecutionVertex> tasksToCommit,
            long completedCheckpointId,
            long completedTimestamp,
            long lastSubsumedCheckpointId) {
        // commit tasks
        for (ExecutionVertex ev : tasksToCommit) {
            Execution ee = ev.getCurrentExecutionAttempt();
            if (ee != null) {
                ee.notifyCheckpointOnComplete(
                        completedCheckpointId, completedTimestamp, lastSubsumedCheckpointId);
            }
        }

        // commit coordinators
        for (OperatorCoordinatorCheckpointContext coordinatorContext : coordinatorsToCheckpoint) {
            coordinatorContext.notifyCheckpointComplete(completedCheckpointId);
        }
    }

    private void sendAbortedMessages(
            List<ExecutionVertex> tasksToAbort, long checkpointId, long timeStamp) {
        assert (Thread.holdsLock(lock));
        long latestCompletedCheckpointId = completedCheckpointStore.getLatestCheckpointId();

        // send notification of aborted checkpoints asynchronously.
        executor.execute(
                () -> {
                    // send the "abort checkpoint" messages to necessary vertices.
                    for (ExecutionVertex ev : tasksToAbort) {
                        Execution ee = ev.getCurrentExecutionAttempt();
                        if (ee != null) {
                            try {
                                ee.notifyCheckpointAborted(
                                        checkpointId, latestCompletedCheckpointId, timeStamp);
                            } catch (Throwable e) {
                                LOG.warn(
                                        "Could not send aborted message of checkpoint {} to task {} belonging to job {}.",
                                        checkpointId,
                                        ee.getAttemptId(),
                                        ee.getVertex().getJobId(),
                                        e);
                            }
                        }
                    }
                });

        // commit coordinators
        for (OperatorCoordinatorCheckpointContext coordinatorContext : coordinatorsToCheckpoint) {
            coordinatorContext.notifyCheckpointAborted(checkpointId);
        }
    }

    /**
     * Fails all pending checkpoints which have not been acknowledged by the given execution attempt
     * id.
     *
     * @param executionAttemptId for which to discard unacknowledged pending checkpoints
     * @param cause of the failure
     */
    public void failUnacknowledgedPendingCheckpointsFor(
            ExecutionAttemptID executionAttemptId, Throwable cause) {
        synchronized (lock) {
            abortPendingCheckpoints(
                    checkpoint -> !checkpoint.isAcknowledgedBy(executionAttemptId),
                    new CheckpointException(CheckpointFailureReason.TASK_FAILURE, cause));
        }
    }

    private void rememberRecentExpiredCheckpointId(long id) {
        if (recentExpiredCheckpoints.size() >= NUM_GHOST_CHECKPOINT_IDS) {
            recentExpiredCheckpoints.removeFirst();
        }
        recentExpiredCheckpoints.addLast(id);
    }

    private void dropSubsumedCheckpoints(long checkpointId) {
        abortPendingCheckpoints(
                checkpoint ->
                        checkpoint.getCheckpointID() < checkpointId && checkpoint.canBeSubsumed(),
                new CheckpointException(CheckpointFailureReason.CHECKPOINT_SUBSUMED));
    }

    // --------------------------------------------------------------------------------------------
    //  Checkpoint State Restoring
    // --------------------------------------------------------------------------------------------

    /**
     * Restores the latest checkpointed state to a set of subtasks. This method represents a "local"
     * or "regional" failover and does restore states to coordinators. Note that a regional failover
     * might still include all tasks.
     *
     * @param tasks Set of job vertices to restore. State for these vertices is restored via {@link
     *     Execution#setInitialState(JobManagerTaskRestore)}.
     * @return An {@code OptionalLong} with the checkpoint ID, if state was restored, an empty
     *     {@code OptionalLong} otherwise.
     * @throws IllegalStateException If the CheckpointCoordinator is shut down.
     * @throws IllegalStateException If no completed checkpoint is available and the <code>
     *     failIfNoCheckpoint</code> flag has been set.
     * @throws IllegalStateException If the checkpoint contains state that cannot be mapped to any
     *     job vertex in <code>tasks</code> and the <code>allowNonRestoredState</code> flag has not
     *     been set.
     * @throws IllegalStateException If the max parallelism changed for an operator that restores
     *     state from this checkpoint.
     * @throws IllegalStateException If the parallelism changed for an operator that restores
     *     <i>non-partitioned</i> state from this checkpoint.
     */
    public OptionalLong restoreLatestCheckpointedStateToSubtasks(
            final Set<ExecutionJobVertex> tasks) throws Exception {
        // when restoring subtasks only we accept potentially unmatched state for the
        // following reasons
        //   - the set frequently does not include all Job Vertices (only the ones that are part
        //     of the restarted region), meaning there will be unmatched state by design.
        //   - because what we might end up restoring from an original savepoint with unmatched
        //     state, if there is was no checkpoint yet.
        return restoreLatestCheckpointedStateInternal(
                tasks,
                OperatorCoordinatorRestoreBehavior
                        .SKIP, // local/regional recovery does not reset coordinators
                false, // recovery might come before first successful checkpoint
                true,
                false); // see explanation above
    }

    /**
     * Restores the latest checkpointed state to all tasks and all coordinators. This method
     * represents a "global restore"-style operation where all stateful tasks and coordinators from
     * the given set of Job Vertices are restored. are restored to their latest checkpointed state.
     *
     * @param tasks Set of job vertices to restore. State for these vertices is restored via {@link
     *     Execution#setInitialState(JobManagerTaskRestore)}.
     * @param allowNonRestoredState Allow checkpoint state that cannot be mapped to any job vertex
     *     in tasks.
     * @return <code>true</code> if state was restored, <code>false</code> otherwise.
     * @throws IllegalStateException If the CheckpointCoordinator is shut down.
     * @throws IllegalStateException If no completed checkpoint is available and the <code>
     *     failIfNoCheckpoint</code> flag has been set.
     * @throws IllegalStateException If the checkpoint contains state that cannot be mapped to any
     *     job vertex in <code>tasks</code> and the <code>allowNonRestoredState</code> flag has not
     *     been set.
     * @throws IllegalStateException If the max parallelism changed for an operator that restores
     *     state from this checkpoint.
     * @throws IllegalStateException If the parallelism changed for an operator that restores
     *     <i>non-partitioned</i> state from this checkpoint.
     */
    public boolean restoreLatestCheckpointedStateToAll(
            final Set<ExecutionJobVertex> tasks, final boolean allowNonRestoredState)
            throws Exception {

        final OptionalLong restoredCheckpointId =
                restoreLatestCheckpointedStateInternal(
                        tasks,
                        OperatorCoordinatorRestoreBehavior
                                .RESTORE_OR_RESET, // global recovery restores coordinators, or
                        // resets them to empty
                        false, // recovery might come before first successful checkpoint
                        allowNonRestoredState,
                        false);

        return restoredCheckpointId.isPresent();
    }

    /**
     * Restores the latest checkpointed at the beginning of the job execution. If there is a
     * checkpoint, this method acts like a "global restore"-style operation where all stateful tasks
     * and coordinators from the given set of Job Vertices are restored.
     *
     * @param tasks Set of job vertices to restore. State for these vertices is restored via {@link
     *     Execution#setInitialState(JobManagerTaskRestore)}.
     * @return True, if a checkpoint was found and its state was restored, false otherwise.
     */
    public boolean restoreInitialCheckpointIfPresent(final Set<ExecutionJobVertex> tasks)
            throws Exception {
        final OptionalLong restoredCheckpointId =
                restoreLatestCheckpointedStateInternal(
                        tasks,
                        OperatorCoordinatorRestoreBehavior.RESTORE_IF_CHECKPOINT_PRESENT,
                        false, // initial checkpoints exist only on JobManager failover. ok if not
                        // present.
                        false,
                        true); // JobManager failover means JobGraphs match exactly.

        return restoredCheckpointId.isPresent();
    }

    /**
     * Performs the actual restore operation to the given tasks.
     *
     * <p>This method returns the restored checkpoint ID (as an optional) or an empty optional, if
     * no checkpoint was restored.
     */
    private OptionalLong restoreLatestCheckpointedStateInternal(
            final Set<ExecutionJobVertex> tasks,
            final OperatorCoordinatorRestoreBehavior operatorCoordinatorRestoreBehavior,
            final boolean errorIfNoCheckpoint,
            final boolean allowNonRestoredState,
            final boolean checkForPartiallyFinishedOperators)
            throws Exception {

        synchronized (lock) {
            if (shutdown) {
                throw new IllegalStateException("CheckpointCoordinator is shut down");
            }
            long restoreTimestamp = SystemClock.getInstance().absoluteTimeMillis();
            statsTracker.reportInitializationStartTs(restoreTimestamp);

            // Restore from the latest checkpoint
            CompletedCheckpoint latest = completedCheckpointStore.getLatestCheckpoint();

            if (latest == null) {
                LOG.info("No checkpoint found during restore.");

                if (errorIfNoCheckpoint) {
                    throw new IllegalStateException("No completed checkpoint available");
                }

                LOG.debug("Resetting the master hooks.");
                MasterHooks.reset(masterHooks.values(), LOG);

                if (operatorCoordinatorRestoreBehavior
                        == OperatorCoordinatorRestoreBehavior.RESTORE_OR_RESET) {
                    // we let the JobManager-side components know that there was a recovery,
                    // even if there was no checkpoint to recover from, yet
                    LOG.info("Resetting the Operator Coordinators to an empty state.");
                    restoreStateToCoordinators(
                            OperatorCoordinator.NO_CHECKPOINT, Collections.emptyMap());
                }

                return OptionalLong.empty();
            }

            statsTracker.reportRestoredCheckpoint(
                    latest.getCheckpointID(),
                    latest.getProperties(),
                    latest.getExternalPointer(),
                    latest.getStateSize());

            LOG.info("Restoring job {} from {}.", job, latest);

            this.forceFullSnapshot = latest.getProperties().isUnclaimed();

            // re-assign the task states
            final Map<OperatorID, OperatorState> operatorStates = extractOperatorStates(latest);

            if (checkForPartiallyFinishedOperators) {
                VertexFinishedStateChecker vertexFinishedStateChecker =
                        vertexFinishedStateCheckerFactory.apply(tasks, operatorStates);
                vertexFinishedStateChecker.validateOperatorsFinishedState();
            }

            StateAssignmentOperation stateAssignmentOperation =
                    new StateAssignmentOperation(
                            latest.getCheckpointID(), tasks, operatorStates, allowNonRestoredState);

            stateAssignmentOperation.assignStates();

            // call master hooks for restore. we currently call them also on "regional restore"
            // because
            // there is no other failure notification mechanism in the master hooks
            // ultimately these should get removed anyways in favor of the operator coordinators

            MasterHooks.restoreMasterHooks(
                    masterHooks,
                    latest.getMasterHookStates(),
                    latest.getCheckpointID(),
                    allowNonRestoredState,
                    LOG);

            if (operatorCoordinatorRestoreBehavior != OperatorCoordinatorRestoreBehavior.SKIP) {
                restoreStateToCoordinators(latest.getCheckpointID(), operatorStates);
            }

            return OptionalLong.of(latest.getCheckpointID());
        }
    }

    private Map<OperatorID, OperatorState> extractOperatorStates(CompletedCheckpoint checkpoint) {
        Map<OperatorID, OperatorState> originalOperatorStates = checkpoint.getOperatorStates();

        if (checkpoint.getCheckpointID() != checkpointIdOfIgnoredInFlightData) {
            // Don't do any changes if it is not required.
            return originalOperatorStates;
        }

        HashMap<OperatorID, OperatorState> newStates = new HashMap<>();
        // Create the new operator states without in-flight data.
        for (OperatorState originalOperatorState : originalOperatorStates.values()) {
            newStates.put(
                    originalOperatorState.getOperatorID(),
                    originalOperatorState.copyAndDiscardInFlightData());
        }

        return newStates;
    }

    /**
     * Restore the state with given savepoint.
     *
     * @param restoreSettings Settings for a snapshot to restore from. Includes the path and
     *     parameters for the restore process.
     * @param tasks Map of job vertices to restore. State for these vertices is restored via {@link
     *     Execution#setInitialState(JobManagerTaskRestore)}.
     * @param userClassLoader The class loader to resolve serialized classes in legacy savepoint
     *     versions.
     */
    public boolean restoreSavepoint(
            SavepointRestoreSettings restoreSettings,
            Map<JobVertexID, ExecutionJobVertex> tasks,
            ClassLoader userClassLoader)
            throws Exception {

        final String savepointPointer = restoreSettings.getRestorePath();
        final boolean allowNonRestored = restoreSettings.allowNonRestoredState();
        Preconditions.checkNotNull(savepointPointer, "The savepoint path cannot be null.");

        LOG.info(
                "Starting job {} from savepoint {} ({})",
                job,
                savepointPointer,
                (allowNonRestored ? "allowing non restored state" : ""));

        final CompletedCheckpointStorageLocation checkpointLocation =
                checkpointStorageView.resolveCheckpoint(savepointPointer);

        // convert to checkpoint so the system can fall back to it
        final CheckpointProperties checkpointProperties;
        switch (restoreSettings.getRestoreMode()) {
            case CLAIM:
                checkpointProperties = this.checkpointProperties;
                break;
            case LEGACY:
                checkpointProperties =
                        CheckpointProperties.forSavepoint(
                                false,
                                // we do not care about the format when restoring, the format is
                                // necessary when triggering a savepoint
                                SavepointFormatType.CANONICAL);
                break;
            case NO_CLAIM:
                checkpointProperties = CheckpointProperties.forUnclaimedSnapshot();
                break;
            default:
                throw new IllegalArgumentException("Unknown snapshot restore mode");
        }

        // Load the savepoint as a checkpoint into the system
        CompletedCheckpoint savepoint =
                Checkpoints.loadAndValidateCheckpoint(
                        job,
                        tasks,
                        checkpointLocation,
                        userClassLoader,
                        allowNonRestored,
                        checkpointProperties);

        // register shared state - even before adding the checkpoint to the store
        // because the latter might trigger subsumption so the ref counts must be up-to-date
        savepoint.registerSharedStatesAfterRestored(
                completedCheckpointStore.getSharedStateRegistry(),
                restoreSettings.getRestoreMode());

        completedCheckpointStore.addCheckpointAndSubsumeOldestOne(
                savepoint, checkpointsCleaner, this::scheduleTriggerRequest);

        // Reset the checkpoint ID counter
        long nextCheckpointId = savepoint.getCheckpointID() + 1;
        checkpointIdCounter.setCount(nextCheckpointId);

        LOG.info("Reset the checkpoint ID of job {} to {}.", job, nextCheckpointId);

        final OptionalLong restoredCheckpointId =
                restoreLatestCheckpointedStateInternal(
                        new HashSet<>(tasks.values()),
                        OperatorCoordinatorRestoreBehavior.RESTORE_IF_CHECKPOINT_PRESENT,
                        true,
                        allowNonRestored,
                        true);

        return restoredCheckpointId.isPresent();
    }

    // ------------------------------------------------------------------------
    //  Accessors
    // ------------------------------------------------------------------------

    public int getNumberOfPendingCheckpoints() {
        synchronized (lock) {
            return this.pendingCheckpoints.size();
        }
    }

    public int getNumberOfRetainedSuccessfulCheckpoints() {
        synchronized (lock) {
            return completedCheckpointStore.getNumberOfRetainedCheckpoints();
        }
    }

    public Map<Long, PendingCheckpoint> getPendingCheckpoints() {
        synchronized (lock) {
            return new HashMap<>(this.pendingCheckpoints);
        }
    }

    public List<CompletedCheckpoint> getSuccessfulCheckpoints() throws Exception {
        synchronized (lock) {
            return completedCheckpointStore.getAllCheckpoints();
        }
    }

    @VisibleForTesting
    public ArrayDeque<Long> getRecentExpiredCheckpoints() {
        return recentExpiredCheckpoints;
    }

    public CheckpointStorageCoordinatorView getCheckpointStorage() {
        return checkpointStorageView;
    }

    public CompletedCheckpointStore getCheckpointStore() {
        return completedCheckpointStore;
    }

    /**
     * Gets the checkpoint interval. Its value might vary depending on whether there is processing
     * backlog.
     */
    private long getCurrentCheckpointInterval() {
        return backlogOperators.isEmpty() ? baseInterval : baseIntervalDuringBacklog;
    }

    public long getCheckpointTimeout() {
        return checkpointTimeout;
    }

    /** @deprecated use {@link #getNumQueuedRequests()} */
    @Deprecated
    @VisibleForTesting
    PriorityQueue<CheckpointTriggerRequest> getTriggerRequestQueue() {
        synchronized (lock) {
            return requestDecider.getTriggerRequestQueue();
        }
    }

    public boolean isTriggering() {
        return isTriggering;
    }

    @VisibleForTesting
    boolean isCurrentPeriodicTriggerAvailable() {
        return currentPeriodicTrigger != null;
    }

    /**
     * Returns whether periodic checkpointing has been configured.
     *
     * @return <code>true</code> if periodic checkpoints have been configured.
     */
    public boolean isPeriodicCheckpointingConfigured() {
        return baseInterval != CheckpointCoordinatorConfiguration.DISABLED_CHECKPOINT_INTERVAL;
    }

    // --------------------------------------------------------------------------------------------
    //  Periodic scheduling of checkpoints
    // --------------------------------------------------------------------------------------------

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *  启动检查点调度器的方法
    */
    public void startCheckpointScheduler() {
        // 使用锁来确保并发访问时的一致性
        synchronized (lock) {
            // 检查是否已关闭，若已关闭则抛出异常
            if (shutdown) {
                throw new IllegalArgumentException("Checkpoint coordinator is shut down");
            }
            // 使用Preconditions工具类来检查是否配置了周期性检查点
            // 如果没有配置周期性检查点，则抛出状态异常
            Preconditions.checkState(
                    isPeriodicCheckpointingConfigured(),
                    "Can not start checkpoint scheduler, if no periodic checkpointing is configured");

            // make sure all prior timers are cancelled
            // 确保所有之前的定时器都被取消，防止重复调度
            stopCheckpointScheduler();
            // 设置periodicScheduling为true，表示现在处于周期性调度状态
            periodicScheduling = true;
            /**
             *  使用clock.relativeTimeMillis()获取当前相对时间（可能是相对于某个基准点的时间），并加上一个随机初始延迟
             * 然后调用scheduleTriggerWithDelay方法来调度检查点触发器
             * scheduleTriggerWithDelay会安排一个定时任务来触发检查点
             */
            scheduleTriggerWithDelay(clock.relativeTimeMillis(), getRandomInitDelay());
        }
    }

    public void stopCheckpointScheduler() {
        synchronized (lock) {
            periodicScheduling = false;

            cancelPeriodicTrigger();

            final CheckpointException reason =
                    new CheckpointException(CheckpointFailureReason.CHECKPOINT_COORDINATOR_SUSPEND);
            abortPendingAndQueuedCheckpoints(reason);
        }
    }

    public boolean isPeriodicCheckpointingStarted() {
        return periodicScheduling;
    }

    /**
     * Aborts all the pending checkpoints due to en exception.
     *
     * @param exception The exception.
     */
    public void abortPendingCheckpoints(CheckpointException exception) {
        synchronized (lock) {
            abortPendingCheckpoints(ignored -> true, exception);
        }
    }

    private void abortPendingCheckpoints(
            Predicate<PendingCheckpoint> checkpointToFailPredicate, CheckpointException exception) {

        assert Thread.holdsLock(lock);

        final PendingCheckpoint[] pendingCheckpointsToFail =
                pendingCheckpoints.values().stream()
                        .filter(checkpointToFailPredicate)
                        .toArray(PendingCheckpoint[]::new);

        // do not traverse pendingCheckpoints directly, because it might be changed during
        // traversing
        for (PendingCheckpoint pendingCheckpoint : pendingCheckpointsToFail) {
            abortPendingCheckpoint(pendingCheckpoint, exception);
        }
    }

    private void rescheduleTrigger(long currentTimeMillis, long tillNextMillis) {
        cancelPeriodicTrigger();
        scheduleTriggerWithDelay(currentTimeMillis, tillNextMillis);
    }

    private void cancelPeriodicTrigger() {
        if (currentPeriodicTrigger != null) {
            nextCheckpointTriggeringRelativeTime = Long.MAX_VALUE;
            currentPeriodicTriggerFuture.cancel(false);
            currentPeriodicTrigger = null;
            currentPeriodicTriggerFuture = null;
        }
    }

    private long getRandomInitDelay() {
        return ThreadLocalRandom.current().nextLong(minPauseBetweenCheckpoints, baseInterval + 1L);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 延迟调度器
    */
    private void scheduleTriggerWithDelay(long currentRelativeTime, long initDelay) {
        // 计算下一个检查点触发的相对时间，为当前相对时间加上初始延迟
        nextCheckpointTriggeringRelativeTime = currentRelativeTime + initDelay;
        // 创建一个新的周期性触发器实例
        currentPeriodicTrigger = new ScheduledTrigger();
        /**
         *  使用timer来调度触发器，在initDelay毫秒后首次触发，之后每隔固定时间间隔（未在方法参数中给出）再次触发
         * 触发器的执行逻辑在ScheduledTrigger类的run方法中定义
         * currentPeriodicTriggerFuture是ScheduledFuture对象，用于管理这个定时任务
         * 如果需要，可以通过这个对象来取消定时任务
         */
        currentPeriodicTriggerFuture =
                timer.schedule(currentPeriodicTrigger, initDelay, TimeUnit.MILLISECONDS);
    }

    private void restoreStateToCoordinators(
            final long checkpointId, final Map<OperatorID, OperatorState> operatorStates)
            throws Exception {

        for (OperatorCoordinatorCheckpointContext coordContext : coordinatorsToCheckpoint) {
            final OperatorState state = operatorStates.get(coordContext.operatorId());
            final ByteStreamStateHandle coordinatorState =
                    state == null ? null : state.getCoordinatorState();
            final byte[] bytes = coordinatorState == null ? null : coordinatorState.getData();
            coordContext.resetToCheckpoint(checkpointId, bytes);
        }
    }

    // ------------------------------------------------------------------------
    //  job status listener that schedules / cancels periodic checkpoints
    // ------------------------------------------------------------------------

    public JobStatusListener createActivatorDeactivator() {
        synchronized (lock) {
            if (shutdown) {
                throw new IllegalArgumentException("Checkpoint coordinator is shut down");
            }

            if (jobStatusListener == null) {
                jobStatusListener = new CheckpointCoordinatorDeActivator(this);
            }

            return jobStatusListener;
        }
    }

    int getNumQueuedRequests() {
        synchronized (lock) {
            return requestDecider.getNumQueuedRequests();
        }
    }

    public void reportCheckpointMetrics(
            long id, ExecutionAttemptID attemptId, CheckpointMetrics metrics) {
        statsTracker.reportIncompleteStats(id, attemptId, metrics);
    }

    public void reportInitializationMetrics(
            ExecutionAttemptID executionAttemptID,
            SubTaskInitializationMetrics initializationMetrics) {
        statsTracker.reportInitializationMetrics(initializationMetrics);
    }

    // ------------------------------------------------------------------------

    final class ScheduledTrigger implements Runnable {

        @Override
        public void run() {
            // 使用锁来保证并发安全性
            synchronized (lock) {
                // 检查当前正在运行的触发器是否和当前对象相同
                if (currentPeriodicTrigger != this) {
                    // Another periodic trigger has been scheduled but this one
                    // has not been force cancelled yet.
                    return;
                }

                // 获取当前检查点的间隔时间
                long checkpointInterval = getCurrentCheckpointInterval();
                // 如果检查点间隔时间不是禁用值
                if (checkpointInterval
                        != CheckpointCoordinatorConfiguration.DISABLED_CHECKPOINT_INTERVAL) {
                    // 则更新下一个检查点触发的相对时间，加上当前检查点间隔时间
                    nextCheckpointTriggeringRelativeTime += checkpointInterval;
                    // 计算距离下一个检查点触发时间的延迟，取该延迟和0之间的最大值
                    // 以确保不会立即触发（如果当前时间已经超过了下次触发的计划时间）
                    // 然后使用timer重新调度当前触发器
                    currentPeriodicTriggerFuture =
                            timer.schedule(
                                    this,
                                    Math.max(
                                            0,
                                            nextCheckpointTriggeringRelativeTime
                                                    - clock.relativeTimeMillis()),
                                    TimeUnit.MILLISECONDS);
                } else {
                    // 如果检查点间隔时间是禁用值
                    // 则设置下一个检查点触发的相对时间为Long.MAX_VALUE（表示永远不再触发）
                    // 并将当前周期性触发器及其Future对象置为null，表示不再使用它们
                    nextCheckpointTriggeringRelativeTime = Long.MAX_VALUE;
                    currentPeriodicTrigger = null;
                    currentPeriodicTriggerFuture = null;
                }
            }

            try {
                // 尝试触发检查点
                triggerCheckpoint(checkpointProperties, null, true);
            } catch (Exception e) {
                //打印错误信息
                LOG.error("Exception while triggering checkpoint for job {}.", job, e);
            }
        }
    }

    /**
     * Discards the given state object asynchronously belonging to the given job, execution attempt
     * id and checkpoint id.
     *
     * @param jobId identifying the job to which the state object belongs
     * @param executionAttemptID identifying the task to which the state object belongs
     * @param checkpointId of the state object
     * @param subtaskState to discard asynchronously
     */
    private void discardSubtaskState(
            final JobID jobId,
            final ExecutionAttemptID executionAttemptID,
            final long checkpointId,
            final TaskStateSnapshot subtaskState) {

        if (subtaskState != null) {
            executor.execute(
                    new Runnable() {
                        @Override
                        public void run() {

                            try {
                                subtaskState.discardState();
                            } catch (Throwable t2) {
                                LOG.warn(
                                        "Could not properly discard state object of checkpoint {} "
                                                + "belonging to task {} of job {}.",
                                        checkpointId,
                                        executionAttemptID,
                                        jobId,
                                        t2);
                            }
                        }
                    });
        }
    }

    private void abortPendingCheckpoint(
            PendingCheckpoint pendingCheckpoint, CheckpointException exception) {

        abortPendingCheckpoint(pendingCheckpoint, exception, null);
    }

    private void abortPendingCheckpoint(
            PendingCheckpoint pendingCheckpoint,
            CheckpointException exception,
            @Nullable final ExecutionAttemptID executionAttemptID) {

        assert (Thread.holdsLock(lock));

        if (!pendingCheckpoint.isDisposed()) {
            try {
                // release resource here
                pendingCheckpoint.abort(
                        exception.getCheckpointFailureReason(),
                        exception.getCause(),
                        checkpointsCleaner,
                        this::scheduleTriggerRequest,
                        executor,
                        statsTracker);

                failureManager.handleCheckpointException(
                        pendingCheckpoint,
                        pendingCheckpoint.getProps(),
                        exception,
                        executionAttemptID,
                        job,
                        getStatsCallback(pendingCheckpoint),
                        statsTracker);
            } finally {
                sendAbortedMessages(
                        pendingCheckpoint.getCheckpointPlan().getTasksToCommitTo(),
                        pendingCheckpoint.getCheckpointID(),
                        pendingCheckpoint.getCheckpointTimestamp());
                pendingCheckpoints.remove(pendingCheckpoint.getCheckpointID());
                if (exception
                        .getCheckpointFailureReason()
                        .equals(CheckpointFailureReason.CHECKPOINT_EXPIRED)) {
                    rememberRecentExpiredCheckpointId(pendingCheckpoint.getCheckpointID());
                }
                scheduleTriggerRequest();
            }
        }
    }

    private void preCheckGlobalState(boolean isPeriodic) throws CheckpointException {
        // abort if the coordinator has been shutdown in the meantime
        if (shutdown) {
            throw new CheckpointException(CheckpointFailureReason.CHECKPOINT_COORDINATOR_SHUTDOWN);
        }

        // Don't allow periodic checkpoint if scheduling has been disabled
        if (isPeriodic && !periodicScheduling) {
            throw new CheckpointException(CheckpointFailureReason.PERIODIC_SCHEDULER_SHUTDOWN);
        }
    }

    private void abortPendingAndQueuedCheckpoints(CheckpointException exception) {
        assert (Thread.holdsLock(lock));
        requestDecider.abortAll(exception);
        abortPendingCheckpoints(exception);
    }

    /**
     * The canceller of checkpoint. The checkpoint might be cancelled if it doesn't finish in a
     * configured period.
     */
    class CheckpointCanceller implements Runnable {

        private final PendingCheckpoint pendingCheckpoint;

        private CheckpointCanceller(PendingCheckpoint pendingCheckpoint) {
            this.pendingCheckpoint = checkNotNull(pendingCheckpoint);
        }

        @Override
        public void run() {
            synchronized (lock) {
                // only do the work if the checkpoint is not discarded anyways
                // note that checkpoint completion discards the pending checkpoint object
                if (!pendingCheckpoint.isDisposed()) {
                    LOG.info(
                            "Checkpoint {} of job {} expired before completing.",
                            pendingCheckpoint.getCheckpointID(),
                            job);

                    abortPendingCheckpoint(
                            pendingCheckpoint,
                            new CheckpointException(CheckpointFailureReason.CHECKPOINT_EXPIRED));
                }
            }
        }
    }

    private static CheckpointException getCheckpointException(
            CheckpointFailureReason defaultReason, Throwable throwable) {

        final Optional<IOException> ioExceptionOptional =
                findThrowable(throwable, IOException.class);
        if (ioExceptionOptional.isPresent()) {
            return new CheckpointException(CheckpointFailureReason.IO_EXCEPTION, throwable);
        } else {
            final Optional<CheckpointException> checkpointExceptionOptional =
                    findThrowable(throwable, CheckpointException.class);
            return checkpointExceptionOptional.orElseGet(
                    () -> new CheckpointException(defaultReason, throwable));
        }
    }

    static class CheckpointTriggerRequest {
        final long timestamp;
        final CheckpointProperties props;
        final @Nullable String externalSavepointLocation;
        final boolean isPeriodic;
        private final CompletableFuture<CompletedCheckpoint> onCompletionPromise =
                new CompletableFuture<>();

        CheckpointTriggerRequest(
                CheckpointProperties props,
                @Nullable String externalSavepointLocation,
                boolean isPeriodic) {

            this.timestamp = System.currentTimeMillis();
            this.props = checkNotNull(props);
            this.externalSavepointLocation = externalSavepointLocation;
            this.isPeriodic = isPeriodic;
        }

        CompletableFuture<CompletedCheckpoint> getOnCompletionFuture() {
            return onCompletionPromise;
        }

        public void completeExceptionally(CheckpointException exception) {
            onCompletionPromise.completeExceptionally(exception);
        }

        public boolean isForce() {
            return props.forceCheckpoint();
        }
    }

    private enum OperatorCoordinatorRestoreBehavior {

        /** Coordinators are always restored. If there is no checkpoint, they are restored empty. */
        RESTORE_OR_RESET,

        /** Coordinators are restored if there was a checkpoint. */
        RESTORE_IF_CHECKPOINT_PRESENT,

        /** Coordinators are not restored during this checkpoint restore. */
        SKIP;
    }

    private PendingCheckpointStats trackPendingCheckpointStats(
            long checkpointId,
            CheckpointPlan checkpointPlan,
            CheckpointProperties props,
            long checkpointTimestamp) {
        Map<JobVertexID, Integer> vertices =
                Stream.concat(
                                checkpointPlan.getTasksToWaitFor().stream(),
                                checkpointPlan.getFinishedTasks().stream())
                        .map(Execution::getVertex)
                        .map(ExecutionVertex::getJobVertex)
                        .distinct()
                        .collect(
                                toMap(
                                        ExecutionJobVertex::getJobVertexId,
                                        ExecutionJobVertex::getParallelism));

        PendingCheckpointStats pendingCheckpointStats =
                statsTracker.reportPendingCheckpoint(
                        checkpointId, checkpointTimestamp, props, vertices);

        reportFinishedTasks(pendingCheckpointStats, checkpointPlan.getFinishedTasks());

        return pendingCheckpointStats;
    }

    private void reportFinishedTasks(
            PendingCheckpointStats pendingCheckpointStats, List<Execution> finishedTasks) {
        long now = System.currentTimeMillis();
        finishedTasks.forEach(
                execution ->
                        pendingCheckpointStats.reportSubtaskStats(
                                execution.getVertex().getJobvertexId(),
                                new SubtaskStateStats(execution.getParallelSubtaskIndex(), now)));
    }

    @Nullable
    private PendingCheckpointStats getStatsCallback(PendingCheckpoint pendingCheckpoint) {
        return statsTracker.getPendingCheckpointStats(pendingCheckpoint.getCheckpointID());
    }
}
