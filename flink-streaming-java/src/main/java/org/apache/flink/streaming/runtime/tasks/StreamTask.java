/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.AutoCloseableRegistry;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.security.FlinkSecurityManager;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.InitializationStatus;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.checkpoint.SubTaskInitializationMetricsBuilder;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.SequentialChannelStateReader;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.writer.MultipleRecordWriters;
import org.apache.flink.runtime.io.network.api.writer.NonRecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterDelegate;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.SingleRecordWriter;
import org.apache.flink.runtime.io.network.partition.ChannelStateHolder;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointableTask;
import org.apache.flink.runtime.jobgraph.tasks.CoordinatedTask;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStorageLoader;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.taskmanager.AsyncExceptionHandler;
import org.apache.flink.runtime.taskmanager.AsynchronousException;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.graph.NonChainedOutput;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManagerImpl;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.io.StreamInputProcessor;
import org.apache.flink.streaming.runtime.io.checkpointing.BarrierAlignmentUtil;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointBarrierHandler;
import org.apache.flink.streaming.runtime.partitioner.ConfigurableStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.mailbox.GaugePeriodTimer;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction.Suspension;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorFactory;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxMetricsController;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor;
import org.apache.flink.streaming.runtime.tasks.mailbox.PeriodTimer;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.TaskManagerOptions.BUFFER_DEBLOAT_PERIOD;
import static org.apache.flink.runtime.metrics.MetricNames.GATE_RESTORE_DURATION;
import static org.apache.flink.runtime.metrics.MetricNames.INITIALIZE_STATE_DURATION;
import static org.apache.flink.runtime.metrics.MetricNames.MAILBOX_START_DURATION;
import static org.apache.flink.runtime.metrics.MetricNames.READ_OUTPUT_DATA_DURATION;
import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.flink.util.concurrent.FutureUtils.assertNoException;

/**
 * Base class for all streaming tasks. A task is the unit of local processing that is deployed and
 * executed by the TaskManagers. Each task runs one or more {@link StreamOperator}s which form the
 * Task's operator chain. Operators that are chained together execute synchronously in the same
 * thread and hence on the same stream partition. A common case for these chains are successive
 * map/flatmap/filter tasks.
 *
 * <p>The task chain contains one "head" operator and multiple chained operators. The StreamTask is
 * specialized for the type of the head operator: one-input and two-input tasks, as well as for
 * sources, iteration heads and iteration tails.
 *
 * <p>The Task class deals with the setup of the streams read by the head operator, and the streams
 * produced by the operators at the ends of the operator chain. Note that the chain may fork and
 * thus have multiple ends.
 *
 * <p>The life cycle of the task is set up as follows:
 *
 * <pre>{@code
 * -- setInitialState -> provides state of all operators in the chain
 *
 * -- invoke()
 *       |
 *       +----> Create basic utils (config, etc) and load the chain of operators
 *       +----> operators.setup()
 *       +----> task specific init()
 *       +----> initialize-operator-states()
 *       +----> open-operators()
 *       +----> run()
 *       +----> finish-operators()
 *       +----> close-operators()
 *       +----> common cleanup
 *       +----> task specific cleanup()
 * }</pre>
 *
 * <p>The {@code StreamTask} has a lock object called {@code lock}. All calls to methods on a {@code
 * StreamOperator} must be synchronized on this lock object to ensure that no methods are called
 * concurrently.
 *
 * @param <OUT>
 * @param <OP>
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 所有流处理任务的基类。任务（Task）是局部处理的单元，由TaskManagers部署并执行。
 * 每个任务运行一个或多个StreamOperator，这些StreamOperator形成了任务的算子链（Operator Chain）。
 * 被链式连接起来的算子在相同的线程（因此也在相同的流分区）中同步执行。这些链的一个常见例子是连续的map/flatmap/filter任务。
 * 总结：基类定义了流处理中任务的基本结构和行为，而任务则负责运行一个或多个算子，这些算子通常会被链式连接在一起以同步处理数据。
 * 1.StreamOperator形成任务算子链。
 * 2.在一起的算子链会在同一个线程中执行。
*/
@Internal
public abstract class StreamTask<OUT, OP extends StreamOperator<OUT>>
        implements TaskInvokable,
                CheckpointableTask,
                CoordinatedTask,
                AsyncExceptionHandler,
                ContainingTaskDetails {

    /** The thread group that holds all trigger timer threads. */
    /** 保存所有触发器计时器线程的线程组 */
    public static final ThreadGroup TRIGGER_THREAD_GROUP = new ThreadGroup("Triggers");

    /** The logger used by the StreamTask and its subclasses. */
    protected static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

    // ------------------------------------------------------------------------

    /**
     * All actions outside of the task {@link #mailboxProcessor mailbox} (i.e. performed by another
     * thread) must be executed through this executor to ensure that we don't have concurrent method
     * calls that void consistent checkpoints. The execution will always be performed in the task
     * thread.
     *
     * <p>CheckpointLock is superseded by {@link MailboxExecutor}, with {@link
     * StreamTaskActionExecutor.SynchronizedStreamTaskActionExecutor
     * SynchronizedStreamTaskActionExecutor} to provide lock to {@link SourceStreamTask}.
     */
    private final StreamTaskActionExecutor actionExecutor;

    /** The input processor. Initialized in {@link #init()} method. */
    /** 输入处理器 在init方法初始化 */
    @Nullable protected StreamInputProcessor inputProcessor;

    /** the main operator that consumes the input streams of this task. */
    /** 消耗该任务的输入流的主运算符。  */
    protected OP mainOperator;

    /** The chain of operators executed by this task. */
    /** 此任务执行的Operator Chain */
    /**
     * StreamTask构建过程中会合并Operator,形成OperatorChain。
     * OperatorChain中所有的Operator都在同一个Task
     */
    protected OperatorChain<OUT, OP> operatorChain;

    /** The configuration of this streaming task. */
    /** 流任务的配置 */
    protected final StreamConfig configuration;

    /** Our state backend. We use this to create a keyed state backend. */
    /** 声明一个状态后端变量 */
    protected final StateBackend stateBackend;

    /** Our checkpoint storage. We use this to create checkpoint streams. */
    /** 检查点存储。用来创建检查点流 */
    protected final CheckpointStorage checkpointStorage;
    /** 协调子任务（即 Task 和 StreamTask）中与检查点（checkpointing）相关的工作 */
    private final SubtaskCheckpointCoordinator subtaskCheckpointCoordinator;

    /**
     * The internal {@link TimerService} used to define the current processing time (default =
     * {@code System.currentTimeMillis()}) and register timers for tasks to be executed in the
     * future.
     */
    /** Task执行过程中用到的定时器服务 */
    protected final TimerService timerService;

    /**
     * In contrast to {@link #timerService} we should not register any user timers here. It should
     * be used only for system level timers.
     */
    protected final TimerService systemTimerService;

    /** The currently active background materialization threads. */
    private final CloseableRegistry cancelables = new CloseableRegistry();

    private final AutoCloseableRegistry resourceCloser;
    /** 封装异步异常处理 */
    private final StreamTaskAsyncExceptionHandler asyncExceptionHandler;

    /**
     * Flag to mark the task "in operation", in which case check needs to be initialized to true, so
     * that early cancel() before invoke() behaves correctly.
     */
    private volatile boolean isRunning;

    /** Flag to mark the task at restoring duration in {@link #restore()}. */
    private volatile boolean isRestoring;

    /** Flag to mark this task as canceled. */
    private volatile boolean canceled;

    /**
     * Flag to mark this task as failing, i.e. if an exception has occurred inside {@link
     * #invoke()}.
     */
    private volatile boolean failing;

    /** Flags indicating the finished method of all the operators are called. */
    private boolean finishedOperators;

    private boolean closedOperators;

    /** Thread pool for async snapshot workers. */
    /** 异步快照工作线程的线程池 */
    private final ExecutorService asyncOperationsThreadPool;
    /** 写出数据的Writer */
    protected final RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> recordWriter;
    /** 封装了逻辑上基于mailbox的执行模型,让Task执行过程中变为单线程。*/
    protected final MailboxProcessor mailboxProcessor;
    /** mailbox执行器 */
    final MailboxExecutor mainMailboxExecutor;

    /** TODO it might be replaced by the global IO executor on TaskManager level future. */
    private final ExecutorService channelIOExecutor;

    // ========================================================
    //  Final  checkpoint / savepoint
    // ========================================================
    private Long syncSavepoint = null;
    private Long finalCheckpointMinId = null;
    private final CompletableFuture<Void> finalCheckpointCompleted = new CompletableFuture<>();

    private long latestReportCheckpointId = -1;

    private long latestAsyncCheckpointStartDelayNanos;

    private volatile boolean endOfDataReceived = false;

    private final long bufferDebloatPeriod;

    private final Environment environment;

    private final Object shouldInterruptOnCancelLock = new Object();

    @GuardedBy("shouldInterruptOnCancelLock")
    private boolean shouldInterruptOnCancel = true;

    @Nullable private final AvailabilityProvider changelogWriterAvailabilityProvider;

    private long initializeStateEndTs;

    // ------------------------------------------------------------------------

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * @param env The task environment for this task.
     */
    protected StreamTask(Environment env) throws Exception {
        this(env, null);
    }

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * @param env The task environment for this task.
     * @param timerService Optionally, a specific timer service to use.
     */
    protected StreamTask(Environment env, @Nullable TimerService timerService) throws Exception {
        this(env, timerService, FatalExitExceptionHandler.INSTANCE);
    }

    protected StreamTask(
            Environment environment,
            @Nullable TimerService timerService,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler)
            throws Exception {
        this(
                environment,
                timerService,
                uncaughtExceptionHandler,
                StreamTaskActionExecutor.IMMEDIATE);
    }

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * <p>This constructor accepts a special {@link TimerService}. By default (and if null is passes
     * for the timer service) a {@link SystemProcessingTimeService DefaultTimerService} will be
     * used.
     *
     * @param environment The task environment for this task.
     * @param timerService Optionally, a specific timer service to use.
     * @param uncaughtExceptionHandler to handle uncaught exceptions in the async operations thread
     *     pool
     * @param actionExecutor a mean to wrap all actions performed by this task thread. Currently,
     *     only SynchronizedActionExecutor can be used to preserve locking semantics.
     */
    protected StreamTask(
            Environment environment,
            @Nullable TimerService timerService,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
            StreamTaskActionExecutor actionExecutor)
            throws Exception {
        this(
                environment,
                timerService,
                uncaughtExceptionHandler,
                actionExecutor,
                new TaskMailboxImpl(Thread.currentThread()));
    }

    protected StreamTask(
            Environment environment,
            @Nullable TimerService timerService,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
            StreamTaskActionExecutor actionExecutor,
            TaskMailbox mailbox)
            throws Exception {
        // The registration of all closeable resources. The order of registration is important.
        /** 所有可关闭资源的注册。里面存放是一个Map结构，用来存放可关闭或者清理的资源 */
        resourceCloser = new AutoCloseableRegistry();
        try {
            // 保存传入的环境配置对象。
            this.environment = environment;
            // 根据环境配置创建流配置对象。
            this.configuration = new StreamConfig(environment.getTaskConfiguration());

            // Initialize mailbox metrics
            /** 初始化 mailbox 相关的指标控制器 */
            MailboxMetricsController mailboxMetricsControl =
                    new MailboxMetricsController(
                            environment.getMetricGroup().getIOMetricGroup().getMailboxLatency(),
                            environment
                                    .getMetricGroup()
                                    .getIOMetricGroup()
                                    .getNumMailsProcessedCounter());
            // 注册 mailbox 的大小提供者到指标组
            environment
                    .getMetricGroup()
                    .getIOMetricGroup()
                    .registerMailboxSizeSupplier(() -> mailbox.size());
            //
            /**
             * 核心点1
             * 创建一个 MailboxProcessor 对象来处理邮箱中的消息
             * 1.使用当前对象的 processInput 方法来处理输入
             * 2.传入的任务邮箱
             * 3.传入的动作执行器
             * 4.传入的 mailbox 指标控制器
             * 核心点
             */
            this.mailboxProcessor =
                    new MailboxProcessor(
                            this::processInput, mailbox, actionExecutor, mailboxMetricsControl);

            // Should be closed last.
            //将 mailboxProcessor 注册为可关闭资源，并确保它在最后关闭。
            resourceCloser.registerCloseable(mailboxProcessor);
            // 创建一个单线程的 Executor 来处理通道 I/O
            this.channelIOExecutor =
                    Executors.newSingleThreadExecutor(
                            new ExecutorThreadFactory("channel-state-unspilling"));
            // 将 channelIOExecutor 的关闭方法注册为可关闭资源
            resourceCloser.registerCloseable(channelIOExecutor::shutdown);
           //核心点2.根据配置和环境创建 recordWriter 代理
            this.recordWriter = createRecordWriterDelegate(configuration, environment);
            // Release the output resources. this method should never fail.
            // 注册一个方法用于释放输出资源，
            resourceCloser.registerCloseable(this::releaseOutputResources);
            // If the operators won't be closed explicitly, register it to a hard close.
            // 如果操作器没有明确关闭，则将其注册为强制关闭。
            resourceCloser.registerCloseable(this::closeAllOperators);
            // 注册内部清理方法为可关闭资源
            resourceCloser.registerCloseable(this::cleanUpInternal);
            // 检查 actionExecutor 是否为空，如果不为空则赋值。
            this.actionExecutor = Preconditions.checkNotNull(actionExecutor);
            // 获取 mailboxProcessor 的主执行器
            this.mainMailboxExecutor = mailboxProcessor.getMainMailboxExecutor();
            // 创建一个 StreamTask 的异步异常处理器
            this.asyncExceptionHandler = new StreamTaskAsyncExceptionHandler(environment);

            // With maxConcurrentCheckpoints + 1 we more or less adhere to the
            // maxConcurrentCheckpoints configuration, but allow for a small leeway with allowing
            // for simultaneous N ongoing concurrent checkpoints and for example clean up of one
            // aborted one.
            // 创建一个异步操作线程池，该线程池的大小由maxConcurrentCheckpoints配置决定，
            // 但会多出一个线程作为额外的缓冲，以便能够处理一些同时进行的N个并发检查点，
            // 例如用于清理一个已中止的检查点。
            this.asyncOperationsThreadPool =
                    new ThreadPoolExecutor(
                            0,// 核心线程数设置为0，意味着线程池在创建后不会立即启动线程
                            configuration.getMaxConcurrentCheckpoints() + 1,// 最大线程数设置为maxConcurrentCheckpoints + 1
                            60L,// 线程空闲60秒后将终止
                            TimeUnit.SECONDS,
                            new LinkedBlockingQueue<>(),// 使用LinkedBlockingQueue作为工作队列
                            new ExecutorThreadFactory("AsyncOperations", uncaughtExceptionHandler)); // 使用自定义的线程工厂和未捕获异常处理器

            // Register all asynchronous checkpoint threads.
            // 注册所有异步检查点线程。当需要关闭资源时，这些线程将被优雅地关闭。
            resourceCloser.registerCloseable(this::shutdownAsyncThreads);
            // 假设cancelables是包含其他可取消资源的集合
            resourceCloser.registerCloseable(cancelables);
            // 设置主邮箱执行器和异步操作线程池
            environment.setMainMailboxExecutor(mainMailboxExecutor);
            environment.setAsyncOperationsThreadPool(asyncOperationsThreadPool);
            // 创建状态后端
            this.stateBackend = createStateBackend();
            // 根据状态后端创建检查点存储
            this.checkpointStorage = createCheckpointStorage(stateBackend);
            // 检查是否有状态变更日志存储，并获取其可用性提供器
            this.changelogWriterAvailabilityProvider =
                    environment.getTaskStateManager().getStateChangelogStorage() == null
                            ? null
                            : environment
                                    .getTaskStateManager()
                                    .getStateChangelogStorage()
                                    .getAvailabilityProvider();
            // 创建检查点存储访问对象
            CheckpointStorageAccess checkpointStorageAccess =
                    checkpointStorage.createCheckpointStorage(getEnvironment().getJobID());
            // 应用文件合并快照管理器到检查点存储访问对象
            checkpointStorageAccess =
                    applyFileMergingCheckpoint(
                            checkpointStorageAccess,
                            environment.getTaskStateManager().getFileMergingSnapshotManager());
            // 将检查点存储访问对象设置到环境中
            environment.setCheckpointStorageAccess(checkpointStorageAccess);

            // if the clock is not already set, then assign a default TimeServiceProvider
            // 如果计时服务尚未设置，则为其分配一个默认的TimeServiceProvider
            if (timerService == null) {
                this.timerService = createTimerService("Time Trigger for " + getName());
            } else {
                this.timerService = timerService;
            }
            // 创建一个系统计时服务
            this.systemTimerService = createTimerService("System Time Trigger for " + getName());
            // 创建一个子任务检查点协调器，用于协调和管理检查点
            // SubtaskCheckpointCoordinatorImpl 是检查点协调器的实现类
            this.subtaskCheckpointCoordinator =
                    new SubtaskCheckpointCoordinatorImpl(
                            checkpointStorage,// 检查点存储的接口
                            checkpointStorageAccess,// 检查点存储访问的接口
                            getName(),// 获取当前任务的名称
                            actionExecutor,// 执行操作的执行器
                            getAsyncOperationsThreadPool(),// 获取异步操作的线程池
                            environment,// 运行时环境
                            this,// 当前任务实例的引用
                            configuration.isUnalignedCheckpointsEnabled(),// 是否启用非对齐检查点
                            configuration
                                    .getConfiguration()
                                    .get(
                                            ExecutionCheckpointingOptions
                                                    .ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH),// 是否在任务完成后启用检查点
                            this::prepareInputSnapshot,// 输入快照的回调函数
                            configuration.getMaxConcurrentCheckpoints(),// 最大并发检查点的数量
                            BarrierAlignmentUtil.createRegisterTimerCallback(
                                    mainMailboxExecutor, systemTimerService),// 创建用于注册时间回调的函数
                            configuration.getMaxSubtasksPerChannelStateFile()); // 每个通道状态文件的最大子任务数
            // 将检查点协调器注册为可关闭资源，确保在任务结束时能够正确关闭
            resourceCloser.registerCloseable(subtaskCheckpointCoordinator::close);

            // Register to stop all timers and threads. Should be closed first.
            // 注册关闭所有计时器和线程的方法，应该首先关闭
            // 停止时间服务
            resourceCloser.registerCloseable(this::tryShutdownTimerService);
            // 将通道状态写入器注入到通道中
            injectChannelStateWriterIntoChannels();
            // 启用busy时间度量
            environment.getMetricGroup().getIOMetricGroup().setEnableBusyTime(true);
            // 获取任务管理器的配置
            Configuration taskManagerConf = environment.getTaskManagerInfo().getConfiguration();
            // 缓冲区膨胀的周期（转换为毫秒）
            this.bufferDebloatPeriod = taskManagerConf.get(BUFFER_DEBLOAT_PERIOD).toMillis();
            // 设置邮件箱指标的延迟测量
            mailboxMetricsControl.setupLatencyMeasurement(systemTimerService, mainMailboxExecutor);
        } catch (Exception ex) {
            // 如果在创建过程中出现异常，尝试关闭已注册的资源
            try {
                resourceCloser.close();
            } catch (Throwable throwable) {
                ex.addSuppressed(throwable);
            }
            throw ex;
        }
    }

    private CheckpointStorageAccess applyFileMergingCheckpoint(
            CheckpointStorageAccess checkpointStorageAccess,
            FileMergingSnapshotManager fileMergingSnapshotManager) {
        // TODO (FLINK-32440): enable FileMergingCheckpoint by configuration
        return checkpointStorageAccess;
    }

    private TimerService createTimerService(String timerThreadName) {
        ThreadFactory timerThreadFactory =
                new DispatcherThreadFactory(TRIGGER_THREAD_GROUP, timerThreadName);
        return new SystemProcessingTimeService(this::handleTimerException, timerThreadFactory);
    }

    private void injectChannelStateWriterIntoChannels() {
        final Environment env = getEnvironment();
        final ChannelStateWriter channelStateWriter =
                subtaskCheckpointCoordinator.getChannelStateWriter();
        for (final InputGate gate : env.getAllInputGates()) {
            gate.setChannelStateWriter(channelStateWriter);
        }
        for (ResultPartitionWriter writer : env.getAllWriters()) {
            if (writer instanceof ChannelStateHolder) {
                ((ChannelStateHolder) writer).setChannelStateWriter(channelStateWriter);
            }
        }
    }

    private CompletableFuture<Void> prepareInputSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
        if (inputProcessor == null) {
            return FutureUtils.completedVoidFuture();
        }
        return inputProcessor.prepareSnapshot(channelStateWriter, checkpointId);
    }

    SubtaskCheckpointCoordinator getCheckpointCoordinator() {
        return subtaskCheckpointCoordinator;
    }

    // ------------------------------------------------------------------------
    //  Life cycle methods for specific implementations
    // ------------------------------------------------------------------------

    protected abstract void init() throws Exception;

    protected void cancelTask() throws Exception {}

    /**
     * This method implements the default action of the task (e.g. processing one event from the
     * input). Implementations should (in general) be non-blocking.
     *
     * @param controller controller object for collaborative interaction between the action and the
     *     stream task.
     * @throws Exception on any problems in the action.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 任务的默认操作（例如，处理来自输入的一个事件）
    */
    protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
        DataInputStatus status = inputProcessor.processInput();
        switch (status) {
            case MORE_AVAILABLE:
                if (taskIsAvailable()) {
                    return;
                }
                break;
            case NOTHING_AVAILABLE:
                break;
            case END_OF_RECOVERY:
                throw new IllegalStateException("We should not receive this event here.");
            case STOPPED:
                endData(StopMode.NO_DRAIN);
                return;
            case END_OF_DATA:
                endData(StopMode.DRAIN);
                notifyEndOfData();
                return;
            case END_OF_INPUT:
                // Suspend the mailbox processor, it would be resumed in afterInvoke and finished
                // after all records processed by the downstream tasks. We also suspend the default
                // actions to avoid repeat executing the empty default operation (namely process
                // records).
                controller.suspendDefaultAction();
                mailboxProcessor.suspend();
                return;
        }

        TaskIOMetricGroup ioMetrics = getEnvironment().getMetricGroup().getIOMetricGroup();
        PeriodTimer timer;
        CompletableFuture<?> resumeFuture;
        if (!recordWriter.isAvailable()) {
            timer = new GaugePeriodTimer(ioMetrics.getSoftBackPressuredTimePerSecond());
            resumeFuture = recordWriter.getAvailableFuture();
        } else if (!inputProcessor.isAvailable()) {
            timer = new GaugePeriodTimer(ioMetrics.getIdleTimeMsPerSecond());
            resumeFuture = inputProcessor.getAvailableFuture();
        } else if (changelogWriterAvailabilityProvider != null
                && !changelogWriterAvailabilityProvider.isAvailable()) {
            // waiting for changelog availability is reported as busy
            timer = new GaugePeriodTimer(ioMetrics.getChangelogBusyTimeMsPerSecond());
            resumeFuture = changelogWriterAvailabilityProvider.getAvailableFuture();
        } else {
            // data availability has changed in the meantime; retry immediately
            return;
        }
        assertNoException(
                resumeFuture.thenRun(
                        new ResumeWrapper(controller.suspendDefaultAction(timer), timer)));
    }

    protected void endData(StopMode mode) throws Exception {

        if (mode == StopMode.DRAIN) {
            advanceToEndOfEventTime();
        }
        // finish all operators in a chain effect way
        operatorChain.finishOperators(actionExecutor, mode);
        this.finishedOperators = true;

        for (ResultPartitionWriter partitionWriter : getEnvironment().getAllWriters()) {
            partitionWriter.notifyEndOfData(mode);
        }

        this.endOfDataReceived = true;
    }

    protected void notifyEndOfData() {
        environment.getTaskManagerActions().notifyEndOfData(environment.getExecutionId());
    }

    protected void setSynchronousSavepoint(long checkpointId) {
        checkState(
                syncSavepoint == null || syncSavepoint == checkpointId,
                "at most one stop-with-savepoint checkpoint at a time is allowed");
        syncSavepoint = checkpointId;
    }

    @VisibleForTesting
    OptionalLong getSynchronousSavepointId() {
        if (syncSavepoint != null) {
            return OptionalLong.of(syncSavepoint);
        } else {
            return OptionalLong.empty();
        }
    }

    private boolean isCurrentSyncSavepoint(long checkpointId) {
        return syncSavepoint != null && syncSavepoint == checkpointId;
    }

    /**
     * Emits the {@link org.apache.flink.streaming.api.watermark.Watermark#MAX_WATERMARK
     * MAX_WATERMARK} so that all registered timers are fired.
     *
     * <p>This is used by the source task when the job is {@code TERMINATED}. In the case, we want
     * all the timers registered throughout the pipeline to fire and the related state (e.g.
     * windows) to be flushed.
     *
     * <p>For tasks other than the source task, this method does nothing.
     */
    protected void advanceToEndOfEventTime() throws Exception {}

    // ------------------------------------------------------------------------
    //  Core work methods of the Stream Task
    // ------------------------------------------------------------------------

    public StreamTaskStateInitializer createStreamTaskStateInitializer(
            SubTaskInitializationMetricsBuilder initializationMetrics) {
        InternalTimeServiceManager.Provider timerServiceProvider =
                configuration.getTimerServiceProvider(getUserCodeClassLoader());
        return new StreamTaskStateInitializerImpl(
                getEnvironment(),
                stateBackend,
                initializationMetrics,
                TtlTimeProvider.DEFAULT,
                timerServiceProvider != null
                        ? timerServiceProvider
                        : InternalTimeServiceManagerImpl::create,
                () -> canceled);
    }

    protected Counter setupNumRecordsInCounter(StreamOperator streamOperator) {
        try {
            return streamOperator.getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();
        } catch (Exception e) {
            LOG.warn("An exception occurred during the metrics setup.", e);
            return new SimpleCounter();
        }
    }

    @Override
    public final void restore() throws Exception {
        restoreInternal();
    }

    void restoreInternal() throws Exception {
        // 如果任务正在运行，则拒绝重新恢复尝试
        if (isRunning) {
            LOG.debug("Re-restore attempt rejected.");
            return;
        }
        // 标记任务正在恢复中
        isRestoring = true;
        // 标记关闭的操作符为未关闭状态
        closedOperators = false;
        // 标记任务初始化开始
        getEnvironment().getMetricGroup().getIOMetricGroup().markTaskInitializationStarted();
        LOG.debug("Initializing {}.", getName());
        // 初始化度量指标构建器
         SubTaskInitializationMetricsBuilder initializationMetrics =
        new SubTaskInitializationMetricsBuilder(
                SystemClock.getInstance().absoluteTimeMillis());
        try {
            //初始化OperatorChain
            operatorChain =
                    getEnvironment().getTaskStateManager().isTaskDeployedAsFinished()
                            ? new FinishedOperatorChain<>(this, recordWriter)
                            : new RegularOperatorChain<>(this, recordWriter);
            // 获取主操作符
            mainOperator = operatorChain.getMainOperator();
            // 获取并设置恢复的检查点ID（如果存在）
            getEnvironment()
                    .getTaskStateManager()
                    .getRestoreCheckpointId()
                    .ifPresent(restoreId -> latestReportCheckpointId = restoreId);

            // task specific initialization
            // 特定任务的初始化操作
            init();
            // 清除初始配置，避免重复加载状态等
            configuration.clearInitialConfigs();

            // save the work of reloading state, etc, if the task is already canceled
            // 确保任务未被取消
            ensureNotCanceled();

            // -------- Invoke --------
            LOG.debug("Invoking {}", getName());

            // we need to make sure that any triggers scheduled in open() cannot be
            // executed before all operators are opened
            // 调用restoreStateAndGates方法，并确保在所有操作符打开之前，open()方法中调度的任何触发器都不会被执行
            CompletableFuture<Void> allGatesRecoveredFuture =
                    actionExecutor.call(() -> restoreStateAndGates(initializationMetrics));

            // Run mailbox until all gates will be recovered.
            // 运行邮箱处理器，直到所有门恢复
            mailboxProcessor.runMailboxLoop();
            // 添加门恢复的时间度量指标
            initializationMetrics.addDurationMetric(
                    GATE_RESTORE_DURATION,
                    SystemClock.getInstance().absoluteTimeMillis() - initializeStateEndTs);
            // 确保任务未被取消
            ensureNotCanceled();
            // 检查所有门是否已恢复完成
            checkState(
                    allGatesRecoveredFuture.isDone(),
                    "Mailbox loop interrupted before recovery was finished.");

            // we recovered all the gates, we can close the channel IO executor as it is no longer
            // needed
            // 如果已经恢复了Gate，可以关闭不再需要的通道IO执行器
            channelIOExecutor.shutdown();
            // 标记任务正在运行
            isRunning = true;
            // 标记任务恢复完成
            isRestoring = false;
            // 设置初始化状态为已完成
            initializationMetrics.setStatus(InitializationStatus.COMPLETED);
        } finally {
            /**
             * 汇报状态，初始化后要向JobMaster汇报Checkpoint等相关状态
             */
            environment
                    .getTaskStateManager()
                    .reportInitializationMetrics(initializationMetrics.build());
        }
    }

    private CompletableFuture<Void> restoreStateAndGates(
            SubTaskInitializationMetricsBuilder initializationMetrics) throws Exception {

        long mailboxStartTs = SystemClock.getInstance().absoluteTimeMillis();
        initializationMetrics.addDurationMetric(
                MAILBOX_START_DURATION,
                mailboxStartTs - initializationMetrics.getInitializationStartTs());

        SequentialChannelStateReader reader =
                getEnvironment().getTaskStateManager().getSequentialChannelStateReader();
        reader.readOutputData(
                getEnvironment().getAllWriters(), !configuration.isGraphContainingLoops());

        long readOutputDataTs = SystemClock.getInstance().absoluteTimeMillis();
        initializationMetrics.addDurationMetric(
                READ_OUTPUT_DATA_DURATION, readOutputDataTs - mailboxStartTs);

        operatorChain.initializeStateAndOpenOperators(
                createStreamTaskStateInitializer(initializationMetrics));

        initializeStateEndTs = SystemClock.getInstance().absoluteTimeMillis();
        initializationMetrics.addDurationMetric(
                INITIALIZE_STATE_DURATION, initializeStateEndTs - readOutputDataTs);
        IndexedInputGate[] inputGates = getEnvironment().getAllInputGates();

        channelIOExecutor.execute(
                () -> {
                    try {
                        reader.readInputData(inputGates);
                    } catch (Exception e) {
                        asyncExceptionHandler.handleAsyncException(
                                "Unable to read channel state", e);
                    }
                });

        // We wait for all input channel state to recover before we go into RUNNING state, and thus
        // start checkpointing. If we implement incremental checkpointing of input channel state
        // we must make sure it supports CheckpointType#FULL_CHECKPOINT
        List<CompletableFuture<?>> recoveredFutures = new ArrayList<>(inputGates.length);
        for (InputGate inputGate : inputGates) {
            recoveredFutures.add(inputGate.getStateConsumedFuture());

            inputGate
                    .getStateConsumedFuture()
                    .thenRun(
                            () ->
                                    mainMailboxExecutor.execute(
                                            inputGate::requestPartitions,
                                            "Input gate request partitions"));
        }

        return CompletableFuture.allOf(recoveredFutures.toArray(new CompletableFuture[0]))
                .thenRun(mailboxProcessor::suspend);
    }

    private void ensureNotCanceled() {
        if (canceled) {
            throw new CancelTaskException();
        }
    }

    @Override
    public final void invoke() throws Exception {
        // Allow invoking method 'invoke' without having to call 'restore' before it.
        // 如果当前不是运行状态，则会在调用 invoke 方法时触发恢复过程。
        if (!isRunning) {
            // 如果当前不是运行状态，则记录日志信息，并调用内部恢复方法
            LOG.debug("Restoring during invoke will be called.");
            // 调用内部恢复方法，可能是为了准备资源或恢复状态
            restoreInternal();
        }

        // final check to exit early before starting to run
        // 在开始运行之前进行最后的检查，确保任务没有被取消
        // 如果任务被取消，则不会继续执行后续的代码
        ensureNotCanceled();

        // 调度缓冲区清理任务（可能是为了清理不再需要的资源或数据）
        scheduleBufferDebloater();

        // let the task do its work
        // 标记任务开始，这通常用于监控和日志记录，以便跟踪任务的执行时间和性能
        getEnvironment().getMetricGroup().getIOMetricGroup().markTaskStart();
        // 执行任务的主要逻辑，可能是通过邮件箱循环处理消息或执行其他任务
        runMailboxLoop();

        // if this left the run() method cleanly despite the fact that this was canceled,
        // make sure the "clean shutdown" is not attempted
        // 再次确保任务没有被取消
        ensureNotCanceled();
       // 调用 afterInvoke 方法进行后续处理
        afterInvoke();
    }

    private void scheduleBufferDebloater() {
        // See https://issues.apache.org/jira/browse/FLINK-23560
        // If there are no input gates, there is no point of calculating the throughput and running
        // the debloater. At the same time, for SourceStreamTask using legacy sources and checkpoint
        // lock, enqueuing even a single mailbox action can cause performance regression. This is
        // especially visible in batch, with disabled checkpointing and no processing time timers.
        if (getEnvironment().getAllInputGates().length == 0
                || !environment
                        .getTaskManagerInfo()
                        .getConfiguration()
                        .get(TaskManagerOptions.BUFFER_DEBLOAT_ENABLED)) {
            return;
        }
        systemTimerService.registerTimer(
                systemTimerService.getCurrentProcessingTime() + bufferDebloatPeriod,
                timestamp ->
                        mainMailboxExecutor.execute(
                                () -> {
                                    debloat();
                                    scheduleBufferDebloater();
                                },
                                "Buffer size recalculation"));
    }

    @VisibleForTesting
    void debloat() {
        for (IndexedInputGate inputGate : environment.getAllInputGates()) {
            inputGate.triggerDebloating();
        }
    }

    @VisibleForTesting
    public boolean runMailboxStep() throws Exception {
        return mailboxProcessor.runMailboxStep();
    }

    @VisibleForTesting
    public boolean isMailboxLoopRunning() {
        return mailboxProcessor.isMailboxLoopRunning();
    }

    public void runMailboxLoop() throws Exception {
        mailboxProcessor.runMailboxLoop();
    }

    protected void afterInvoke() throws Exception {
        LOG.debug("Finished task {}", getName());
        getCompletionFuture().exceptionally(unused -> null).join();

        Set<CompletableFuture<Void>> terminationConditions = new HashSet<>();
        // If checkpoints are enabled, waits for all the records get processed by the downstream
        // tasks. During this process, this task could coordinate with its downstream tasks to
        // continue perform checkpoints.
        if (endOfDataReceived && areCheckpointsWithFinishedTasksEnabled()) {
            LOG.debug("Waiting for all the records processed by the downstream tasks.");

            for (ResultPartitionWriter partitionWriter : getEnvironment().getAllWriters()) {
                terminationConditions.add(partitionWriter.getAllDataProcessedFuture());
            }

            terminationConditions.add(finalCheckpointCompleted);
        }

        if (syncSavepoint != null) {
            terminationConditions.add(finalCheckpointCompleted);
        }

        FutureUtils.waitForAll(terminationConditions)
                .thenRun(mailboxProcessor::allActionsCompleted);

        // Resumes the mailbox processor. The mailbox processor would be completed
        // after all records are processed by the downstream tasks.
        mailboxProcessor.runMailboxLoop();

        // make sure no further checkpoint and notification actions happen.
        // at the same time, this makes sure that during any "regular" exit where still
        actionExecutor.runThrowing(
                () -> {
                    // make sure no new timers can come
                    timerService.quiesce().get();
                    systemTimerService.quiesce().get();

                    // let mailbox execution reject all new letters from this point
                    mailboxProcessor.prepareClose();
                });

        // processes the remaining mails; no new mails can be enqueued
        mailboxProcessor.drain();

        // Set isRunning to false after all the mails are drained so that
        // the queued checkpoint requirements could be triggered normally.
        actionExecutor.runThrowing(
                () -> {
                    // only set the StreamTask to not running after all operators have been
                    // finished!
                    // See FLINK-7430
                    isRunning = false;
                });

        LOG.debug("Finished operators for task {}", getName());

        // make sure all buffered data is flushed
        operatorChain.flushOutputs();

        if (areCheckpointsWithFinishedTasksEnabled()) {
            // No new checkpoints could be triggered since mailbox has been drained.
            subtaskCheckpointCoordinator.waitForPendingCheckpoints();
            LOG.debug("All pending checkpoints are finished");
        }

        disableInterruptOnCancel();

        // make an attempt to dispose the operators such that failures in the dispose call
        // still let the computation fail
        closeAllOperators();
    }

    private boolean areCheckpointsWithFinishedTasksEnabled() {
        return configuration
                        .getConfiguration()
                        .get(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH)
                && configuration.isCheckpointingEnabled();
    }

    @Override
    public final void cleanUp(Throwable throwable) throws Exception {
        LOG.debug(
                "Cleanup StreamTask (operators closed: {}, cancelled: {})",
                closedOperators,
                canceled);

        failing = !canceled && throwable != null;

        Exception cancelException = null;
        if (throwable != null) {
            try {
                cancelTask();
            } catch (Throwable t) {
                cancelException = t instanceof Exception ? (Exception) t : new Exception(t);
            }
        }

        disableInterruptOnCancel();

        // note: This `.join()` is already uninterruptible, so it doesn't matter if we have already
        // disabled the interruptions or not.
        getCompletionFuture().exceptionally(unused -> null).join();
        // clean up everything we initialized
        isRunning = false;

        // clear any previously issued interrupt for a more graceful shutdown
        Thread.interrupted();

        try {
            resourceCloser.close();
        } catch (Throwable t) {
            Exception e = t instanceof Exception ? (Exception) t : new Exception(t);
            throw firstOrSuppressed(e, cancelException);
        }
    }

    protected void cleanUpInternal() throws Exception {
        if (inputProcessor != null) {
            inputProcessor.close();
        }
    }

    protected CompletableFuture<Void> getCompletionFuture() {
        return FutureUtils.completedVoidFuture();
    }

    @Override
    public final void cancel() throws Exception {
        isRunning = false;
        canceled = true;

        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        // the "cancel task" call must come first, but the cancelables must be
        // closed no matter what
        try {
            cancelTask();
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
            getCompletionFuture()
                    .whenComplete(
                            (unusedResult, unusedError) -> {
                                // WARN: the method is called from the task thread but the callback
                                // can be invoked from a different thread
                                mailboxProcessor.allActionsCompleted();
                                try {
                                    subtaskCheckpointCoordinator.cancel();
                                    cancelables.close();
                                } catch (IOException e) {
                                    throw new CompletionException(e);
                                }
                            });
        }
    }

    public MailboxExecutorFactory getMailboxExecutorFactory() {
        return this.mailboxProcessor::getMailboxExecutor;
    }

    private boolean taskIsAvailable() {
        return recordWriter.isAvailable()
                && (changelogWriterAvailabilityProvider == null
                        || changelogWriterAvailabilityProvider.isAvailable());
    }

    public CanEmitBatchOfRecordsChecker getCanEmitBatchOfRecords() {
        return () -> !this.mailboxProcessor.hasMail() && taskIsAvailable();
    }

    public final boolean isRunning() {
        return isRunning;
    }

    public final boolean isCanceled() {
        return canceled;
    }

    public final boolean isFailing() {
        return failing;
    }

    private void shutdownAsyncThreads() throws Exception {
        if (!asyncOperationsThreadPool.isShutdown()) {
            asyncOperationsThreadPool.shutdownNow();
        }
    }

    private void releaseOutputResources() throws Exception {
        if (operatorChain != null) {
            // beware: without synchronization, #performCheckpoint() may run in
            //         parallel and this call is not thread-safe
            actionExecutor.run(() -> operatorChain.close());
        } else {
            // failed to allocate operatorChain, clean up record writers
            recordWriter.close();
        }
    }

    /** Closes all the operators if not closed before. */
    private void closeAllOperators() throws Exception {
        if (operatorChain != null && !closedOperators) {
            closedOperators = true;
            operatorChain.closeAllOperators();
        }
    }

    /**
     * The finalize method shuts down the timer. This is a fail-safe shutdown, in case the original
     * shutdown method was never called.
     *
     * <p>This should not be relied upon! It will cause shutdown to happen much later than if manual
     * shutdown is attempted, and cause threads to linger for longer than needed.
     */
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (!timerService.isTerminated()) {
            LOG.info("Timer service is shutting down.");
            timerService.shutdownService();
        }

        if (!systemTimerService.isTerminated()) {
            LOG.info("System timer service is shutting down.");
            systemTimerService.shutdownService();
        }

        cancelables.close();
    }

    boolean isSerializingTimestamps() {
        TimeCharacteristic tc = configuration.getTimeCharacteristic();
        return tc == TimeCharacteristic.EventTime | tc == TimeCharacteristic.IngestionTime;
    }

    // ------------------------------------------------------------------------
    //  Access to properties and utilities
    // ------------------------------------------------------------------------

    /**
     * Gets the name of the task, in the form "taskname (2/5)".
     *
     * @return The name of the task.
     */
    public final String getName() {
        return getEnvironment().getTaskInfo().getTaskNameWithSubtasks();
    }

    /**
     * Gets the name of the task, appended with the subtask indicator and execution id.
     *
     * @return The name of the task, with subtask indicator and execution id.
     */
    String getTaskNameWithSubtaskAndId() {
        return getEnvironment().getTaskInfo().getTaskNameWithSubtasks()
                + " ("
                + getEnvironment().getExecutionId()
                + ')';
    }

    public CheckpointStorageWorkerView getCheckpointStorage() {
        return subtaskCheckpointCoordinator.getCheckpointStorage();
    }

    public StreamConfig getConfiguration() {
        return configuration;
    }

    RecordWriterOutput<?>[] getStreamOutputs() {
        return operatorChain.getStreamOutputs();
    }

    // ------------------------------------------------------------------------
    //  Checkpoint and Restore
    // ------------------------------------------------------------------------

    @Override
    public CompletableFuture<Boolean> triggerCheckpointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {
        checkForcedFullSnapshotSupport(checkpointOptions);

        CompletableFuture<Boolean> result = new CompletableFuture<>();
        mainMailboxExecutor.execute(
                () -> {
                    try {
                        boolean noUnfinishedInputGates =
                                Arrays.stream(getEnvironment().getAllInputGates())
                                        .allMatch(InputGate::isFinished);

                        if (noUnfinishedInputGates) {
                            result.complete(
                                    triggerCheckpointAsyncInMailbox(
                                            checkpointMetaData, checkpointOptions));
                        } else {
                            result.complete(
                                    triggerUnfinishedChannelsCheckpoint(
                                            checkpointMetaData, checkpointOptions));
                        }
                    } catch (Exception ex) {
                        // Report the failure both via the Future result but also to the mailbox
                        result.completeExceptionally(ex);
                        throw ex;
                    }
                },
                "checkpoint %s with %s",
                checkpointMetaData,
                checkpointOptions);
        return result;
    }

    private boolean triggerCheckpointAsyncInMailbox(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions)
            throws Exception {
        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        try {
            latestAsyncCheckpointStartDelayNanos =
                    1_000_000
                            * Math.max(
                                    0,
                                    System.currentTimeMillis() - checkpointMetaData.getTimestamp());

            // No alignment if we inject a checkpoint
            CheckpointMetricsBuilder checkpointMetrics =
                    new CheckpointMetricsBuilder()
                            .setAlignmentDurationNanos(0L)
                            .setBytesProcessedDuringAlignment(0L)
                            .setCheckpointStartDelayNanos(latestAsyncCheckpointStartDelayNanos);

            subtaskCheckpointCoordinator.initInputsCheckpoint(
                    checkpointMetaData.getCheckpointId(), checkpointOptions);

            boolean success =
                    performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);
            if (!success) {
                declineCheckpoint(checkpointMetaData.getCheckpointId());
            }
            return success;
        } catch (Exception e) {
            // propagate exceptions only if the task is still in "running" state
            if (isRunning) {
                throw new Exception(
                        "Could not perform checkpoint "
                                + checkpointMetaData.getCheckpointId()
                                + " for operator "
                                + getName()
                                + '.',
                        e);
            } else {
                LOG.debug(
                        "Could not perform checkpoint {} for operator {} while the "
                                + "invokable was not in state running.",
                        checkpointMetaData.getCheckpointId(),
                        getName(),
                        e);
                return false;
            }
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
        }
    }

    private boolean triggerUnfinishedChannelsCheckpoint(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions)
            throws Exception {
        Optional<CheckpointBarrierHandler> checkpointBarrierHandler = getCheckpointBarrierHandler();
        checkState(
                checkpointBarrierHandler.isPresent(),
                "CheckpointBarrier should exist for tasks with network inputs.");

        CheckpointBarrier barrier =
                new CheckpointBarrier(
                        checkpointMetaData.getCheckpointId(),
                        checkpointMetaData.getTimestamp(),
                        checkpointOptions);

        for (IndexedInputGate inputGate : getEnvironment().getAllInputGates()) {
            if (!inputGate.isFinished()) {
                for (InputChannelInfo channelInfo : inputGate.getUnfinishedChannels()) {
                    checkpointBarrierHandler.get().processBarrier(barrier, channelInfo, true);
                }
            }
        }

        return true;
    }

    /**
     * Acquires the optional {@link CheckpointBarrierHandler} associated with this stream task. The
     * {@code CheckpointBarrierHandler} should exist if the task has data inputs and requires to
     * align the barriers.
     */
    protected Optional<CheckpointBarrierHandler> getCheckpointBarrierHandler() {
        return Optional.empty();
    }

    @Override
    public void triggerCheckpointOnBarrier(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointMetricsBuilder checkpointMetrics)
            throws IOException {

        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        try {
            performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);
        } catch (CancelTaskException e) {
            LOG.info(
                    "Operator {} was cancelled while performing checkpoint {}.",
                    getName(),
                    checkpointMetaData.getCheckpointId());
            throw e;
        } catch (Exception e) {
            throw new IOException(
                    "Could not perform checkpoint "
                            + checkpointMetaData.getCheckpointId()
                            + " for operator "
                            + getName()
                            + '.',
                    e);
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
        }
    }

    @Override
    public void abortCheckpointOnBarrier(long checkpointId, CheckpointException cause)
            throws IOException {
        if (isCurrentSyncSavepoint(checkpointId)) {
            throw new FlinkRuntimeException("Stop-with-savepoint failed.");
        }
        subtaskCheckpointCoordinator.abortCheckpointOnBarrier(checkpointId, cause, operatorChain);
    }

    private boolean performCheckpoint(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointMetricsBuilder checkpointMetrics)
            throws Exception {

        final SnapshotType checkpointType = checkpointOptions.getCheckpointType();
        LOG.debug(
                "Starting checkpoint {} {} on task {}",
                checkpointMetaData.getCheckpointId(),
                checkpointType,
                getName());

        if (isRunning) {
            actionExecutor.runThrowing(
                    () -> {
                        if (isSynchronous(checkpointType)) {
                            setSynchronousSavepoint(checkpointMetaData.getCheckpointId());
                        }

                        if (areCheckpointsWithFinishedTasksEnabled()
                                && endOfDataReceived
                                && this.finalCheckpointMinId == null) {
                            this.finalCheckpointMinId = checkpointMetaData.getCheckpointId();
                        }

                        subtaskCheckpointCoordinator.checkpointState(
                                checkpointMetaData,
                                checkpointOptions,
                                checkpointMetrics,
                                operatorChain,
                                finishedOperators,
                                this::isRunning);
                    });

            return true;
        } else {
            actionExecutor.runThrowing(
                    () -> {
                        // we cannot perform our checkpoint - let the downstream operators know that
                        // they
                        // should not wait for any input from this operator

                        // we cannot broadcast the cancellation markers on the 'operator chain',
                        // because it may not
                        // yet be created
                        final CancelCheckpointMarker message =
                                new CancelCheckpointMarker(checkpointMetaData.getCheckpointId());
                        recordWriter.broadcastEvent(message);
                    });

            return false;
        }
    }

    private boolean isSynchronous(SnapshotType checkpointType) {
        return checkpointType.isSavepoint() && ((SavepointType) checkpointType).isSynchronous();
    }

    private void checkForcedFullSnapshotSupport(CheckpointOptions checkpointOptions) {
        if (checkpointOptions.getCheckpointType().equals(CheckpointType.FULL_CHECKPOINT)
                && !stateBackend.supportsNoClaimRestoreMode()) {
            throw new IllegalStateException(
                    String.format(
                            "Configured state backend (%s) does not support enforcing a full"
                                    + " snapshot. If you are restoring in %s mode, please"
                                    + " consider choosing %s restore mode.",
                            stateBackend, RestoreMode.NO_CLAIM, RestoreMode.CLAIM));
        } else if (checkpointOptions.getCheckpointType().isSavepoint()) {
            SavepointType savepointType = (SavepointType) checkpointOptions.getCheckpointType();
            if (!stateBackend.supportsSavepointFormat(savepointType.getFormatType())) {
                throw new IllegalStateException(
                        String.format(
                                "Configured state backend (%s) does not support %s savepoints",
                                stateBackend, savepointType.getFormatType()));
            }
        }
    }

    protected void declineCheckpoint(long checkpointId) {
        getEnvironment()
                .declineCheckpoint(
                        checkpointId,
                        new CheckpointException(
                                "Task Name" + getName(),
                                CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY));
    }

    public final ExecutorService getAsyncOperationsThreadPool() {
        return asyncOperationsThreadPool;
    }

    @Override
    public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
        return notifyCheckpointOperation(
                () -> notifyCheckpointComplete(checkpointId),
                String.format("checkpoint %d complete", checkpointId));
    }

    @Override
    public Future<Void> notifyCheckpointAbortAsync(
            long checkpointId, long latestCompletedCheckpointId) {
        return notifyCheckpointOperation(
                () -> {
                    if (latestCompletedCheckpointId > 0) {
                        notifyCheckpointComplete(latestCompletedCheckpointId);
                    }

                    if (isCurrentSyncSavepoint(checkpointId)) {
                        throw new FlinkRuntimeException("Stop-with-savepoint failed.");
                    }
                    subtaskCheckpointCoordinator.notifyCheckpointAborted(
                            checkpointId, operatorChain, this::isRunning);
                },
                String.format("checkpoint %d aborted", checkpointId));
    }

    @Override
    public Future<Void> notifyCheckpointSubsumedAsync(long checkpointId) {
        return notifyCheckpointOperation(
                () ->
                        subtaskCheckpointCoordinator.notifyCheckpointSubsumed(
                                checkpointId, operatorChain, this::isRunning),
                String.format("checkpoint %d subsumed", checkpointId));
    }

    private Future<Void> notifyCheckpointOperation(
            RunnableWithException runnable, String description) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        mailboxProcessor
                .getMailboxExecutor(TaskMailbox.MAX_PRIORITY)
                .execute(
                        () -> {
                            try {
                                runnable.run();
                            } catch (Exception ex) {
                                result.completeExceptionally(ex);
                                throw ex;
                            }
                            result.complete(null);
                        },
                        description);
        return result;
    }

    private void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.debug("Notify checkpoint {} complete on task {}", checkpointId, getName());

        if (checkpointId <= latestReportCheckpointId) {
            return;
        }

        latestReportCheckpointId = checkpointId;

        subtaskCheckpointCoordinator.notifyCheckpointComplete(
                checkpointId, operatorChain, this::isRunning);
        if (isRunning) {
            if (isCurrentSyncSavepoint(checkpointId)) {
                finalCheckpointCompleted.complete(null);
            } else if (syncSavepoint == null
                    && finalCheckpointMinId != null
                    && checkpointId >= finalCheckpointMinId) {
                finalCheckpointCompleted.complete(null);
            }
        }
    }

    private void tryShutdownTimerService() {
        final long timeoutMs =
                getEnvironment()
                        .getTaskManagerInfo()
                        .getConfiguration()
                        .get(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT_TIMERS);
        tryShutdownTimerService(timeoutMs, timerService);
        tryShutdownTimerService(timeoutMs, systemTimerService);
    }

    private void tryShutdownTimerService(long timeoutMs, TimerService timerService) {
        if (!timerService.isTerminated()) {
            if (!timerService.shutdownServiceUninterruptible(timeoutMs)) {
                LOG.warn(
                        "Timer service shutdown exceeded time limit of {} ms while waiting for pending "
                                + "timers. Will continue with shutdown procedure.",
                        timeoutMs);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Operator Events
    // ------------------------------------------------------------------------

    @Override
    public void dispatchOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> event)
            throws FlinkException {
        try {
            mainMailboxExecutor.execute(
                    () -> operatorChain.dispatchOperatorEvent(operator, event),
                    "dispatch operator event");
        } catch (RejectedExecutionException e) {
            // this happens during shutdown, we can swallow this
        }
    }

    // ------------------------------------------------------------------------
    //  State backend
    // ------------------------------------------------------------------------

    private StateBackend createStateBackend() throws Exception {
        final StateBackend fromApplication =
                configuration.getStateBackend(getUserCodeClassLoader());

        return StateBackendLoader.fromApplicationOrConfigOrDefault(
                fromApplication,
                getJobConfiguration(),
                getEnvironment().getTaskManagerInfo().getConfiguration(),
                getUserCodeClassLoader(),
                LOG);
    }

    private CheckpointStorage createCheckpointStorage(StateBackend backend) throws Exception {
        final CheckpointStorage fromApplication =
                configuration.getCheckpointStorage(getUserCodeClassLoader());
        return CheckpointStorageLoader.load(
                fromApplication,
                backend,
                getJobConfiguration(),
                getEnvironment().getTaskManagerInfo().getConfiguration(),
                getUserCodeClassLoader(),
                LOG);
    }

    /**
     * Returns the {@link TimerService} responsible for telling the current processing time and
     * registering actual timers.
     */
    @VisibleForTesting
    TimerService getTimerService() {
        return timerService;
    }

    @VisibleForTesting
    OP getMainOperator() {
        return this.mainOperator;
    }

    @VisibleForTesting
    StreamTaskActionExecutor getActionExecutor() {
        return actionExecutor;
    }

    public ProcessingTimeServiceFactory getProcessingTimeServiceFactory() {
        return mailboxExecutor ->
                new ProcessingTimeServiceImpl(
                        timerService,
                        callback -> deferCallbackToMailbox(mailboxExecutor, callback));
    }

    /**
     * Handles an exception thrown by another thread (e.g. a TriggerTask), other than the one
     * executing the main task by failing the task entirely.
     *
     * <p>In more detail, it marks task execution failed for an external reason (a reason other than
     * the task code itself throwing an exception). If the task is already in a terminal state (such
     * as FINISHED, CANCELED, FAILED), or if the task is already canceling this does nothing.
     * Otherwise it sets the state to FAILED, and, if the invokable code is running, starts an
     * asynchronous thread that aborts that code.
     *
     * <p>This method never blocks.
     */
    @Override
    public void handleAsyncException(String message, Throwable exception) {
        if (isRestoring || isRunning) {
            // only fail if the task is still in restoring or running
            asyncExceptionHandler.handleAsyncException(message, exception);
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return getName();
    }

    // ------------------------------------------------------------------------

    /** Utility class to encapsulate the handling of asynchronous exceptions. */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 封装异步异常处理
    */
    static class StreamTaskAsyncExceptionHandler implements AsyncExceptionHandler {
        private final Environment environment;

        StreamTaskAsyncExceptionHandler(Environment environment) {
            this.environment = environment;
        }

        @Override
        public void handleAsyncException(String message, Throwable exception) {
            environment.failExternally(new AsynchronousException(message, exception));
        }
    }

    public final CloseableRegistry getCancelables() {
        return cancelables;
    }

    // ------------------------------------------------------------------------
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 象用于序列化StreamRecord的写入操作。
     *
     * @param configuration StreamConfig类型的配置对象，用于配置流的相关参数
     * @param environment Environment类型的环境对象，可能包含运行时环境信息
     * @param <OUT> StreamRecord中泛型参数的类型，表示StreamRecord所承载的数据类型
     * @return 返回一个RecordWriterDelegate对象，该对象包装了SerializationDelegate<StreamRecord<OUT>>类型的RecordWriter
     *
    */
    @VisibleForTesting
    public static <OUT>
            RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>>
                    createRecordWriterDelegate(
                            StreamConfig configuration, Environment environment) {
        // 根据配置和环境信息创建RecordWriter的列表
        List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWrites =
                createRecordWriters(configuration, environment);
        // 如果创建的RecordWriter只有一个
        if (recordWrites.size() == 1) {
            // 则返回一个SingleRecordWriter的实例，该实例包装了唯一的RecordWriter
            return new SingleRecordWriter<>(recordWrites.get(0));
            // 则返回一个NonRecordWriter的实例，表示不进行任何写入操作
        } else if (recordWrites.size() == 0) {
            return new NonRecordWriter<>();
            // 如果创建的RecordWriter有多个
        } else {
            // 则返回一个MultipleRecordWriters的实例，该实例能够处理多个RecordWriter的写入操作
            return new MultipleRecordWriters<>(recordWrites);
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 根据给定的 StreamConfig 和 Environment 创建 RecordWriter 列表，
     * 这些 RecordWriter 用于序列化 StreamRecord 类型的输出。
     * @param configuration StreamConfig 对象，包含流配置信息
     * @param environment 当前执行环境的上下文
     * @param <OUT> StreamRecord 中输出的泛型类型
     * @return 包含 RecordWriter 对象的列表，这些对象用于将 SerializationDelegate<StreamRecord<OUT>> 写入输出
    */
    private static <OUT>
            List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> createRecordWriters(
                    StreamConfig configuration, Environment environment) {
        // 创建一个空的 RecordWriter 列表
        List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWriters =
                new ArrayList<>();
        // 获取按非链式顺序排列的输出列表
        List<NonChainedOutput> outputsInOrder =
                configuration.getVertexNonChainedOutputs(
                        environment.getUserCodeClassLoader().asClassLoader());
        // 遍历每个非链式输出
        int index = 0;
        for (NonChainedOutput streamOutput : outputsInOrder) {
            replaceForwardPartitionerIfConsumerParallelismDoesNotMatch(
                    environment, streamOutput, index);
            // 调用 createRecordWriter 方法，并将返回的 RecordWriter 添加到列表中
            recordWriters.add(
                    createRecordWriter(
                            streamOutput,
                            index++,
                            environment,
                            environment.getTaskInfo().getTaskNameWithSubtasks(),
                            streamOutput.getBufferTimeout()));
        }
        // 返回创建的 RecordWriter 列表
        return recordWriters;
    }

    private static void replaceForwardPartitionerIfConsumerParallelismDoesNotMatch(
            Environment environment, NonChainedOutput streamOutput, int outputIndex) {
        if (streamOutput.getPartitioner() instanceof ForwardPartitioner
                && environment.getWriter(outputIndex).getNumberOfSubpartitions()
                        != environment.getTaskInfo().getNumberOfParallelSubtasks()) {
            LOG.debug(
                    "Replacing forward partitioner with rebalance for {}",
                    environment.getTaskInfo().getTaskNameWithSubtasks());
            streamOutput.setPartitioner(new RebalancePartitioner<>());
        }
    }

    @SuppressWarnings("unchecked")
    private static <OUT> RecordWriter<SerializationDelegate<StreamRecord<OUT>>> createRecordWriter(
            NonChainedOutput streamOutput,
            int outputIndex,
            Environment environment,
            String taskNameWithSubtask,
            long bufferTimeout) {
        // 创建 StreamPartitioner 对象，这里是为了避免多个流边共享同一个流分区器，
        StreamPartitioner<OUT> outputPartitioner = null;

        // Clones the partition to avoid multiple stream edges sharing the same stream partitioner,
        // like the case of https://issues.apache.org/jira/browse/FLINK-14087.
        try {
            // 使用环境提供的用户代码类加载器来克隆分区器
            outputPartitioner =
                    InstantiationUtil.clone(
                            (StreamPartitioner<OUT>) streamOutput.getPartitioner(),
                            environment.getUserCodeClassLoader().asClassLoader());
        } catch (Exception e) {
            // 如果克隆失败，则重新抛出异常
            ExceptionUtils.rethrow(e);
        }

        LOG.debug(
                "Using partitioner {} for output {} of task {}",
                outputPartitioner,
                outputIndex,
                taskNameWithSubtask);
        // 从环境中获取结果分区写入器
        ResultPartitionWriter bufferWriter = environment.getWriter(outputIndex);

        // we initialize the partitioner here with the number of key groups (aka max. parallelism)
        // 如果分区器是可配置的，则使用目标键组数（即最大并行度）进行配置
        // 我们在这里初始化分区器
        if (outputPartitioner instanceof ConfigurableStreamPartitioner) {
            int numKeyGroups = bufferWriter.getNumTargetKeyGroups();
            if (0 < numKeyGroups) {
                ((ConfigurableStreamPartitioner) outputPartitioner).configure(numKeyGroups);
            }
        }
        // 创建一个 RecordWriter 对象，并设置相关的配置
        RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output =
                new RecordWriterBuilder<SerializationDelegate<StreamRecord<OUT>>>()
                        .setChannelSelector(outputPartitioner)  // 设置通道选择器为前面创建的分区器
                        .setTimeout(bufferTimeout)// 设置缓冲区超时时间
                        .setTaskName(taskNameWithSubtask) // 设置任务名称
                        .build(bufferWriter);// 构建 RecordWriter 对象
        // 设置度量组
        output.setMetricGroup(environment.getMetricGroup().getIOMetricGroup());
        // 返回构建好的 RecordWriter 对象
        return output;
    }

    private void handleTimerException(Exception ex) {
        handleAsyncException("Caught exception while processing timer.", new TimerException(ex));
    }

    @VisibleForTesting
    ProcessingTimeCallback deferCallbackToMailbox(
            MailboxExecutor mailboxExecutor, ProcessingTimeCallback callback) {
        return timestamp -> {
            mailboxExecutor.execute(
                    () -> invokeProcessingTimeCallback(callback, timestamp),
                    "Timer callback for %s @ %d",
                    callback,
                    timestamp);
        };
    }

    private void invokeProcessingTimeCallback(ProcessingTimeCallback callback, long timestamp) {
        try {
            callback.onProcessingTime(timestamp);
        } catch (Throwable t) {
            handleAsyncException("Caught exception while processing timer.", new TimerException(t));
        }
    }

    protected long getAsyncCheckpointStartDelayNanos() {
        return latestAsyncCheckpointStartDelayNanos;
    }

    private static class ResumeWrapper implements Runnable {
        private final Suspension suspendedDefaultAction;
        @Nullable private final PeriodTimer timer;

        public ResumeWrapper(Suspension suspendedDefaultAction, @Nullable PeriodTimer timer) {
            this.suspendedDefaultAction = suspendedDefaultAction;
            if (timer != null) {
                timer.markStart();
            }
            this.timer = timer;
        }

        @Override
        public void run() {
            if (timer != null) {
                timer.markEnd();
            }
            suspendedDefaultAction.resume();
        }
    }

    @Override
    public boolean isUsingNonBlockingInput() {
        return true;
    }

    /**
     * While we are outside the user code, we do not want to be interrupted further upon
     * cancellation. The shutdown logic below needs to make sure it does not issue calls that block
     * and stall shutdown. Additionally, the cancellation watch dog will issue a hard-cancel (kill
     * the TaskManager process) as a backup in case some shutdown procedure blocks outside our
     * control.
     */
    private void disableInterruptOnCancel() {
        synchronized (shouldInterruptOnCancelLock) {
            shouldInterruptOnCancel = false;
        }
    }

    @Override
    public void maybeInterruptOnCancel(
            Thread toInterrupt, @Nullable String taskName, @Nullable Long timeout) {
        synchronized (shouldInterruptOnCancelLock) {
            if (shouldInterruptOnCancel) {
                if (taskName != null && timeout != null) {
                    Task.logTaskThreadStackTrace(toInterrupt, taskName, timeout, "interrupting");
                }

                toInterrupt.interrupt();
            }
        }
    }

    @Override
    public final Environment getEnvironment() {
        return environment;
    }

    /** Check whether records can be emitted in batch. */
    @FunctionalInterface
    public interface CanEmitBatchOfRecordsChecker {

        boolean check();
    }
}
