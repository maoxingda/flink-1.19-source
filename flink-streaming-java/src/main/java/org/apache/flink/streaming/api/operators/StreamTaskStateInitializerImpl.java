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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.PrioritizedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.SubTaskInitializationMetricsBuilder;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackendParametersImpl;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.util.OperatorSubtaskDescriptionText;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTaskCancellationContext;
import org.apache.flink.util.CloseableIterable;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.SystemClock;

import org.apache.commons.io.IOUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.flink.runtime.state.StateBackendLoader.loadStateBackendFromKeyedStateHandles;
import static org.apache.flink.runtime.state.StateUtil.unexpectedStateHandleException;

/**
 * This class is the main implementation of a {@link StreamTaskStateInitializer}. This class obtains
 * the state to create {@link StreamOperatorStateContext} objects for stream operators from the
 * {@link TaskStateManager} of the task that runs the stream task and hence the operator.
 *
 * <p>This implementation operates on top a {@link TaskStateManager}, from which it receives
 * everything required to restore state in the backends from checkpoints or savepoints.
 */
public class StreamTaskStateInitializerImpl implements StreamTaskStateInitializer {

    /**
     * The environment of the task. This is required as parameter to construct state backends via
     * their factory.
     */
    private final Environment environment;

    /**
     * The state manager of the tasks provides the information used to restore potential previous
     * state.
     */
    private final TaskStateManager taskStateManager;

    /** This object is the factory for everything related to state backends and checkpointing. */
    private final StateBackend stateBackend;

    private final SubTaskInitializationMetricsBuilder initializationMetrics;
    private final TtlTimeProvider ttlTimeProvider;

    private final InternalTimeServiceManager.Provider timeServiceManagerProvider;

    private final StreamTaskCancellationContext cancellationContext;

    public StreamTaskStateInitializerImpl(Environment environment, StateBackend stateBackend) {

        this(
                environment,
                stateBackend,
                new SubTaskInitializationMetricsBuilder(
                        SystemClock.getInstance().absoluteTimeMillis()),
                TtlTimeProvider.DEFAULT,
                InternalTimeServiceManagerImpl::create,
                StreamTaskCancellationContext.alwaysRunning());
    }

    public StreamTaskStateInitializerImpl(
            Environment environment,
            StateBackend stateBackend,
            SubTaskInitializationMetricsBuilder initializationMetrics,
            TtlTimeProvider ttlTimeProvider,
            InternalTimeServiceManager.Provider timeServiceManagerProvider,
            StreamTaskCancellationContext cancellationContext) {

        this.environment = environment;
        this.taskStateManager = Preconditions.checkNotNull(environment.getTaskStateManager());
        this.stateBackend = Preconditions.checkNotNull(stateBackend);
        this.initializationMetrics = initializationMetrics;
        this.ttlTimeProvider = ttlTimeProvider;
        this.timeServiceManagerProvider = Preconditions.checkNotNull(timeServiceManagerProvider);
        this.cancellationContext = cancellationContext;
    }

    // -----------------------------------------------------------------------------------------------------------------
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 根据给定的参数创建一个 StreamOperatorStateContext 对象。
     *
     * @param operatorID            操作符的唯一标识
     * @param operatorClassName     操作符的类名
     * @param processingTimeService 处理时间服务
     * @param keyContext            键的上下文
     * @param keySerializer         键的序列化器（可以为空）
     * @param streamTaskCloseableRegistry 流任务可关闭资源注册表
     * @param metricGroup           度量组
     * @param managedMemoryFraction 管理的内存比例
     * @param isUsingCustomRawKeyedState 是否使用自定义的原始键状态
     * @return StreamOperatorStateContext 上下文对象
     * @throws Exception 如果在创建过程中发生异常
    */
    @Override
    public StreamOperatorStateContext streamOperatorStateContext(
            @Nonnull OperatorID operatorID,
            @Nonnull String operatorClassName,
            @Nonnull ProcessingTimeService processingTimeService,
            @Nonnull KeyContext keyContext,
            @Nullable TypeSerializer<?> keySerializer,
            @Nonnull CloseableRegistry streamTaskCloseableRegistry,
            @Nonnull MetricGroup metricGroup,
            double managedMemoryFraction,
            boolean isUsingCustomRawKeyedState)
            throws Exception {
        // 获取任务信息
        TaskInfo taskInfo = environment.getTaskInfo();
        // 创建一个OperatorSubtaskDescriptionText对象，用于描述操作符的子任务
        OperatorSubtaskDescriptionText operatorSubtaskDescription =
                new OperatorSubtaskDescriptionText(
                        operatorID,// 操作符的唯一标识
                        operatorClassName,// 操作符的类名
                        taskInfo.getIndexOfThisSubtask(),// 当前子任务的索引
                        taskInfo.getNumberOfParallelSubtasks()); // 并行子任务的数量
        // 将OperatorSubtaskDescriptionText对象转换为字符串，作为操作符的标识符
        final String operatorIdentifierText = operatorSubtaskDescription.toString();
        // 根据操作符的唯一标识，从任务状态管理器中获取该操作符的优先级状态
        final PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates =
                taskStateManager.prioritizedOperatorState(operatorID);
        // CheckpointableKeyedStateBackend类型变量 键状态后端
        CheckpointableKeyedStateBackend<?> keyedStatedBackend = null;
        // OperatorStateBackend类型变量 Operator状态后端
        OperatorStateBackend operatorStateBackend = null;
        //原始键状态的输入流
        CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs = null;
        // 原始操作符状态的输入流
        CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs = null;
        // 时间服务管理
        InternalTimeServiceManager<?> timeServiceManager;
    // 创建一个状态大小统计收集器，用于收集状态对象的大小统计信息
        final StateObject.StateObjectSizeStatsCollector statsCollector =
                StateObject.StateObjectSizeStatsCollector.create();

        try {

            // -------------- Keyed State Backend --------------
            // 创建一个Keyed State Backend实例
            // 这个实例用于管理基于键的状态
            // 参数包括键的序列化器、操作符标识符、优先级操作符子任务状态、
            // 流任务可关闭资源注册表、度量组、管理的内存比例和状态大小统计收集器
            keyedStatedBackend =
                    keyedStatedBackend(
                            keySerializer,
                            operatorIdentifierText,
                            prioritizedOperatorSubtaskStates,
                            streamTaskCloseableRegistry,
                            metricGroup,
                            managedMemoryFraction,
                            statsCollector);

            // -------------- Operator State Backend --------------
            // 创建一个Operator State Backend实例
            // 这个实例用于管理非基于键的状态（即操作符级别的状态）
            // 参数包括操作符标识符、优先级操作符子任务状态、
            // 流任务可关闭资源注册表和状态大小统计收集器
            operatorStateBackend =
                    operatorStateBackend(
                            operatorIdentifierText,
                            prioritizedOperatorSubtaskStates,
                            streamTaskCloseableRegistry,
                            statsCollector);

            // -------------- Raw State Streams --------------
            // 获取并处理原始Keyed State的输入流
            // 通过迭代优先级操作符子任务状态的原始Keyed State列表，
            // 创建一个CloseableIterable对象用于处理输入流
            // 并将这个对象注册到流任务可关闭资源注册表中，以便在需要时关闭
            rawKeyedStateInputs =
                    rawKeyedStateInputs(
                            prioritizedOperatorSubtaskStates
                                    .getPrioritizedRawKeyedState()
                                    .iterator(),
                            statsCollector);
            streamTaskCloseableRegistry.registerCloseable(rawKeyedStateInputs);
            // 获取并处理原始Operator State的输入流
            // 通过迭代优先级操作符子任务状态的原始Operator State列表，
            // 创建一个CloseableIterable对象用于处理输入流
            // 并将这个对象注册到流任务可关闭资源注册表中，以便在需要时关闭
            rawOperatorStateInputs =
                    rawOperatorStateInputs(
                            prioritizedOperatorSubtaskStates
                                    .getPrioritizedRawOperatorState()
                                    .iterator(),
                            statsCollector);
            streamTaskCloseableRegistry.registerCloseable(rawOperatorStateInputs);

            // -------------- Internal Timer Service Manager --------------
            if (keyedStatedBackend != null) {

                // if the operator indicates that it is using custom raw keyed state,
                // then whatever was written in the raw keyed state snapshot was NOT written
                // by the internal timer services (because there is only ever one user of raw keyed
                // state);
                // in this case, timers should not attempt to restore timers from the raw keyed
                // state.
                // 如果Keyed State Backend不为空，表示存在基于键的状态后端
                // 如果操作符表明它正在使用自定义的原始Keyed State，
                // 那么在原始Keyed State快照中写入的数据并非由内部计时器服务所写（因为原始Keyed State只会有一个使用者）；
                // 在这种情况下，计时器不应该尝试从原始Keyed State中恢复计时器。
                final Iterable<KeyGroupStatePartitionStreamProvider> restoredRawKeyedStateTimers =
                        (prioritizedOperatorSubtaskStates.isRestored()
                                        && !isUsingCustomRawKeyedState)
                                ? rawKeyedStateInputs
                                : Collections.emptyList();
                 // 创建一个时间服务管理器实例，用于管理计时器
                // 参数包括Keyed State Backend、用户代码类加载器、键上下文、处理时间服务、
                // 恢复后的原始Keyed State计时器数据以及取消上下文
                timeServiceManager =
                        timeServiceManagerProvider.create(
                                keyedStatedBackend,
                                environment.getUserCodeClassLoader().asClassLoader(),
                                keyContext,
                                processingTimeService,
                                restoredRawKeyedStateTimers,
                                cancellationContext);
            } else {
                // 如果Keyed State Backend为空，则不创建时间服务管理器
                timeServiceManager = null;
            }

            // Add stats for input channel and result partition state
            // 合并输入通道状态和结果分区状态，并收集它们的统计信息
            // 这段代码将输入通道状态和结果分区状态的统计信息合并，并通过statsCollector进行收集
            // 用于监控和报告流处理任务中的状态大小
            Stream.concat(
                            prioritizedOperatorSubtaskStates.getPrioritizedInputChannelState()
                                    .stream(),
                            prioritizedOperatorSubtaskStates.getPrioritizedResultSubpartitionState()
                                    .stream())
                    .filter(Objects::nonNull)
                    .forEach(channelHandle -> channelHandle.collectSizeStats(statsCollector));

            // Report collected stats to metrics
            // 遍历并添加已恢复的状态大小到初始化指标中
            statsCollector
                    .getStats()
                    .forEach(
                            (location, metricValue) ->
                                    initializationMetrics.addDurationMetric(
                                            MetricNames.RESTORED_STATE_SIZE + "." + location,
                                            metricValue));

            // -------------- Preparing return value --------------
            // 创建一个StreamOperatorStateContext的实例，并返回它
            // 传入参数包括：
            // - 恢复的checkpoint ID
            // - 操作符状态后端
            // - 基于键的状态后端
            // - 时间服务管理器
            // - 原始的操作符状态输入
            // - 原始的基于键的状态输入
            return new StreamOperatorStateContextImpl(
                    prioritizedOperatorSubtaskStates.getRestoredCheckpointId(),
                    operatorStateBackend,
                    keyedStatedBackend,
                    timeServiceManager,
                    rawOperatorStateInputs,
                    rawKeyedStateInputs);
        } catch (Exception ex) {
            // 如果在创建StreamOperatorStateContext过程中发生异常，则执行以下清理逻辑

            // cleanup if something went wrong before results got published.
            // 如果在发布结果之前出现错误，执行清理工作
            if (keyedStatedBackend != null) {
                // 尝试从可关闭资源注册表中注销基于键的状态后端
                if (streamTaskCloseableRegistry.unregisterCloseable(keyedStatedBackend)) {
                    // 如果注销成功，则尝试关闭它
                    IOUtils.closeQuietly(keyedStatedBackend);
                }
                // release resource (e.g native resource)
                // 释放资源（例如本地资源）
                keyedStatedBackend.dispose();
            }

            if (operatorStateBackend != null) {
                // 尝试从可关闭资源注册表中注销操作符状态后端
                if (streamTaskCloseableRegistry.unregisterCloseable(operatorStateBackend)) {
                    // 如果注销成功，则尝试关闭它
                    IOUtils.closeQuietly(operatorStateBackend);
                }
                // 释放资源
                operatorStateBackend.dispose();
            }

            // 尝试从可关闭资源注册表中注销原始的基于键的状态输入
            if (streamTaskCloseableRegistry.unregisterCloseable(rawKeyedStateInputs)) {
                IOUtils.closeQuietly(rawKeyedStateInputs);
            }
            // 尝试从可关闭资源注册表中注销原始的操作符状态输入
            if (streamTaskCloseableRegistry.unregisterCloseable(rawOperatorStateInputs)) {
                IOUtils.closeQuietly(rawOperatorStateInputs);
            }
            // 抛出带有原始异常信息的异常
            throw new Exception("Exception while creating StreamOperatorStateContext.", ex);
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建OperatorStateBackend
    */
    protected OperatorStateBackend operatorStateBackend(
            String operatorIdentifierText,
            PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates,
            CloseableRegistry backendCloseableRegistry,
            StateObject.StateObjectSizeStatsCollector statsCollector)
            throws Exception {

        String logDescription = "operator state backend for " + operatorIdentifierText;

        // Now restore processing is included in backend building/constructing process, so we need
        // to make sure
        // each stream constructed in restore could also be closed in case of task cancel, for
        // example the data
        // input stream opened for serDe during restore.
        // 现在恢复处理已经包含在后端构建/构造过程中，因此我们需要确保
        // 在恢复过程中创建的每个流（例如，在恢复过程中为序列化/反序列化打开的数据输入流）
        // 在任务取消时也可以被关闭。
        CloseableRegistry cancelStreamRegistryForRestore = new CloseableRegistry();
        backendCloseableRegistry.registerCloseable(cancelStreamRegistryForRestore);
        // 创建一个后端恢复器，用于创建和恢复操作符状态后端
        BackendRestorerProcedure<OperatorStateBackend, OperatorStateHandle> backendRestorer =
                new BackendRestorerProcedure<>(
                        (stateHandles) ->
                                // 传入一个函数，该函数用于根据给定的状态句柄集合创建操作符状态后端
                                stateBackend.createOperatorStateBackend(
                                        new OperatorStateBackendParametersImpl(
                                                environment,// 环境上下文
                                                operatorIdentifierText,// 操作符标识符
                                                stateHandles, // 状态句柄集合
                                                cancelStreamRegistryForRestore)),// 恢复过程中流关闭的注册中心
                        backendCloseableRegistry,// 后端可关闭资源的注册中心
                        logDescription);//日志描述

        try {
            // 调用恢复器的createAndRestore方法，根据优先级排序的操作符子任务状态恢复并返回操作符状态后端
            return backendRestorer.createAndRestore(
                    prioritizedOperatorSubtaskStates.getPrioritizedManagedOperatorState(),
                    statsCollector);
        } finally {
            // 无论是否发生异常，都尝试从主注册中心中注销并关闭用于恢复过程中流关闭的注册中心
            if (backendCloseableRegistry.unregisterCloseable(cancelStreamRegistryForRestore)) {
                IOUtils.closeQuietly(cancelStreamRegistryForRestore);
            }
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建一个用于键控状态管理的 CheckpointableKeyedStateBackend 实例。
     *
     * @param keySerializer 键的序列化器
     * @param operatorIdentifierText 操作符的标识符文本
     * @param prioritizedOperatorSubtaskStates 操作符子任务状态的优先级
     * @param backendCloseableRegistry 后端可关闭资源的注册中心
     * @param metricGroup 度量组，用于收集和报告各种指标
     * @param managedMemoryFraction 托管内存的比例
     * @param statsCollector 状态对象大小统计收集器
     * @param <K> 键的类型
     * @return CheckpointableKeyedStateBackend 实例，如果 keySerializer 为 null，则返回 null
     * @throws Exception 如果在创建后端时发生任何异常
    */
    protected <K> CheckpointableKeyedStateBackend<K> keyedStatedBackend(
            TypeSerializer<K> keySerializer,
            String operatorIdentifierText,
            PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates,
            CloseableRegistry backendCloseableRegistry,
            MetricGroup metricGroup,
            double managedMemoryFraction,
            StateObject.StateObjectSizeStatsCollector statsCollector)
            throws Exception {
        // 如果键序列化器为空，则返回 null
        if (keySerializer == null) {
            return null;
        }
        // 记录日志的描述信息
        String logDescription = "keyed state backend for " + operatorIdentifierText;
        // 获取任务信息
        TaskInfo taskInfo = environment.getTaskInfo();
        // 计算当前操作符子任务的键组范围
        final KeyGroupRange keyGroupRange =
                KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                        taskInfo.getMaxNumberOfParallelSubtasks(),
                        taskInfo.getNumberOfParallelSubtasks(),
                        taskInfo.getIndexOfThisSubtask());

        // Now restore processing is included in backend building/constructing process, so we need
        // to make sure
        // each stream constructed in restore could also be closed in case of task cancel, for
        // example the data
        // input stream opened for serDe during restore.
        // 现在恢复处理已经包含在后端构建过程中，所以我们需要确保
        // 在恢复过程中构造的每个流（例如，在恢复期间为序列化/反序列化打开的数据输入流）
        // 在任务取消时也可以被关闭
        CloseableRegistry cancelStreamRegistryForRestore = new CloseableRegistry();
        // 将这个可关闭注册中心注册到后端可关闭资源注册中心中
        backendCloseableRegistry.registerCloseable(cancelStreamRegistryForRestore);
        // 创建一个 BackendRestorerProcedure 实例，用于从 KeyedStateHandle 恢复 CheckpointableKeyedStateBackend
        BackendRestorerProcedure<CheckpointableKeyedStateBackend<K>, KeyedStateHandle>
                // 这是一个 Lambda 表达式，定义了如何从给定的 stateHandles 恢复后端
                backendRestorer =
                // 创建一个 KeyedStateBackendParametersImpl 实例，该实例包含了恢复后端所需的所有参数
                new BackendRestorerProcedure<>(
                                (stateHandles) -> {
                                    KeyedStateBackendParametersImpl<K> parameters =
                                            new KeyedStateBackendParametersImpl<>(
                                                    environment, // 环境信息
                                                    environment.getJobID(),// 作业ID
                                                    operatorIdentifierText,// 操作符标识符文本
                                                    keySerializer, // 键的序列化器
                                                    taskInfo.getMaxNumberOfParallelSubtasks(),// 最大并行子任务数
                                                    keyGroupRange,// 键组范围
                                                    environment.getTaskKvStateRegistry(),// 任务KV状态注册表
                                                    ttlTimeProvider,// TTL时间提供者（可能是状态过期时间的提供者）
                                                    metricGroup,// 度量组
                                                    initializationMetrics::addDurationMetric, // 初始化度量指标的添加方法
                                                    stateHandles,// 要恢复的 KeyedStateHandle 列表
                                                    cancelStreamRegistryForRestore,// 取消流注册中心，用于在恢复过程中管理可关闭资源
                                                    managedMemoryFraction);// 托管内存的比例
                                    // 从 stateHandles 加载状态后端，并创建 KeyedStateBackend 实例
                                    return loadStateBackendFromKeyedStateHandles(
                                                    stateBackend,// 状态后端
                                                    environment
                                                            .getUserCodeClassLoader()
                                                            .asClassLoader(), // 用户代码类加载器
                                                    stateHandles)// 要恢复的 KeyedStateHandle 列表
                                            .createKeyedStateBackend(parameters);//创建状态后端
                                },
                                backendCloseableRegistry,// 后端可关闭资源的注册中心，用于管理在恢复过程中创建的可关闭资源
                                logDescription);

        try {
            // 调用backendRestorer对象的createAndRestore方法，传入两个参数：
            // 1. prioritizedOperatorSubtaskStates的getPrioritizedManagedKeyedState()方法返回的优先化管理的键控状态
            // 2. statsCollector，可能是用于收集统计信息的对象
            return backendRestorer.createAndRestore(
                    prioritizedOperatorSubtaskStates.getPrioritizedManagedKeyedState(),
                    statsCollector);
        } finally {
            //关闭注册
            if (backendCloseableRegistry.unregisterCloseable(cancelStreamRegistryForRestore)) {
                IOUtils.closeQuietly(cancelStreamRegistryForRestore);
            }
        }
    }

    protected CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs(
            @Nonnull Iterator<StateObjectCollection<OperatorStateHandle>> restoreStateAlternatives,
            @Nonnull StateObject.StateObjectSizeStatsCollector statsCollector) {

        if (restoreStateAlternatives.hasNext()) {

            Collection<OperatorStateHandle> rawOperatorState = restoreStateAlternatives.next();
            // TODO currently this does not support local state recovery, so we expect there is only
            // one handle.
            Preconditions.checkState(
                    !restoreStateAlternatives.hasNext(),
                    "Local recovery is currently not implemented for raw operator state, but found state alternative.");

            if (rawOperatorState != null) {
                // Report restore size stats
                rawOperatorState.forEach(
                        stateObject -> stateObject.collectSizeStats(statsCollector));

                return new CloseableIterable<StatePartitionStreamProvider>() {

                    final CloseableRegistry closeableRegistry = new CloseableRegistry();

                    @Override
                    public void close() throws IOException {
                        closeableRegistry.close();
                    }

                    @Nonnull
                    @Override
                    public Iterator<StatePartitionStreamProvider> iterator() {
                        return new OperatorStateStreamIterator(
                                DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME,
                                rawOperatorState.iterator(),
                                closeableRegistry);
                    }
                };
            }
        }

        return CloseableIterable.empty();
    }

    protected CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs(
            @Nonnull Iterator<StateObjectCollection<KeyedStateHandle>> restoreStateAlternatives,
            @Nonnull StateObject.StateObjectSizeStatsCollector statsCollector) {

        if (restoreStateAlternatives.hasNext()) {
            Collection<KeyedStateHandle> rawKeyedState = restoreStateAlternatives.next();

            // TODO currently this does not support local state recovery, so we expect there is only
            // one handle.
            Preconditions.checkState(
                    !restoreStateAlternatives.hasNext(),
                    "Local recovery is currently not implemented for raw keyed state, but found state alternative.");

            if (rawKeyedState != null) {
                Collection<KeyGroupsStateHandle> keyGroupsStateHandles = transform(rawKeyedState);
                // Report restore size stats
                keyGroupsStateHandles.forEach(
                        stateObject -> stateObject.collectSizeStats(statsCollector));
                final CloseableRegistry closeableRegistry = new CloseableRegistry();

                return new CloseableIterable<KeyGroupStatePartitionStreamProvider>() {
                    @Override
                    public void close() throws IOException {
                        closeableRegistry.close();
                    }

                    @Override
                    public Iterator<KeyGroupStatePartitionStreamProvider> iterator() {
                        return new KeyGroupStreamIterator(
                                keyGroupsStateHandles.iterator(), closeableRegistry);
                    }
                };
            }
        }

        return CloseableIterable.empty();
    }

    // =================================================================================================================

    private static class KeyGroupStreamIterator
            extends AbstractStateStreamIterator<
                    KeyGroupStatePartitionStreamProvider, KeyGroupsStateHandle> {

        private Iterator<Tuple2<Integer, Long>> currentOffsetsIterator;

        KeyGroupStreamIterator(
                Iterator<KeyGroupsStateHandle> stateHandleIterator,
                CloseableRegistry closableRegistry) {

            super(stateHandleIterator, closableRegistry);
        }

        @Override
        public boolean hasNext() {

            if (null != currentStateHandle && currentOffsetsIterator.hasNext()) {

                return true;
            }

            closeCurrentStream();

            while (stateHandleIterator.hasNext()) {
                currentStateHandle = stateHandleIterator.next();
                if (currentStateHandle.getKeyGroupRange().getNumberOfKeyGroups() > 0) {
                    currentOffsetsIterator = unsetOffsetsSkippingIterator(currentStateHandle);

                    if (currentOffsetsIterator.hasNext()) {
                        return true;
                    }
                }
            }

            return false;
        }

        private static Iterator<Tuple2<Integer, Long>> unsetOffsetsSkippingIterator(
                KeyGroupsStateHandle keyGroupsStateHandle) {
            return StreamSupport.stream(
                            keyGroupsStateHandle.getGroupRangeOffsets().spliterator(), false)
                    .filter(
                            keyGroupIdAndOffset ->
                                    keyGroupIdAndOffset.f1
                                            != KeyedStateCheckpointOutputStream.NO_OFFSET_SET)
                    .iterator();
        }

        @Override
        public KeyGroupStatePartitionStreamProvider next() {

            if (!hasNext()) {

                throw new NoSuchElementException("Iterator exhausted");
            }

            Tuple2<Integer, Long> keyGroupOffset = currentOffsetsIterator.next();
            try {
                if (null == currentStream) {
                    openCurrentStream();
                }

                currentStream.seek(keyGroupOffset.f1);
                return new KeyGroupStatePartitionStreamProvider(currentStream, keyGroupOffset.f0);

            } catch (IOException ioex) {
                return new KeyGroupStatePartitionStreamProvider(ioex, keyGroupOffset.f0);
            }
        }
    }

    private static class OperatorStateStreamIterator
            extends AbstractStateStreamIterator<StatePartitionStreamProvider, OperatorStateHandle> {

        private final String
                stateName; // TODO since we only support a single named state in raw, this could be
        // dropped
        private long[] offsets;
        private int offPos;

        OperatorStateStreamIterator(
                String stateName,
                Iterator<OperatorStateHandle> stateHandleIterator,
                CloseableRegistry closableRegistry) {

            super(stateHandleIterator, closableRegistry);
            this.stateName = Preconditions.checkNotNull(stateName);
        }

        @Override
        public boolean hasNext() {

            if (null != offsets && offPos < offsets.length) {

                return true;
            }

            closeCurrentStream();

            while (stateHandleIterator.hasNext()) {
                currentStateHandle = stateHandleIterator.next();
                OperatorStateHandle.StateMetaInfo metaInfo =
                        currentStateHandle.getStateNameToPartitionOffsets().get(stateName);

                if (null != metaInfo) {
                    long[] metaOffsets = metaInfo.getOffsets();
                    if (null != metaOffsets && metaOffsets.length > 0) {
                        this.offsets = metaOffsets;
                        this.offPos = 0;

                        if (closableRegistry.unregisterCloseable(currentStream)) {
                            IOUtils.closeQuietly(currentStream);
                            currentStream = null;
                        }

                        return true;
                    }
                }
            }

            return false;
        }

        @Override
        public StatePartitionStreamProvider next() {

            if (!hasNext()) {

                throw new NoSuchElementException("Iterator exhausted");
            }

            long offset = offsets[offPos++];

            try {
                if (null == currentStream) {
                    openCurrentStream();
                }

                currentStream.seek(offset);
                return new StatePartitionStreamProvider(currentStream);

            } catch (IOException ioex) {
                return new StatePartitionStreamProvider(ioex);
            }
        }
    }

    private abstract static class AbstractStateStreamIterator<
                    T extends StatePartitionStreamProvider, H extends StreamStateHandle>
            implements Iterator<T> {

        protected final Iterator<H> stateHandleIterator;
        protected final CloseableRegistry closableRegistry;

        protected H currentStateHandle;
        protected FSDataInputStream currentStream;

        AbstractStateStreamIterator(
                Iterator<H> stateHandleIterator, CloseableRegistry closableRegistry) {

            this.stateHandleIterator = Preconditions.checkNotNull(stateHandleIterator);
            this.closableRegistry = Preconditions.checkNotNull(closableRegistry);
        }

        protected void openCurrentStream() throws IOException {

            Preconditions.checkState(currentStream == null);

            FSDataInputStream stream = currentStateHandle.openInputStream();
            closableRegistry.registerCloseable(stream);
            currentStream = stream;
        }

        protected void closeCurrentStream() {
            if (closableRegistry.unregisterCloseable(currentStream)) {
                IOUtils.closeQuietly(currentStream);
            }
            currentStream = null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Read only Iterator");
        }
    }

    private static Collection<KeyGroupsStateHandle> transform(
            Collection<KeyedStateHandle> keyedStateHandles) {

        if (keyedStateHandles == null) {
            return null;
        }

        List<KeyGroupsStateHandle> keyGroupsStateHandles =
                new ArrayList<>(keyedStateHandles.size());

        for (KeyedStateHandle keyedStateHandle : keyedStateHandles) {

            if (keyedStateHandle instanceof KeyGroupsStateHandle) {
                keyGroupsStateHandles.add((KeyGroupsStateHandle) keyedStateHandle);
            } else if (keyedStateHandle != null) {
                throw unexpectedStateHandleException(
                        KeyGroupsStateHandle.class, keyedStateHandle.getClass());
            }
        }

        return keyGroupsStateHandles;
    }

    private static class StreamOperatorStateContextImpl implements StreamOperatorStateContext {

        private final @Nullable Long restoredCheckpointId;

        private final OperatorStateBackend operatorStateBackend;
        private final CheckpointableKeyedStateBackend<?> keyedStateBackend;
        private final InternalTimeServiceManager<?> internalTimeServiceManager;

        private final CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs;
        private final CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs;

        StreamOperatorStateContextImpl(
                @Nullable Long restoredCheckpointId,
                OperatorStateBackend operatorStateBackend,
                CheckpointableKeyedStateBackend<?> keyedStateBackend,
                InternalTimeServiceManager<?> internalTimeServiceManager,
                CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs,
                CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs) {

            this.restoredCheckpointId = restoredCheckpointId;
            this.operatorStateBackend = operatorStateBackend;
            this.keyedStateBackend = keyedStateBackend;
            this.internalTimeServiceManager = internalTimeServiceManager;
            this.rawOperatorStateInputs = rawOperatorStateInputs;
            this.rawKeyedStateInputs = rawKeyedStateInputs;
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return restoredCheckpointId == null
                    ? OptionalLong.empty()
                    : OptionalLong.of(restoredCheckpointId);
        }

        @Override
        public CheckpointableKeyedStateBackend<?> keyedStateBackend() {
            return keyedStateBackend;
        }

        @Override
        public OperatorStateBackend operatorStateBackend() {
            return operatorStateBackend;
        }

        @Override
        public InternalTimeServiceManager<?> internalTimerServiceManager() {
            return internalTimeServiceManager;
        }

        @Override
        public CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs() {
            return rawOperatorStateInputs;
        }

        @Override
        public CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs() {
            return rawKeyedStateInputs;
        }
    }
}
