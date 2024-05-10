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

package org.apache.flink.runtime.io.network;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGateID;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateFactory;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.METRIC_GROUP_INPUT;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.METRIC_GROUP_OUTPUT;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.createShuffleIOOwnerMetricGroup;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.registerDebloatingTaskMetrics;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.registerInputMetrics;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.registerOutputMetrics;
import static org.apache.flink.util.ExecutorUtils.gracefulShutdown;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The implementation of {@link ShuffleEnvironment} based on netty network communication, local
 * memory and disk files. The network environment contains the data structures that keep track of
 * all intermediate results and shuffle data exchanges.
 */
public class NettyShuffleEnvironment
        implements ShuffleEnvironment<ResultPartition, SingleInputGate> {

    private static final Logger LOG = LoggerFactory.getLogger(NettyShuffleEnvironment.class);

    private final Object lock = new Object();

    private final ResourceID taskExecutorResourceId;

    private final NettyShuffleEnvironmentConfiguration config;

    private final NetworkBufferPool networkBufferPool;

    private final ConnectionManager connectionManager;

    private final ResultPartitionManager resultPartitionManager;

    private final FileChannelManager fileChannelManager;

    private final Map<InputGateID, Set<SingleInputGate>> inputGatesById;

    private final ResultPartitionFactory resultPartitionFactory;

    private final SingleInputGateFactory singleInputGateFactory;

    private final Executor ioExecutor;

    private final BatchShuffleReadBufferPool batchShuffleReadBufferPool;

    private final ScheduledExecutorService batchShuffleReadIOExecutor;

    private boolean isClosed;

    NettyShuffleEnvironment(
            ResourceID taskExecutorResourceId,
            NettyShuffleEnvironmentConfiguration config,
            NetworkBufferPool networkBufferPool,
            ConnectionManager connectionManager,
            ResultPartitionManager resultPartitionManager,
            FileChannelManager fileChannelManager,
            ResultPartitionFactory resultPartitionFactory,
            SingleInputGateFactory singleInputGateFactory,
            Executor ioExecutor,
            BatchShuffleReadBufferPool batchShuffleReadBufferPool,
            ScheduledExecutorService batchShuffleReadIOExecutor) {
        this.taskExecutorResourceId = taskExecutorResourceId;
        this.config = config;
        this.networkBufferPool = networkBufferPool;
        this.connectionManager = connectionManager;
        this.resultPartitionManager = resultPartitionManager;
        this.inputGatesById = new ConcurrentHashMap<>(10);
        this.fileChannelManager = fileChannelManager;
        this.resultPartitionFactory = resultPartitionFactory;
        this.singleInputGateFactory = singleInputGateFactory;
        this.ioExecutor = ioExecutor;
        this.batchShuffleReadBufferPool = batchShuffleReadBufferPool;
        this.batchShuffleReadIOExecutor = batchShuffleReadIOExecutor;
        this.isClosed = false;
    }

    // --------------------------------------------------------------------------------------------
    //  Properties
    // --------------------------------------------------------------------------------------------

    @VisibleForTesting
    public ResultPartitionManager getResultPartitionManager() {
        return resultPartitionManager;
    }

    @VisibleForTesting
    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    @VisibleForTesting
    public NetworkBufferPool getNetworkBufferPool() {
        return networkBufferPool;
    }

    @VisibleForTesting
    public BatchShuffleReadBufferPool getBatchShuffleReadBufferPool() {
        return batchShuffleReadBufferPool;
    }

    @VisibleForTesting
    public ScheduledExecutorService getBatchShuffleReadIOExecutor() {
        return batchShuffleReadIOExecutor;
    }

    @VisibleForTesting
    public NettyShuffleEnvironmentConfiguration getConfiguration() {
        return config;
    }

    @VisibleForTesting
    public Optional<Collection<SingleInputGate>> getInputGate(InputGateID id) {
        return Optional.ofNullable(inputGatesById.get(id));
    }

    @Override
    public void releasePartitionsLocally(Collection<ResultPartitionID> partitionIds) {
        ioExecutor.execute(
                () -> {
                    for (ResultPartitionID partitionId : partitionIds) {
                        resultPartitionManager.releasePartition(partitionId, null);
                    }
                });
    }

    /**
     * Report unreleased partitions.
     *
     * @return collection of partitions which still occupy some resources locally on this task
     *     executor and have been not released yet.
     */
    @Override
    public Collection<ResultPartitionID> getPartitionsOccupyingLocalResources() {
        return resultPartitionManager.getUnreleasedPartitions();
    }

    // --------------------------------------------------------------------------------------------
    //  Create Output Writers and Input Readers
    // --------------------------------------------------------------------------------------------

    @Override
    public ShuffleIOOwnerContext createShuffleIOOwnerContext(
            String ownerName, ExecutionAttemptID executionAttemptID, MetricGroup parentGroup) {
        MetricGroup nettyGroup = createShuffleIOOwnerMetricGroup(checkNotNull(parentGroup));
        return new ShuffleIOOwnerContext(
                checkNotNull(ownerName),
                checkNotNull(executionAttemptID),
                parentGroup,
                nettyGroup.addGroup(METRIC_GROUP_OUTPUT),
                nettyGroup.addGroup(METRIC_GROUP_INPUT));
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建ResultPartitionWriter 结果分区
     * @param ownerContext 用于提供与Shuffle I/O相关的上下文信息
     * @param resultPartitionDeploymentDescriptors 描述了结果分区的部署详情
     */
    @Override
    public List<ResultPartition> createResultPartitionWriters(
            ShuffleIOOwnerContext ownerContext,
            List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors) {
        /** 使用synchronized关键字对lock对象进行同步，确保在并发环境下此方法的线程安全*/
        synchronized (lock) {
            /** 使用Preconditions工具类检查NettyShuffleEnvironment是否已被关闭   */
            Preconditions.checkState(
                    !isClosed, "The NettyShuffleEnvironment has already been shut down.");
            /** 根据resultPartitionDeploymentDescriptors列表的大小创建ResultPartition数组   */
            ResultPartition[] resultPartitions =
                    new ResultPartition[resultPartitionDeploymentDescriptors.size()];
            /**
             * 遍历resultPartitionDeploymentDescriptors列表，为每个ResultPartitionDeploymentDescriptor
             * 创建一个ResultPartition
             */
            for (int partitionIndex = 0;
                    partitionIndex < resultPartitions.length;
                    partitionIndex++) {
                /** 构建ResultPartition */
                resultPartitions[partitionIndex] =
                        resultPartitionFactory.create(
                                /** 传入ownerContext的ownerName，用于标识结果分区的所有者 */
                                ownerContext.getOwnerName(),
                                /** 传入当前分区的索引  */
                                partitionIndex,
                                /** 传入当前索引对应的ResultPartitionDeploymentDescriptor   */
                                resultPartitionDeploymentDescriptors.get(partitionIndex));
            }
            /** 注册输出指标，根据config是否启用网络详细指标、ownerContext的输出组以及结果分区数组来执行   */
            registerOutputMetrics(
                    config.isNetworkDetailedMetrics(),
                    ownerContext.getOutputGroup(),
                    resultPartitions);
            /** 将ResultPartition数组转换为List并返回 */
            return Arrays.asList(resultPartitions);
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 用于创建生成InputGates，InputGates用于消费结果分区 ResultPartitions。
     * @param ownerContext 传入的ShuffleIO拥有者上下文
     * @param partitionProducerStateProvider 分区生产者状态提供者
     * @param inputGateDeploymentDescriptors 输入门部署描述符列表
     */
    @Override
    public List<SingleInputGate> createInputGates(
            ShuffleIOOwnerContext ownerContext,
            PartitionProducerStateProvider partitionProducerStateProvider,
            List<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors) {
        /** 使用同步块确保线程安全  */
        synchronized (lock) {
            /** 检查NettyShuffleEnvironment是否已关闭 */
            Preconditions.checkState(
                    !isClosed, "The NettyShuffleEnvironment has already been shut down.");
            /** 获取网络输入组的MetricGroup */
            MetricGroup networkInputGroup = ownerContext.getInputGroup();
            /** 创建一个InputChannelMetrics实例，用于监控输入通道的性能指标  */
            InputChannelMetrics inputChannelMetrics =
                    new InputChannelMetrics(networkInputGroup, ownerContext.getParentGroup());
            /** 根据输入门部署描述符的数量初始化SingleInputGate数组  */
            SingleInputGate[] inputGates =
                    new SingleInputGate[inputGateDeploymentDescriptors.size()];
            /** 遍历每个输入门部署描述符，创建对应的SingleInputGate  */
            for (int gateIndex = 0; gateIndex < inputGates.length; gateIndex++) {
                final InputGateDeploymentDescriptor igdd =
                        inputGateDeploymentDescriptors.get(gateIndex);
                /** 使用工厂方法创建SingleInputGate实例 */
                SingleInputGate inputGate =
                        singleInputGateFactory.create(
                                ownerContext,
                                gateIndex,
                                igdd,
                                partitionProducerStateProvider,
                                inputChannelMetrics);
                /** 创建一个InputGateID，用于唯一标识该输入门 */
                InputGateID id =
                        new InputGateID(
                                igdd.getConsumedResultId(), ownerContext.getExecutionAttemptID());
                /** 如果当前id的inputGateSet不存在，则创建一个新的并添加到inputGatesById中 */
                Set<SingleInputGate> inputGateSet =
                        inputGatesById.computeIfAbsent(
                                id, ignored -> ConcurrentHashMap.newKeySet());
                /** 将新创建的inputGate添加到对应的inputGateSet中   */
                inputGateSet.add(inputGate);
                /** 更新inputGatesById中的inputGateSet   */
                inputGatesById.put(id, inputGateSet);
                /** 当inputGate关闭时，从inputGateSet中移除它，如果inputGateSet为空，则从inputGatesById中移除 */
                inputGate
                        .getCloseFuture()
                        .thenRun(
                                () ->
                                        inputGatesById.computeIfPresent(
                                                id,
                                                (key, value) -> {
                                                    /** 移除空的inputGateSet   */
                                                    value.remove(inputGate);
                                                    if (value.isEmpty()) {
                                                        return null;
                                                    }
                                                    /** 返回更新后的inputGateSet */
                                                    return value;
                                                }));
                /** 将创建好的inputGate赋值给数组中的对应位置 */
                inputGates[gateIndex] = inputGate;
            }
            /** 如果开启了Debloat配置，则注册Debloating任务指标   */
            if (config.getDebloatConfiguration().isEnabled()) {
                /** 注册监控指标 */
                registerDebloatingTaskMetrics(inputGates, ownerContext.getParentGroup());
            }
            /** 注册输入指标 */
            registerInputMetrics(config.isNetworkDetailedMetrics(), networkInputGroup, inputGates);
            /** 将SingleInputGate数组转换为List并返回 */
            return Arrays.asList(inputGates);
        }
    }

    /**
     * Registers legacy network metric groups before shuffle service refactoring.
     *
     * <p>Registers legacy metric groups if shuffle service implementation is original default one.
     *
     * @deprecated should be removed in future
     */
    @SuppressWarnings("DeprecatedIsStillUsed")
    @Deprecated
    public void registerLegacyNetworkMetrics(
            MetricGroup metricGroup,
            ResultPartitionWriter[] producedPartitions,
            InputGate[] inputGates) {
        NettyShuffleMetricFactory.registerLegacyNetworkMetrics(
                config.isNetworkDetailedMetrics(), metricGroup, producedPartitions, inputGates);
    }

    @Override
    public boolean updatePartitionInfo(ExecutionAttemptID consumerID, PartitionInfo partitionInfo)
            throws IOException, InterruptedException {
        IntermediateDataSetID intermediateResultPartitionID =
                partitionInfo.getIntermediateDataSetID();
        InputGateID id = new InputGateID(intermediateResultPartitionID, consumerID);
        Set<SingleInputGate> inputGates = inputGatesById.get(id);
        if (inputGates == null || inputGates.isEmpty()) {
            return false;
        }

        ShuffleDescriptor shuffleDescriptor = partitionInfo.getShuffleDescriptor();
        checkArgument(
                shuffleDescriptor instanceof NettyShuffleDescriptor,
                "Tried to update unknown channel with unknown ShuffleDescriptor %s.",
                shuffleDescriptor.getClass().getName());
        for (SingleInputGate inputGate : inputGates) {
            inputGate.updateInputChannel(
                    taskExecutorResourceId, (NettyShuffleDescriptor) shuffleDescriptor);
        }
        return true;
    }

    /*
     * Starts the internal related components for network connection and communication.
     *
     * @return a port to connect to the task executor for shuffle data exchange, -1 if only local connection is possible.
     */
    @Override
    public int start() throws IOException {
        synchronized (lock) {
            Preconditions.checkState(
                    !isClosed, "The NettyShuffleEnvironment has already been shut down.");

            LOG.info("Starting the network environment and its components.");

            try {
                LOG.debug("Starting network connection manager");
                return connectionManager.start();
            } catch (IOException t) {
                throw new IOException("Failed to instantiate network connection manager.", t);
            }
        }
    }

    /** Tries to shut down all network I/O components. */
    @Override
    public void close() {
        synchronized (lock) {
            if (isClosed) {
                return;
            }

            LOG.info("Shutting down the network environment and its components.");

            // terminate all network connections
            try {
                LOG.debug("Shutting down network connection manager");
                connectionManager.shutdown();
            } catch (Throwable t) {
                LOG.warn("Cannot shut down the network connection manager.", t);
            }

            // shutdown all intermediate results
            try {
                LOG.debug("Shutting down intermediate result partition manager");
                resultPartitionManager.shutdown();
            } catch (Throwable t) {
                LOG.warn("Cannot shut down the result partition manager.", t);
            }

            // make sure that the global buffer pool re-acquires all buffers
            try {
                networkBufferPool.destroyAllBufferPools();
            } catch (Throwable t) {
                LOG.warn("Could not destroy all buffer pools.", t);
            }

            // destroy the buffer pool
            try {
                networkBufferPool.destroy();
            } catch (Throwable t) {
                LOG.warn("Network buffer pool did not shut down properly.", t);
            }

            // delete all the temp directories
            try {
                fileChannelManager.close();
            } catch (Throwable t) {
                LOG.warn("Cannot close the file channel manager properly.", t);
            }

            try {
                gracefulShutdown(10, TimeUnit.SECONDS, batchShuffleReadIOExecutor);
            } catch (Throwable t) {
                LOG.warn("Cannot shut down batch shuffle read IO executor properly.", t);
            }

            try {
                batchShuffleReadBufferPool.destroy();
            } catch (Throwable t) {
                LOG.warn("Cannot shut down batch shuffle read buffer pool properly.", t);
            }

            isClosed = true;
        }
    }

    public boolean isClosed() {
        synchronized (lock) {
            return isClosed;
        }
    }
}
