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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageConfiguration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageInputChannelId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyServiceImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.NettyShuffleUtils;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.apache.flink.runtime.throughput.BufferDebloatConfiguration;
import org.apache.flink.runtime.throughput.BufferDebloater;
import org.apache.flink.runtime.throughput.ThroughputCalculator;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.partition.consumer.InputGateSpecUtils.createGateBuffersSpec;
import static org.apache.flink.runtime.shuffle.ShuffleUtils.applyWithShuffleTypeCheck;

/** Factory for {@link SingleInputGate} to use in {@link NettyShuffleEnvironment}. */
public class SingleInputGateFactory {
    private static final Logger LOG = LoggerFactory.getLogger(SingleInputGateFactory.class);

    @Nonnull protected final ResourceID taskExecutorResourceId;

    protected final int partitionRequestInitialBackoff;

    protected final int partitionRequestMaxBackoff;

    protected final int partitionRequestListenerTimeout;

    @Nonnull protected final ConnectionManager connectionManager;

    @Nonnull protected final ResultPartitionManager partitionManager;

    @Nonnull protected final TaskEventPublisher taskEventPublisher;

    @Nonnull protected final NetworkBufferPool networkBufferPool;

    private final Optional<Integer> maxRequiredBuffersPerGate;

    protected final int configuredNetworkBuffersPerChannel;

    private final int floatingNetworkBuffersPerGate;

    private final boolean batchShuffleCompressionEnabled;

    private final String compressionCodec;

    private final int networkBufferSize;

    private final BufferDebloatConfiguration debloatConfiguration;

    /** The following attributes will be null if tiered storage shuffle is disabled. */
    @Nullable private final TieredStorageConfiguration tieredStorageConfiguration;

    @Nullable private final TieredStorageNettyServiceImpl tieredStorageNettyService;

    public SingleInputGateFactory(
            @Nonnull ResourceID taskExecutorResourceId,
            @Nonnull NettyShuffleEnvironmentConfiguration networkConfig,
            @Nonnull ConnectionManager connectionManager,
            @Nonnull ResultPartitionManager partitionManager,
            @Nonnull TaskEventPublisher taskEventPublisher,
            @Nonnull NetworkBufferPool networkBufferPool,
            @Nullable TieredStorageConfiguration tieredStorageConfiguration,
            @Nullable TieredStorageNettyServiceImpl tieredStorageNettyService) {
        this.taskExecutorResourceId = taskExecutorResourceId;
        this.partitionRequestInitialBackoff = networkConfig.partitionRequestInitialBackoff();
        this.partitionRequestMaxBackoff = networkConfig.partitionRequestMaxBackoff();
        this.partitionRequestListenerTimeout = networkConfig.getPartitionRequestListenerTimeout();
        this.maxRequiredBuffersPerGate = networkConfig.maxRequiredBuffersPerGate();
        this.configuredNetworkBuffersPerChannel =
                NettyShuffleUtils.getNetworkBuffersPerInputChannel(
                        networkConfig.networkBuffersPerChannel());
        this.floatingNetworkBuffersPerGate = networkConfig.floatingNetworkBuffersPerGate();
        this.batchShuffleCompressionEnabled = networkConfig.isBatchShuffleCompressionEnabled();
        this.compressionCodec = networkConfig.getCompressionCodec();
        this.networkBufferSize = networkConfig.networkBufferSize();
        this.connectionManager = connectionManager;
        this.partitionManager = partitionManager;
        this.taskEventPublisher = taskEventPublisher;
        this.networkBufferPool = networkBufferPool;
        this.debloatConfiguration = networkConfig.getDebloatConfiguration();
        this.tieredStorageConfiguration = tieredStorageConfiguration;
        this.tieredStorageNettyService = tieredStorageNettyService;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建一个SingleInputGate的实例
     * @param owner ShuffleIOOwnerContext的实例，通常表示拥有该InputGate的任务上下文
     * @param gateIndex 输入门的索引，用于区分多个输入门
     * @param igdd 描述输入门部署属性的InputGateDeploymentDescriptor实例
     * @param partitionProducerStateProvider 提供分区生产者状态的PartitionProducerStateProvider实例
     * @param metrics 输入通道相关的度量指标
    */
    /** Creates an input gate and all of its input channels. */
    public SingleInputGate create(
            @Nonnull ShuffleIOOwnerContext owner,
            int gateIndex,
            @Nonnull InputGateDeploymentDescriptor igdd,
            @Nonnull PartitionProducerStateProvider partitionProducerStateProvider,
            @Nonnull InputChannelMetrics metrics) {
        // This variable describes whether it is supported to have one input channel consume
        // multiple subpartitions, if multiple subpartitions from the same partition are
        // assigned to this input gate.
        //
        // For now this function is only supported in the new mode of Hybrid Shuffle.
        // 变量描述是否支持一个输入通道消费多个子分区，
        // 如果来自同一分区的多个子分区被分配到这个输入门
        // 目前，此功能仅在Hybrid Shuffle的新模式下受支持
        // 对于现在，这个函数仅在Hybrid Shuffle的新模式下支持
        boolean isSharedInputChannelSupported =
                igdd.getConsumedPartitionType().isHybridResultPartition()
                        && tieredStorageConfiguration != null;
       // 创建GateBuffersSpec的实例，它描述了InputGate所需的缓冲区规格
        GateBuffersSpec gateBuffersSpec =
                createGateBuffersSpec(
                        maxRequiredBuffersPerGate,
                        configuredNetworkBuffersPerChannel,
                        floatingNetworkBuffersPerGate,
                        igdd.getConsumedPartitionType(),
                        calculateNumChannels(
                                igdd.getShuffleDescriptors().length,
                                igdd.getConsumedSubpartitionIndexRange().size(),
                                isSharedInputChannelSupported),
                        tieredStorageConfiguration != null);
        // 创建一个bufferPoolFactory的SupplierWithException实例，
        // BufferPool是缓冲区的池化管理
        SupplierWithException<BufferPool, IOException> bufferPoolFactory =
                createBufferPoolFactory(
                        networkBufferPool,
                        gateBuffersSpec.getRequiredFloatingBuffers(),
                        gateBuffersSpec.getTotalFloatingBuffers());
        // 如果支持压缩并且启用了批处理Shuffle压缩，则创建一个BufferDecompressor实例
        BufferDecompressor bufferDecompressor = null;
        if (igdd.getConsumedPartitionType().supportCompression()
                && batchShuffleCompressionEnabled) {
            bufferDecompressor = new BufferDecompressor(networkBufferSize, compressionCodec);
        }
        // 获取拥有该InputGate的任务的名称
        final String owningTaskName = owner.getOwnerName();
        // 获取输入相关的度量组
        final MetricGroup networkInputGroup = owner.getInputGroup();
        // 创建一个ResultSubpartitionIndexSet实例，它包含了被消费的子分区索引集合
        ResultSubpartitionIndexSet subpartitionIndexSet =
                new ResultSubpartitionIndexSet(igdd.getConsumedSubpartitionIndexRange());
        // 创建一个SingleInputGate实例，并传入相关参数
        SingleInputGate inputGate =
                new SingleInputGate(
                        owningTaskName,
                        gateIndex,
                        igdd.getConsumedResultId(),
                        igdd.getConsumedPartitionType(),
                        calculateNumChannels(
                                igdd.getShuffleDescriptors().length,
                                subpartitionIndexSet.size(),
                                isSharedInputChannelSupported),
                        partitionProducerStateProvider,
                        bufferPoolFactory,
                        bufferDecompressor,
                        networkBufferPool,
                        networkBufferSize,
                        new ThroughputCalculator(SystemClock.getInstance()),
                        maybeCreateBufferDebloater(
                                owningTaskName, gateIndex, networkInputGroup.addGroup(gateIndex)));
        //创建InputChannel
        createInputChannelsAndTieredStorageService(
                owningTaskName,
                igdd,
                inputGate,
                subpartitionIndexSet,
                gateBuffersSpec,
                metrics,
                isSharedInputChannelSupported);
        // 返回创建的SingleInputGate实例
        return inputGate;
    }

    private BufferDebloater maybeCreateBufferDebloater(
            String owningTaskName, int gateIndex, MetricGroup inputGroup) {
        if (debloatConfiguration.isEnabled()) {
            final BufferDebloater bufferDebloater =
                    new BufferDebloater(
                            owningTaskName,
                            gateIndex,
                            debloatConfiguration.getTargetTotalBufferSize().toMillis(),
                            debloatConfiguration.getMaxBufferSize(),
                            debloatConfiguration.getMinBufferSize(),
                            debloatConfiguration.getBufferDebloatThresholdPercentages(),
                            debloatConfiguration.getNumberOfSamples());
            inputGroup.gauge(
                    MetricNames.ESTIMATED_TIME_TO_CONSUME_BUFFERS,
                    () -> bufferDebloater.getLastEstimatedTimeToConsumeBuffers().toMillis());
            inputGroup.gauge(MetricNames.DEBLOATED_BUFFER_SIZE, bufferDebloater::getLastBufferSize);
            return bufferDebloater;
        }

        return null;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * @param owningTaskName  拥有任务名称
     * @param inputGateDeploymentDescriptor inputGate部署描述符
     * @param inputGate inputGate实例对象
     * @param subpartitionIndexSet 结果子分区索引集
     * @param gateBuffersSpec 通道缓冲区配置
     * @param metrics InputChannel通道度量指标
     * @param isSharedInputChannelSupported 是否支持共享输入通道
    */
    private void createInputChannelsAndTieredStorageService(
            String owningTaskName,
            InputGateDeploymentDescriptor inputGateDeploymentDescriptor,
            SingleInputGate inputGate,
            ResultSubpartitionIndexSet subpartitionIndexSet,
            GateBuffersSpec gateBuffersSpec,
            InputChannelMetrics metrics,
            boolean isSharedInputChannelSupported) {
        // 获取shuffle描述符数组
        ShuffleDescriptor[] shuffleDescriptors =
                inputGateDeploymentDescriptor.getShuffleDescriptors();

        // Create the input channels. There is one input channel for each consumed subpartition.
        // 创建输入通道。每个消耗的子分区都有一个输入通道。
        InputChannel[] inputChannels =
                new InputChannel
                        [calculateNumChannels(
                                shuffleDescriptors.length,
                                subpartitionIndexSet.size(),
                                isSharedInputChannelSupported)];
        // 通道统计信息
        ChannelStatistics channelStatistics = new ChannelStatistics();

        int channelIdx = 0;
        // 分层存储消费者配置 用于从分层存储读取缓冲区
        final List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs = new ArrayList<>();
        // 遍历每个shuffle描述符
        for (ShuffleDescriptor descriptor : shuffleDescriptors) {
            // 获取分层存储分区ID
            TieredStoragePartitionId partitionId =
                    TieredStorageIdMappingUtils.convertId(descriptor.getResultPartitionID());
            // 如果支持共享输入通道
            if (isSharedInputChannelSupported) {
                // 创建输入通道
                inputChannels[channelIdx] =
                        createInputChannel(
                                inputGate,
                                channelIdx,
                                gateBuffersSpec.getEffectiveExclusiveBuffersPerChannel(),
                                descriptor,
                                subpartitionIndexSet,
                                channelStatistics,
                                metrics);
                // 如果分层存储配置不为空
                if (tieredStorageConfiguration != null) {
                    // 添加分层存储消费者配置
                    tieredStorageConsumerSpecs.add(
                            new TieredStorageConsumerSpec(
                                    partitionId,
                                    new TieredStorageInputChannelId(channelIdx),
                                    subpartitionIndexSet));
                }
                // 递增通道索引
                channelIdx++;
            } else {
                // 对于每个子分区索引
                for (int subpartitionIndex : subpartitionIndexSet.values()) {
                    // 创建输入通道
                    inputChannels[channelIdx] =
                            createInputChannel(
                                    inputGate,
                                    channelIdx,
                                    gateBuffersSpec.getEffectiveExclusiveBuffersPerChannel(),
                                    descriptor,
                                    new ResultSubpartitionIndexSet(subpartitionIndex),
                                    channelStatistics,
                                    metrics);
                    // 如果分层存储配置不为空
                    if (tieredStorageConfiguration != null) {
                        tieredStorageConsumerSpecs.add(
                                new TieredStorageConsumerSpec(
                                        partitionId,
                                        new TieredStorageInputChannelId(channelIdx),
                                        new ResultSubpartitionIndexSet(subpartitionIndex)));
                    }
                    // 递增通道索引
                    channelIdx++;
                }
            }
        }
        //将创建的InputChannel对对象设置给InputGate设置管道
        inputGate.setInputChannels(inputChannels);
        //如果TieredStorageConsumerClient不为空
        if (tieredStorageConfiguration != null) {
            //创建TieredStorageConsumerClient
            TieredStorageConsumerClient tieredStorageConsumerClient =
                    new TieredStorageConsumerClient(
                            tieredStorageConfiguration.getTierFactories(),
                            tieredStorageConsumerSpecs,
                            tieredStorageNettyService);
            inputGate.setTieredStorageService(
                    tieredStorageConsumerSpecs,
                    tieredStorageConsumerClient,
                    tieredStorageNettyService);
        }

        LOG.debug(
                "{}: Created {} input channels ({}).",
                owningTaskName,
                inputChannels.length,
                channelStatistics);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 根据给定的参数创建一个InputChannel实例。
     *
     * @param inputGate              SingleInputGate对象，表示输入门
     * @param index                  输入通道的索引
     * @param buffersPerChannel      每个输入通道的缓冲区数量
     * @param shuffleDescriptor      ShuffleDescriptor对象，表示shuffle的描述信息
     * @param subpartitionIndexSet   ResultSubpartitionIndexSet对象，表示结果子分区的索引集
     * @param channelStatistics      ChannelStatistics对象，用于统计通道信息
     * @param metrics                InputChannelMetrics对象，用于度量输入通道的性能
     * @return 返回一个InputChannel实例
     */
    private InputChannel createInputChannel(
            SingleInputGate inputGate,
            int index,
            int buffersPerChannel,
            ShuffleDescriptor shuffleDescriptor,
            ResultSubpartitionIndexSet subpartitionIndexSet,
            ChannelStatistics channelStatistics,
            InputChannelMetrics metrics) {
        return applyWithShuffleTypeCheck(
                NettyShuffleDescriptor.class,
                shuffleDescriptor,
                //如果Descriptor是unknownShuffleDescriptor
                unknownShuffleDescriptor -> {
                    channelStatistics.numUnknownChannels++;
                    // 创建一个UnknownInputChannel实例
                    return new UnknownInputChannel(
                            inputGate,
                            index,
                            unknownShuffleDescriptor.getResultPartitionID(),
                            subpartitionIndexSet,
                            partitionManager,
                            taskEventPublisher,
                            connectionManager,
                            partitionRequestInitialBackoff,
                            partitionRequestMaxBackoff,
                            partitionRequestListenerTimeout,
                            buffersPerChannel,
                            metrics);
                },
                //如果Descriptor是nettyShuffleDescriptor
                nettyShuffleDescriptor ->
                        // 创建一个UnknownInputChannel实例
                        createKnownInputChannel(
                                inputGate,
                                index,
                                buffersPerChannel,
                                nettyShuffleDescriptor,
                                subpartitionIndexSet,
                                channelStatistics,
                                metrics));
    }

    private static int calculateNumChannels(
            int numShuffleDescriptors,
            int numSubpartitions,
            boolean isSharedInputChannelSupported) {
        if (isSharedInputChannelSupported) {
            return numShuffleDescriptors;
        } else {
            return MathUtils.checkedDownCast(((long) numShuffleDescriptors) * numSubpartitions);
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 根据给定的参数创建一个已知类型的输入通道。
     *
     * @param inputGate            与输入通道关联的单输入门（SingleInputGate）
     * @param index                输入通道在输入门中的索引
     * @param buffersPerChannel    每个输入通道拥有的缓冲区数量
     * @param inputChannelDescriptor Netty 传输层描述的输入通道信息
     * @param subpartitionIndexSet   结果子分区索引集，表示该输入通道对应哪些子分区
     * @param channelStatistics      通道统计信息，用于记录本地和远程通道的数量
     * @param metrics                输入通道的性能度量指标
     * @return 创建的输入通道实例
     */
    @VisibleForTesting
    protected InputChannel createKnownInputChannel(
            SingleInputGate inputGate,
            int index,
            int buffersPerChannel,
            NettyShuffleDescriptor inputChannelDescriptor,
            ResultSubpartitionIndexSet subpartitionIndexSet,
            ChannelStatistics channelStatistics,
            InputChannelMetrics metrics) {
        // 获取输入通道描述符对应的结果分区ID
        ResultPartitionID partitionId = inputChannelDescriptor.getResultPartitionID();
        // 检查输入通道是否是本地的（即，消费任务是否部署在与分区相同的 TaskManager 上）
        if (inputChannelDescriptor.isLocalTo(taskExecutorResourceId)) {
            // Consuming task is deployed to the same TaskManager as the partition => local
            channelStatistics.numLocalChannels++;
            // 创建一个本地恢复的输入通道实例
            // 它将使用本地连接和相关的参数进行初始化
            return new LocalRecoveredInputChannel(
                    inputGate,
                    index,
                    partitionId,
                    subpartitionIndexSet,
                    partitionManager,
                    taskEventPublisher,
                    partitionRequestInitialBackoff,
                    partitionRequestMaxBackoff,
                    buffersPerChannel,
                    metrics);
        } else {
            // Different instances => remote
            // 如果不是本地的（即，是远程的），则增加远程通道数量的统计
            channelStatistics.numRemoteChannels++;
            // 创建一个远程恢复的输入通道实例
            // 它将使用远程连接ID和相关的参数进行初始化
            return new RemoteRecoveredInputChannel(
                    inputGate,
                    index,
                    partitionId,
                    subpartitionIndexSet,
                    inputChannelDescriptor.getConnectionId(),
                    connectionManager,
                    partitionRequestInitialBackoff,
                    partitionRequestMaxBackoff,
                    partitionRequestListenerTimeout,
                    buffersPerChannel,
                    metrics);
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 调用工厂方法创建LocalBufferPool
    */
    @VisibleForTesting
    static SupplierWithException<BufferPool, IOException> createBufferPoolFactory(
            BufferPoolFactory bufferPoolFactory,
            int minFloatingBuffersPerGate,
            int maxFloatingBuffersPerGate) {
        Pair<Integer, Integer> pair = Pair.of(minFloatingBuffersPerGate, maxFloatingBuffersPerGate);
        return () -> bufferPoolFactory.createBufferPool(pair.getLeft(), pair.getRight());
    }

    /** Statistics of input channels. */
    protected static class ChannelStatistics {
        int numLocalChannels;
        int numRemoteChannels;
        int numUnknownChannels;

        @Override
        public String toString() {
            return String.format(
                    "local: %s, remote: %s, unknown: %s",
                    numLocalChannels, numRemoteChannels, numUnknownChannels);
        }
    }
}
