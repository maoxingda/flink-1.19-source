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
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageConfiguration;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyServiceImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredResultPartitionFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleEnvironmentContext;
import org.apache.flink.runtime.shuffle.ShuffleMasterContext;
import org.apache.flink.runtime.shuffle.ShuffleServiceFactory;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.registerShuffleMetrics;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Netty based shuffle service implementation. */
public class NettyShuffleServiceFactory
        implements ShuffleServiceFactory<NettyShuffleDescriptor, ResultPartition, SingleInputGate> {

    private static final Logger LOG = LoggerFactory.getLogger(NettyShuffleServiceFactory.class);
    private static final String DIR_NAME_PREFIX = "netty-shuffle";

    @Override
    public NettyShuffleMaster createShuffleMaster(ShuffleMasterContext shuffleMasterContext) {
        return new NettyShuffleMaster(shuffleMasterContext.getConfiguration());
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 构建NettyShuffleEnvironment
    */
    @Override
    public NettyShuffleEnvironment createShuffleEnvironment(
            ShuffleEnvironmentContext shuffleEnvironmentContext) {
        checkNotNull(shuffleEnvironmentContext);
        //获取NetWorkConfig相关的网络配置getNetworkMemorySize
        NettyShuffleEnvironmentConfiguration networkConfig =
                NettyShuffleEnvironmentConfiguration.fromConfiguration(
                        shuffleEnvironmentContext.getConfiguration(),
                        shuffleEnvironmentContext.getNetworkMemorySize(),
                        shuffleEnvironmentContext.isLocalCommunicationOnly(),
                        shuffleEnvironmentContext.getHostAddress());
        //构建NettyShuffleEnvironment
        return createNettyShuffleEnvironment(
                networkConfig,
                shuffleEnvironmentContext.getTaskExecutorResourceId(),
                shuffleEnvironmentContext.getEventPublisher(),
                shuffleEnvironmentContext.getParentMetricGroup(),
                shuffleEnvironmentContext.getIoExecutor(),
                shuffleEnvironmentContext.getScheduledExecutor(),
                shuffleEnvironmentContext.getNumberOfSlots(),
                shuffleEnvironmentContext.getTmpDirPaths());
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 调用内部方法重载构建NettyShuffleEnvironment
     * createNettyShuffleEnvironment
    */
    @VisibleForTesting
    static NettyShuffleEnvironment createNettyShuffleEnvironment(
            NettyShuffleEnvironmentConfiguration config,
            ResourceID taskExecutorResourceId,
            TaskEventPublisher taskEventPublisher,
            MetricGroup metricGroup,
            Executor ioExecutor,
            ScheduledExecutor scheduledExecutor,
            int numberOfSlots,
            String[] tmpDirPaths) {
        return createNettyShuffleEnvironment(
                config,
                taskExecutorResourceId,
                taskEventPublisher,
                new ResultPartitionManager(
                        config.getPartitionRequestListenerTimeout(), scheduledExecutor),
                metricGroup,
                ioExecutor,
                numberOfSlots,
                tmpDirPaths);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 构建一个基于Netty的NettyShuffleEnvironment
    */
    @VisibleForTesting
    static NettyShuffleEnvironment createNettyShuffleEnvironment(
            NettyShuffleEnvironmentConfiguration config,
            ResourceID taskExecutorResourceId,
            TaskEventPublisher taskEventPublisher,
            ResultPartitionManager resultPartitionManager,
            MetricGroup metricGroup,
            Executor ioExecutor,
            int numberOfSlots,
            String[] tmpDirPaths) {
        //获取Netty环境创建的相关配置
        NettyConfig nettyConfig = config.nettyConfig();
        //构建ConnectionManager
        ConnectionManager connectionManager =
                nettyConfig != null
                        ? new NettyConnectionManager(
                                resultPartitionManager,
                                taskEventPublisher,
                                nettyConfig,
                                config.getMaxNumberOfConnections(),
                                config.isConnectionReuseEnabled())
                        : new LocalConnectionManager();
        //构建netty执行环境上下文NettyShuffleEnvironment
        return createNettyShuffleEnvironment(
                config,
                taskExecutorResourceId,
                taskEventPublisher,
                resultPartitionManager,
                connectionManager,
                metricGroup,
                ioExecutor,
                numberOfSlots,
                tmpDirPaths);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建一个NettyShuffleEnvironment的静态方法
     * @param config NettyShuffleEnvironment的配置参数
     * @param taskExecutorResourceId 任务执行器的资源ID
     * @param taskEventPublisher 任务事件发布者
     * @param resultPartitionManager 结果分区管理器
     * @param connectionManager 连接管理器
     * @param metricGroup 度量组
     * @param ioExecutor IO执行器
     * @param numberOfSlots 插槽数量
     * @param tmpDirPaths 临时目录路径数组
    */
    @VisibleForTesting
    public static NettyShuffleEnvironment createNettyShuffleEnvironment(
            NettyShuffleEnvironmentConfiguration config,
            ResourceID taskExecutorResourceId,
            TaskEventPublisher taskEventPublisher,
            ResultPartitionManager resultPartitionManager,
            ConnectionManager connectionManager,
            MetricGroup metricGroup,
            Executor ioExecutor,
            int numberOfSlots,
            String[] tmpDirPaths) {
        // 检查配置等参数是否为空
        checkNotNull(config);
        checkNotNull(taskExecutorResourceId);
        checkNotNull(taskEventPublisher);
        checkNotNull(resultPartitionManager);
        checkNotNull(metricGroup);
        checkNotNull(connectionManager);
        // 根据配置中的临时目录和前缀创建一个FileChannelManager实例
        FileChannelManager fileChannelManager =
                new FileChannelManagerImpl(config.getTempDirs(), DIR_NAME_PREFIX);
        // 如果日志级别允许，记录已创建的FileChannelManager使用的目录
        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "Created a new {} for storing result partitions of BLOCKING shuffles. Used directories:\n\t{}",
                    FileChannelManager.class.getSimpleName(),
                    Arrays.stream(fileChannelManager.getPaths())
                            .map(File::getAbsolutePath)
                            .collect(Collectors.joining("\n\t")));
        }

        // 创建一个网络缓冲区池，用于网络传输的缓冲区管理
        NetworkBufferPool networkBufferPool =
                new NetworkBufferPool(
                        config.numNetworkBuffers(),
                        config.networkBufferSize(),
                        config.getRequestSegmentsTimeout());

        // we create a separated buffer pool here for batch shuffle instead of reusing the network
        // buffer pool directly to avoid potential side effects of memory contention, for example,
        // dead lock or "insufficient network buffer" error

        // 为批处理shuffle创建一个独立的缓冲区池，以避免直接复用网络缓冲区池可能导致的内存竞争等副作用
        // 例如死锁或"网络缓冲区不足"的错误
        BatchShuffleReadBufferPool batchShuffleReadBufferPool =
                new BatchShuffleReadBufferPool(
                        config.batchShuffleReadMemoryBytes(), config.networkBufferSize());

        // we create a separated IO executor pool here for batch shuffle instead of reusing the
        // TaskManager IO executor pool directly to avoid the potential side effects of execution
        // contention, for example, too long IO or waiting time leading to starvation or timeout
        // 为批处理shuffle创建一个独立的IO执行器池，以避免直接复用TaskManager的IO执行器池可能导致的执行竞争等副作用
        // 例如过长的IO操作或等待时间导致资源饥饿或超时
        // 注意：此段代码在原始文本中被截断了，但这里我们假设它将创建一个ScheduledExecutorService
        ScheduledExecutorService batchShuffleReadIOExecutor =
                Executors.newScheduledThreadPool(
                        Math.max(
                                1,
                                Math.min(
                                        batchShuffleReadBufferPool.getMaxConcurrentRequests(),
                                        Math.max(numberOfSlots, tmpDirPaths.length))),
                        new ExecutorThreadFactory("blocking-shuffle-io"));

        registerShuffleMetrics(metricGroup, networkBufferPool);

        Optional<TieredResultPartitionFactory> tieredResultPartitionFactory = Optional.empty();
        TieredStorageConfiguration tieredStorageConfiguration =
                config.getTieredStorageConfiguration();
        TieredStorageNettyServiceImpl tieredStorageNettyService = null;
        if (tieredStorageConfiguration != null) {
            TieredStorageResourceRegistry tieredStorageResourceRegistry =
                    new TieredStorageResourceRegistry();
            tieredStorageNettyService =
                    new TieredStorageNettyServiceImpl(tieredStorageResourceRegistry);
            tieredResultPartitionFactory =
                    Optional.of(
                            new TieredResultPartitionFactory(
                                    tieredStorageConfiguration,
                                    tieredStorageNettyService,
                                    tieredStorageResourceRegistry));
        }
        ResultPartitionFactory resultPartitionFactory =
                new ResultPartitionFactory(
                        resultPartitionManager,
                        fileChannelManager,
                        networkBufferPool,
                        batchShuffleReadBufferPool,
                        batchShuffleReadIOExecutor,
                        config.getBlockingSubpartitionType(),
                        config.networkBuffersPerChannel(),
                        config.floatingNetworkBuffersPerGate(),
                        config.networkBufferSize(),
                        config.isBatchShuffleCompressionEnabled(),
                        config.getCompressionCodec(),
                        config.getMaxBuffersPerChannel(),
                        config.sortShuffleMinBuffers(),
                        config.sortShuffleMinParallelism(),
                        config.isSSLEnabled(),
                        config.getMaxOverdraftBuffersPerGate(),
                        config.getHybridShuffleSpilledIndexRegionGroupSize(),
                        config.getHybridShuffleNumRetainedInMemoryRegionsMax(),
                        tieredResultPartitionFactory);
        //构建SingleInputGateFactory
        SingleInputGateFactory singleInputGateFactory =
                new SingleInputGateFactory(
                        taskExecutorResourceId,
                        config,
                        connectionManager,
                        resultPartitionManager,
                        taskEventPublisher,
                        networkBufferPool,
                        tieredStorageConfiguration,
                        tieredStorageNettyService);
        //构建Netty上下文执行环境
        return new NettyShuffleEnvironment(
                taskExecutorResourceId,
                config,
                networkBufferPool,
                connectionManager,
                resultPartitionManager,
                fileChannelManager,
                resultPartitionFactory,
                singleInputGateFactory,
                ioExecutor,
                batchShuffleReadBufferPool,
                batchShuffleReadIOExecutor);
    }
}
