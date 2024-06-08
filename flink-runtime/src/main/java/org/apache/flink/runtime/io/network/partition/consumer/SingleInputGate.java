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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.RecoveryMetadata;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.PrioritizedDeque;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageInputChannelId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyServiceImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.AvailabilityNotifier;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.throughput.BufferDebloater;
import org.apache.flink.runtime.throughput.ThroughputCalculator;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An input gate consumes one or more partitions of a single produced intermediate result.
 *
 * <p>Each intermediate result is partitioned over its producing parallel subtasks; each of these
 * partitions is furthermore partitioned into one or more subpartitions.
 *
 * <p>As an example, consider a map-reduce program, where the map operator produces data and the
 * reduce operator consumes the produced data.
 *
 * <pre>{@code
 * +-----+              +---------------------+              +--------+
 * | Map | = produce => | Intermediate Result | <= consume = | Reduce |
 * +-----+              +---------------------+              +--------+
 * }</pre>
 *
 * <p>When deploying such a program in parallel, the intermediate result will be partitioned over
 * its producing parallel subtasks; each of these partitions is furthermore partitioned into one or
 * more subpartitions.
 *
 * <pre>{@code
 *                            Intermediate result
 *               +-----------------------------------------+
 *               |                      +----------------+ |              +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <=======+=== | Input Gate | Reduce 1 |
 * | Map 1 | ==> | | Partition 1 | =|   +----------------+ |         |    +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+    |
 *               |                      +----------------+ |    |    | Subpartition request
 *               |                                         |    |    |
 *               |                      +----------------+ |    |    |
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <==+====+
 * | Map 2 | ==> | | Partition 2 | =|   +----------------+ |    |         +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+======== | Input Gate | Reduce 2 |
 *               |                      +----------------+ |              +-----------------------+
 *               +-----------------------------------------+
 * }</pre>
 *
 * <p>In the above example, two map subtasks produce the intermediate result in parallel, resulting
 * in two partitions (Partition 1 and 2). Each of these partitions is further partitioned into two
 * subpartitions -- one for each parallel reduce subtask.
 */
public class SingleInputGate extends IndexedInputGate {

    private static final Logger LOG = LoggerFactory.getLogger(SingleInputGate.class);

    /** Lock object to guard partition requests and runtime channel updates. */
    private final Object requestLock = new Object();

    /** The name of the owning task, for logging purposes. */
    private final String owningTaskName;

    private final int gateIndex;

    /**
     * The ID of the consumed intermediate result. Each input gate consumes partitions of the
     * intermediate result specified by this ID. This ID also identifies the input gate at the
     * consuming task.
     */
    private final IntermediateDataSetID consumedResultId;

    /** The type of the partition the input gate is consuming. */
    private final ResultPartitionType consumedPartitionType;

    /** The number of input channels (equivalent to the number of consumed partitions). */
    private final int numberOfInputChannels;

    /** Input channels. We store this in a map for runtime updates of single channels. */
    private final Map<IntermediateResultPartitionID, Map<InputChannelInfo, InputChannel>>
            inputChannels;

    @GuardedBy("requestLock")
    private final InputChannel[] channels;

    /** Channels, which notified this input gate about available data. */
    private final PrioritizedDeque<InputChannel> inputChannelsWithData = new PrioritizedDeque<>();

    /**
     * Field guaranteeing uniqueness for inputChannelsWithData queue. Both of those fields should be
     * unified onto one.
     */
    @GuardedBy("inputChannelsWithData")
    private final BitSet enqueuedInputChannelsWithData;

    @GuardedBy("inputChannelsWithData")
    private final BitSet channelsWithEndOfPartitionEvents;

    @GuardedBy("inputChannelsWithData")
    private final BitSet channelsWithEndOfUserRecords;

    @GuardedBy("inputChannelsWithData")
    private int[] lastPrioritySequenceNumber;

    /** The partition producer state listener. */
    private final PartitionProducerStateProvider partitionProducerStateProvider;

    /**
     * Buffer pool for incoming buffers. Incoming data from remote channels is copied to buffers
     * from this pool.
     */
    private BufferPool bufferPool;

    private boolean hasReceivedAllEndOfPartitionEvents;

    private boolean hasReceivedEndOfData;

    /** Flag indicating whether partitions have been requested. */
    /** 指示是否已请求分区的标志 */
    private boolean requestedPartitionsFlag;

    private final List<TaskEvent> pendingEvents = new ArrayList<>();

    private int numberOfUninitializedChannels;

    /** A timer to retrigger local partition requests. Only initialized if actually needed. */
    private Timer retriggerLocalRequestTimer;

    private final SupplierWithException<BufferPool, IOException> bufferPoolFactory;

    private final CompletableFuture<Void> closeFuture;

    @Nullable private final BufferDecompressor bufferDecompressor;

    private final MemorySegmentProvider memorySegmentProvider;

    /**
     * The segment to read data from file region of bounded blocking partition by local input
     * channel.
     */
    private final MemorySegment unpooledSegment;

    private final ThroughputCalculator throughputCalculator;
    private final BufferDebloater bufferDebloater;
    private boolean shouldDrainOnEndOfData = true;

    // The consumer client will be null if the tiered storage is not enabled.
    @Nullable private TieredStorageConsumerClient tieredStorageConsumerClient;

    // The consumer specs in tiered storage will be null if the tiered storage is not enabled.
    @Nullable private List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs;

    // The availability notifier will be null if the tiered storage is not enabled.
    @Nullable private AvailabilityNotifier availabilityNotifier;

    /**
     * A map containing the status of the last consumed buffer in each input channel. The status
     * contains the following information: 1) whether the buffer contains partial record, and 2) the
     * index of the subpartition where the buffer comes from.
     */
    private final Map<Integer, Tuple2<Boolean, Integer>> lastBufferStatusMapInTieredStore =
            new HashMap<>();

    /** A map of counters for the number of {@link EndOfData}s received from each input channel. */
    private final int[] endOfDatas;

    /**
     * A map of counters for the number of {@link EndOfPartitionEvent}s received from each input
     * channel.
     */
    private final int[] endOfPartitions;

    public SingleInputGate(
            String owningTaskName,
            int gateIndex,
            IntermediateDataSetID consumedResultId,
            final ResultPartitionType consumedPartitionType,
            int numberOfInputChannels,
            PartitionProducerStateProvider partitionProducerStateProvider,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory,
            @Nullable BufferDecompressor bufferDecompressor,
            MemorySegmentProvider memorySegmentProvider,
            int segmentSize,
            ThroughputCalculator throughputCalculator,
            @Nullable BufferDebloater bufferDebloater) {

        this.owningTaskName = checkNotNull(owningTaskName);
        Preconditions.checkArgument(0 <= gateIndex, "The gate index must be positive.");
        this.gateIndex = gateIndex;

        this.consumedResultId = checkNotNull(consumedResultId);
        this.consumedPartitionType = checkNotNull(consumedPartitionType);
        this.bufferPoolFactory = checkNotNull(bufferPoolFactory);

        checkArgument(numberOfInputChannels > 0);
        this.numberOfInputChannels = numberOfInputChannels;

        this.inputChannels = CollectionUtil.newHashMapWithExpectedSize(numberOfInputChannels);
        this.channels = new InputChannel[numberOfInputChannels];
        this.channelsWithEndOfPartitionEvents = new BitSet(numberOfInputChannels);
        this.channelsWithEndOfUserRecords = new BitSet(numberOfInputChannels);
        this.enqueuedInputChannelsWithData = new BitSet(numberOfInputChannels);
        this.lastPrioritySequenceNumber = new int[numberOfInputChannels];
        Arrays.fill(lastPrioritySequenceNumber, Integer.MIN_VALUE);

        this.partitionProducerStateProvider = checkNotNull(partitionProducerStateProvider);

        this.bufferDecompressor = bufferDecompressor;
        this.memorySegmentProvider = checkNotNull(memorySegmentProvider);

        this.closeFuture = new CompletableFuture<>();

        this.unpooledSegment = MemorySegmentFactory.allocateUnpooledSegment(segmentSize);
        this.bufferDebloater = bufferDebloater;
        this.throughputCalculator = checkNotNull(throughputCalculator);

        this.tieredStorageConsumerClient = null;
        this.tieredStorageConsumerSpecs = null;
        this.availabilityNotifier = null;

        this.endOfDatas = new int[numberOfInputChannels];
        Arrays.fill(endOfDatas, 0);
        this.endOfPartitions = new int[numberOfInputChannels];
        Arrays.fill(endOfPartitions, 0);
    }

    protected PrioritizedDeque<InputChannel> getInputChannelsWithData() {
        return inputChannelsWithData;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * SingleInputGate初始化InputChannel
    */
    @Override
    public void setup() throws IOException {

        // 检查状态，确保当前对象的bufferPool属性为null
        // 如果不为null，则抛出异常，表示在输入门设置逻辑中存在bug，因为已经注册了buffer pool
        checkState(
                this.bufferPool == null,
                "Bug in input gate setup logic: Already registered buffer pool.");
        // 通过bufferPoolFactory获取一个BufferPool对象
        //一个动态大小的缓冲池
        BufferPool bufferPool = bufferPoolFactory.get();
        // 设置当前对象的bufferPool属性为刚才获取的BufferPool对象
        setBufferPool(bufferPool);
        // 调用setupChannels()方法，用于设置初始化InputChannel
        setupChannels();
    }

    @Override
    public CompletableFuture<Void> getStateConsumedFuture() {
        synchronized (requestLock) {
            List<CompletableFuture<?>> futures = new ArrayList<>(numberOfInputChannels);
            for (InputChannel inputChannel : inputChannels()) {
                if (inputChannel instanceof RecoveredInputChannel) {
                    futures.add(((RecoveredInputChannel) inputChannel).getStateConsumedFuture());
                }
            }
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 请求Partitions
    */
    @Override
    public void requestPartitions() {
        // 使用同步块，确保在多线程环境下对共享资源的访问是线程安全的
        synchronized (requestLock) {
            // 检查是否已经请求过分区
            if (!requestedPartitionsFlag) {
                // 检查是否已经关闭
                if (closeFuture.isDone()) {
                    // 如果已经关闭，则抛出异常
                    throw new IllegalStateException("Already released.");
                }

                // 合理性检查：确保当前总的输入通道数与预设的输入通道数一致
                // Sanity checks
                long numInputChannels =
                        inputChannels.values().stream().mapToLong(x -> x.values().size()).sum();
                if (numberOfInputChannels != numInputChannels) {
                    // 如果不一致，则抛出异常
                    throw new IllegalStateException(
                            String.format(
                                    "Bug in input gate setup logic: mismatch between "
                                            + "number of total input channels [%s] and the currently set number of input "
                                            + "channels [%s].",
                                    numInputChannels, numberOfInputChannels));
                }
                // 转换恢复中的输入通道
                convertRecoveredInputChannels();
                // 内部请求分区的方法
                internalRequestPartitions();
            }
            // 标记已经请求过分区
            requestedPartitionsFlag = true;
            // Start the reader only when all InputChannels have been converted to either
            // LocalInputChannel or RemoteInputChannel, as this will prevent RecoveredInputChannels
            // from being queued again.
            if (enabledTieredStorage()) {
                tieredStorageConsumerClient.start();
            }
        }
    }

    @VisibleForTesting
    public void convertRecoveredInputChannels() {
        LOG.debug("Converting recovered input channels ({} channels)", getNumberOfInputChannels());
        for (Map<InputChannelInfo, InputChannel> inputChannelsForCurrentPartition :
                inputChannels.values()) {
            Set<InputChannelInfo> oldInputChannelInfos =
                    new HashSet<>(inputChannelsForCurrentPartition.keySet());
            for (InputChannelInfo inputChannelInfo : oldInputChannelInfos) {
                InputChannel inputChannel = inputChannelsForCurrentPartition.get(inputChannelInfo);
                if (inputChannel instanceof RecoveredInputChannel) {
                    try {
                        InputChannel realInputChannel =
                                ((RecoveredInputChannel) inputChannel).toInputChannel();
                        inputChannel.releaseAllResources();
                        inputChannelsForCurrentPartition.remove(inputChannelInfo);
                        inputChannelsForCurrentPartition.put(
                                realInputChannel.getChannelInfo(), realInputChannel);
                        channels[inputChannel.getChannelIndex()] = realInputChannel;
                    } catch (Throwable t) {
                        inputChannel.setError(t);
                        return;
                    }
                }
            }
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 每个输入通道请求子分区
    */
    private void internalRequestPartitions() {
        // 遍历所有的输入通道
        for (InputChannel inputChannel : inputChannels()) {
            try {
                // 向每个输入通道请求子分区
                inputChannel.requestSubpartitions();
            } catch (Throwable t) {
                //抛出异常
                inputChannel.setError(t);
                //返回
                return;
            }
        }
    }

    @Override
    public void finishReadRecoveredState() throws IOException {
        for (final InputChannel channel : channels) {
            if (channel instanceof RecoveredInputChannel) {
                ((RecoveredInputChannel) channel).finishReadRecoveredState();
            }
        }
    }

    // ------------------------------------------------------------------------
    // Properties
    // ------------------------------------------------------------------------

    @Override
    public int getNumberOfInputChannels() {
        return numberOfInputChannels;
    }

    @Override
    public int getGateIndex() {
        return gateIndex;
    }

    @Override
    public List<InputChannelInfo> getUnfinishedChannels() {
        List<InputChannelInfo> unfinishedChannels =
                new ArrayList<>(
                        numberOfInputChannels - channelsWithEndOfPartitionEvents.cardinality());
        synchronized (inputChannelsWithData) {
            for (int i = channelsWithEndOfPartitionEvents.nextClearBit(0);
                    i < numberOfInputChannels;
                    i = channelsWithEndOfPartitionEvents.nextClearBit(i + 1)) {
                unfinishedChannels.add(getChannel(i).getChannelInfo());
            }
        }

        return unfinishedChannels;
    }

    @VisibleForTesting
    int getBuffersInUseCount() {
        int total = 0;
        for (InputChannel channel : channels) {
            total += channel.getBuffersInUseCount();
        }
        return total;
    }

    @VisibleForTesting
    public void announceBufferSize(int newBufferSize) {
        for (InputChannel channel : channels) {
            if (!channel.isReleased()) {
                channel.announceBufferSize(newBufferSize);
            }
        }
    }

    @Override
    public void triggerDebloating() {
        if (isFinished() || closeFuture.isDone()) {
            return;
        }

        checkState(bufferDebloater != null, "Buffer debloater should not be null");
        final long currentThroughput = throughputCalculator.calculateThroughput();
        bufferDebloater
                .recalculateBufferSize(currentThroughput, getBuffersInUseCount())
                .ifPresent(this::announceBufferSize);
    }

    public Duration getLastEstimatedTimeToConsume() {
        return bufferDebloater.getLastEstimatedTimeToConsumeBuffers();
    }

    /**
     * Returns the type of this input channel's consumed result partition.
     *
     * @return consumed result partition type
     */
    public ResultPartitionType getConsumedPartitionType() {
        return consumedPartitionType;
    }

    BufferProvider getBufferProvider() {
        return bufferPool;
    }

    public BufferPool getBufferPool() {
        return bufferPool;
    }

    MemorySegmentProvider getMemorySegmentProvider() {
        return memorySegmentProvider;
    }

    public String getOwningTaskName() {
        return owningTaskName;
    }

    public int getNumberOfQueuedBuffers() {
        // re-try 3 times, if fails, return 0 for "unknown"
        for (int retry = 0; retry < 3; retry++) {
            try {
                int totalBuffers = 0;

                for (InputChannel channel : inputChannels()) {
                    totalBuffers += channel.unsynchronizedGetNumberOfQueuedBuffers();
                }

                return totalBuffers;
            } catch (Exception ex) {
                LOG.debug("Fail to get number of queued buffers :", ex);
            }
        }

        return 0;
    }

    public long getSizeOfQueuedBuffers() {
        // re-try 3 times, if fails, return 0 for "unknown"
        for (int retry = 0; retry < 3; retry++) {
            try {
                long totalSize = 0;

                for (InputChannel channel : inputChannels()) {
                    totalSize += channel.unsynchronizedGetSizeOfQueuedBuffers();
                }

                return totalSize;
            } catch (Exception ex) {
                LOG.debug("Fail to get size of queued buffers :", ex);
            }
        }

        return 0;
    }

    public CompletableFuture<Void> getCloseFuture() {
        return closeFuture;
    }

    @Override
    public InputChannel getChannel(int channelIndex) {
        return channels[channelIndex];
    }

    // ------------------------------------------------------------------------
    // Setup/Life-cycle
    // ------------------------------------------------------------------------

    public void setBufferPool(BufferPool bufferPool) {
        checkState(
                this.bufferPool == null,
                "Bug in input gate setup logic: buffer pool has"
                        + "already been set for this input gate.");

        this.bufferPool = checkNotNull(bufferPool);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 设置Channel
    */
    /** Assign the exclusive buffers to all remote input channels directly for credit-based mode. */
    @VisibleForTesting
    public void setupChannels() throws IOException {
        // Allocate enough exclusive and floating buffers to guarantee that job can make progress.
        // Note: An exception will be thrown if there is no buffer available in the given timeout.

        // First allocate a single floating buffer to avoid potential deadlock when the exclusive
        // buffer is 0. See FLINK-24035 for more information.
        // 为保证作业能够进行，分配足够的独占和浮动缓冲区
        // 注意：如果在给定的超时时间内没有可用的缓冲区，将会抛出异常
        // 首先分配一个单独的浮动缓冲区，以避免当独占缓冲区数量为0时可能出现的死锁。
        bufferPool.reserveSegments(1);

        // Next allocate the exclusive buffers per channel when the number of exclusive buffer is
        // larger than 0.
        // 当独占缓冲区的数量大于0时，为每个通道分配独占缓冲区
        synchronized (requestLock) {
            // 为每个输入通道设置独占缓冲区
            for (InputChannel inputChannel : inputChannels()) {
                /** 为此输入通道分配独占缓冲区，并且此方法应在创建此输入通道后仅调用一次。 */
                inputChannel.setup();
            }
        }
    }

    public void setInputChannels(InputChannel... channels) {
        if (channels.length != numberOfInputChannels) {
            throw new IllegalArgumentException(
                    "Expected "
                            + numberOfInputChannels
                            + " channels, "
                            + "but got "
                            + channels.length);
        }
        synchronized (requestLock) {
            System.arraycopy(channels, 0, this.channels, 0, numberOfInputChannels);
            for (InputChannel inputChannel : channels) {
                if (inputChannels
                                        .computeIfAbsent(
                                                inputChannel.getPartitionId().getPartitionId(),
                                                ignored -> new HashMap<>())
                                        .put(inputChannel.getChannelInfo(), inputChannel)
                                == null
                        && inputChannel instanceof UnknownInputChannel) {

                    numberOfUninitializedChannels++;
                }
            }
        }
    }

    public void setTieredStorageService(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            TieredStorageConsumerClient client,
            TieredStorageNettyServiceImpl nettyService) {
        this.tieredStorageConsumerSpecs = tieredStorageConsumerSpecs;
        this.tieredStorageConsumerClient = client;
        if (client != null) {
            this.availabilityNotifier = new AvailabilityNotifierImpl();
            setupTieredStorageNettyService(nettyService, tieredStorageConsumerSpecs);
            client.registerAvailabilityNotifier(availabilityNotifier);
        }
    }

    public void updateInputChannel(
            ResourceID localLocation, NettyShuffleDescriptor shuffleDescriptor)
            throws IOException, InterruptedException {
        synchronized (requestLock) {
            if (closeFuture.isDone()) {
                // There was a race with a task failure/cancel
                return;
            }

            IntermediateResultPartitionID partitionId =
                    shuffleDescriptor.getResultPartitionID().getPartitionId();

            Map<InputChannelInfo, InputChannel> newInputChannels = new HashMap<>();
            for (InputChannel current : inputChannels.get(partitionId).values()) {
                if (current instanceof UnknownInputChannel) {
                    UnknownInputChannel unknownChannel = (UnknownInputChannel) current;
                    boolean isLocal = shuffleDescriptor.isLocalTo(localLocation);
                    InputChannel newChannel;
                    if (isLocal) {
                        newChannel =
                                unknownChannel.toLocalInputChannel(
                                        shuffleDescriptor.getResultPartitionID());
                    } else {
                        RemoteInputChannel remoteInputChannel =
                                unknownChannel.toRemoteInputChannel(
                                        shuffleDescriptor.getConnectionId());
                        remoteInputChannel.setup();
                        newChannel = remoteInputChannel;
                    }
                    LOG.debug(
                            "{}: Updated unknown input channel to {}.", owningTaskName, newChannel);

                    newInputChannels.put(newChannel.getChannelInfo(), newChannel);
                    channels[current.getChannelIndex()] = newChannel;

                    if (requestedPartitionsFlag) {
                        newChannel.requestSubpartitions();
                    }

                    for (TaskEvent event : pendingEvents) {
                        newChannel.sendTaskEvent(event);
                    }

                    if (--numberOfUninitializedChannels == 0) {
                        pendingEvents.clear();
                    }
                }
            }

            inputChannels.put(partitionId, newInputChannels);
        }
    }

    /** Retriggers a partition request. */
    public void retriggerPartitionRequest(
            IntermediateResultPartitionID partitionId, InputChannelInfo inputChannelInfo)
            throws IOException {
        synchronized (requestLock) {
            if (!closeFuture.isDone()) {
                final InputChannel ch = inputChannels.get(partitionId).get(inputChannelInfo);

                checkNotNull(ch, "Unknown input channel with ID " + partitionId);

                LOG.debug(
                        "{}: Retriggering partition request {}:{}.",
                        owningTaskName,
                        ch.partitionId,
                        ch.getConsumedSubpartitionIndexSet());

                if (ch.getClass() == RemoteInputChannel.class) {
                    final RemoteInputChannel rch = (RemoteInputChannel) ch;
                    rch.retriggerSubpartitionRequest();
                } else if (ch.getClass() == LocalInputChannel.class) {
                    final LocalInputChannel ich = (LocalInputChannel) ch;

                    if (retriggerLocalRequestTimer == null) {
                        retriggerLocalRequestTimer = new Timer(true);
                    }

                    ich.retriggerSubpartitionRequest(retriggerLocalRequestTimer);
                } else {
                    throw new IllegalStateException(
                            "Unexpected type of channel to retrigger partition: " + ch.getClass());
                }
            }
        }
    }

    @VisibleForTesting
    Timer getRetriggerLocalRequestTimer() {
        return retriggerLocalRequestTimer;
    }

    MemorySegment getUnpooledSegment() {
        return unpooledSegment;
    }

    @Override
    public void close() throws IOException {
        boolean released = false;
        synchronized (requestLock) {
            if (!closeFuture.isDone()) {
                try {
                    LOG.debug("{}: Releasing {}.", owningTaskName, this);

                    if (retriggerLocalRequestTimer != null) {
                        retriggerLocalRequestTimer.cancel();
                    }

                    for (InputChannel inputChannel : inputChannels()) {
                        try {
                            inputChannel.releaseAllResources();
                        } catch (IOException e) {
                            LOG.warn(
                                    "{}: Error during release of channel resources: {}.",
                                    owningTaskName,
                                    e.getMessage(),
                                    e);
                        }
                    }

                    // The buffer pool can actually be destroyed immediately after the
                    // reader received all of the data from the input channels.
                    if (bufferPool != null) {
                        bufferPool.lazyDestroy();
                    }
                } finally {
                    released = true;
                    closeFuture.complete(null);
                }
            }
        }

        if (released) {
            synchronized (inputChannelsWithData) {
                inputChannelsWithData.notifyAll();
            }
            if (enabledTieredStorage()) {
                tieredStorageConsumerClient.close();
            }
        }
    }

    @Override
    public boolean isFinished() {
        return hasReceivedAllEndOfPartitionEvents;
    }

    @Override
    public EndOfDataStatus hasReceivedEndOfData() {
        if (!hasReceivedEndOfData) {
            return EndOfDataStatus.NOT_END_OF_DATA;
        } else if (shouldDrainOnEndOfData) {
            return EndOfDataStatus.DRAINED;
        } else {
            return EndOfDataStatus.STOPPED;
        }
    }

    @Override
    public String toString() {
        return "SingleInputGate{"
                + "owningTaskName='"
                + owningTaskName
                + '\''
                + ", gateIndex="
                + gateIndex
                + '}';
    }

    // ------------------------------------------------------------------------
    // Consume
    // ------------------------------------------------------------------------

    @Override
    public Optional<BufferOrEvent> getNext() throws IOException, InterruptedException {
        return getNextBufferOrEvent(true);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 调用getNextBufferOrEvent读取BufferOrEvent
    */
    @Override
    public Optional<BufferOrEvent> pollNext() throws IOException, InterruptedException {
        return getNextBufferOrEvent(false);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 从输入流中获取下一个Buffer或Event，根据blocking参数决定是否为阻塞模式。
     *
     * @param blocking 是否为阻塞模式，true为阻塞模式，false为非阻塞模式
     * @return 包含下一个Buffer或Event的Optional对象，如果没有则返回Optional.empty()
     * @throws IOException 如果在读取过程中发生I/O错误
     * @throws InterruptedException 如果线程在等待时被中断
    */
    private Optional<BufferOrEvent> getNextBufferOrEvent(boolean blocking)
            throws IOException, InterruptedException {
        // 如果已经收到了所有分区结束的事件，则返回空的Optional对象
        if (hasReceivedAllEndOfPartitionEvents) {
            return Optional.empty();
        }
        // 如果closeFuture已经完成（表示InputGate已经关闭），则抛出异常
        if (closeFuture.isDone()) {
            throw new CancelTaskException("Input gate is already closed.");
        }
        // 等待并获取下一个数据，根据blocking参数决定是否阻塞
        Optional<InputWithData<InputChannel, Buffer>> next = waitAndGetNextData(blocking);
        // 如果没有获取到数据，则暂停吞吐量计算并返回空的Optional对象
        if (!next.isPresent()) {
            throughputCalculator.pauseMeasurement();
            return Optional.empty();
        }
        // 恢复吞吐量计算
        throughputCalculator.resumeMeasurement();

        InputWithData<InputChannel, Buffer> inputWithData = next.get();
        // 将输入数据转换为BufferOrEvent对象
        final BufferOrEvent bufferOrEvent =
                transformToBufferOrEvent(
                        inputWithData.data,
                        inputWithData.moreAvailable,
                        inputWithData.input,
                        inputWithData.morePriorityEvents);
        // 更新吞吐量计算的输入数据大小
        throughputCalculator.incomingDataSize(bufferOrEvent.getSize());
        // 返回包含BufferOrEvent的Optional对象
        return Optional.of(bufferOrEvent);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 等待并获取下一个输入数据。
     *
     * @param blocking 是否为阻塞模式，true为阻塞模式，false为非阻塞模式
     * @return 包含下一个输入数据的Optional对象，如果没有则返回Optional.empty()
     * @throws IOException 如果在读取过程中发生I/O错误
     * @throws InterruptedException 如果线程在等待时被中断
    */
    private Optional<InputWithData<InputChannel, Buffer>> waitAndGetNextData(boolean blocking)
            throws IOException, InterruptedException {
        while (true) {
            // 使用synchronized关键字确保对inputChannelsWithData的访问是线程安全的
            synchronized (inputChannelsWithData) {
                // 尝试从输入通道中获取一个可用的通道
                Optional<InputChannel> inputChannelOpt = getChannel(blocking);
                if (!inputChannelOpt.isPresent()) {
                    // 如果没有可用的输入通道，则返回空的Optional对象
                    return Optional.empty();
                }
                // 获取到可用的输入通道
                final InputChannel inputChannel = inputChannelOpt.get();
                // 从输入通道中读取一个Buffer，可能是恢复的数据或正常数据
                Optional<Buffer> buffer = readRecoveredOrNormalBuffer(inputChannel);
                if (!buffer.isPresent()) {
                    // 如果没有读取到Buffer，则检查不可用性（可能是通道已关闭或数据已读取完）
                    checkUnavailability();
                    continue;
                }
                // 获取输入通道已消费的子分区索引集的大小
                int numSubpartitions = inputChannel.getConsumedSubpartitionIndexSet().size();
                if (numSubpartitions > 1) {
                    // 如果当前通道有多个子分区的数据
                    switch (buffer.get().getDataType()) {
                        case END_OF_DATA:
                            // 如果读取到的是“数据结束”类型的Buffer
                            endOfDatas[inputChannel.getChannelIndex()]++;
                            // 如果该通道接收到的“数据结束”事件数小于子分区数，则回收Buffer并继续循环
                            if (endOfDatas[inputChannel.getChannelIndex()] < numSubpartitions) {
                                buffer.get().recycleBuffer();
                                continue;
                            }
                            break;
                        case END_OF_PARTITION:
                            // 如果读取到的是“分区结束”类型的Buffer
                            endOfPartitions[inputChannel.getChannelIndex()]++;
                            // 如果该通道接收到的“分区结束”事件数小于子分区数，则回收Buffer并继续循环
                            if (endOfPartitions[inputChannel.getChannelIndex()]
                                    < numSubpartitions) {
                                buffer.get().recycleBuffer();
                                continue;
                            }
                            break;
                        default:
                            // 其他类型的数据，直接结束
                            break;
                    }
                }
                // 检查是否还有优先级事件等待处理
                final boolean morePriorityEvents =
                        inputChannelsWithData.getNumPriorityElements() > 0;
                if (buffer.get().getDataType().hasPriority()) {
                    if (!morePriorityEvents) {
                        priorityAvailabilityHelper.resetUnavailable();
                    }
                }
                checkUnavailability();
                //需要实际构建InputWithData对象
                return Optional.of(
                        new InputWithData<>(
                                inputChannel,
                                buffer.get(),
                                !inputChannelsWithData.isEmpty(),
                                morePriorityEvents));
            }
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 读取缓存数据
    */
    private Optional<Buffer> readRecoveredOrNormalBuffer(InputChannel inputChannel)
            throws IOException, InterruptedException {
        // Firstly, read the buffers from the recovered channel
        // 尝试首先从恢复通道读取缓存 输入通道从以前未对齐的检查点快照读取恢复的状态 checkpoint
        if (inputChannel instanceof RecoveredInputChannel && !inputChannel.isReleased()) {
            // 如果输入通道是RecoveredInputChannel的实例且未被释放
            // 读取缓存
            Optional<Buffer> buffer = readBufferFromInputChannel(inputChannel);
            // 如果恢复通道的状态消费Future还未完成（即还有数据可读）
            if (!((RecoveredInputChannel) inputChannel).getStateConsumedFuture().isDone()) {
                // 则直接返回读取到的缓存（可能还有后续数据等待读取）
                return buffer;
            }
        }

        //  After the recovered buffers are read, read the normal buffers
        // 读取完恢复通道中的缓存后，读取正常缓存
        return enabledTieredStorage()
                // 从分层存储中读取缓存
                ? readBufferFromTieredStore(inputChannel)
                : readBufferFromInputChannel(inputChannel); // 否则，直接从输入通道中读取缓存
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 通过Channel读取数据
    */
    private Optional<Buffer> readBufferFromInputChannel(InputChannel inputChannel)
            throws IOException, InterruptedException {
        // 尝试从输入通道中获取下一个缓存和可用性信息
        Optional<BufferAndAvailability> bufferAndAvailabilityOpt = inputChannel.getNextBuffer();
        // 如果没有可用的缓存和可用性信息，则返回空的Optional对象
        if (!bufferAndAvailabilityOpt.isPresent()) {
            return Optional.empty();
        }
        // 获取缓存和可用性信息
        final BufferAndAvailability bufferAndAvailability = bufferAndAvailabilityOpt.get();
        // 如果还有更多可用的缓存，将输入通道重新放入队列的末尾
        if (bufferAndAvailability.moreAvailable()) {
            // enqueue the inputChannel at the end to avoid starvation
            queueChannelUnsafe(inputChannel, bufferAndAvailability.morePriorityEvents());
        }
        // 如果缓存具有优先级，则更新最后处理的优先级序列号
        if (bufferAndAvailability.hasPriority()) {
            lastPrioritySequenceNumber[inputChannel.getChannelIndex()] =
                    bufferAndAvailability.getSequenceNumber();
        }
        // 获取缓存本身
        Buffer buffer = bufferAndAvailability.buffer();
        // 如果缓存的数据类型是恢复元数据
        if (buffer.getDataType() == Buffer.DataType.RECOVERY_METADATA) {
            // 从缓存的可读ByteBuffer中反序列化恢复元数据
            RecoveryMetadata recoveryMetadata =
                    (RecoveryMetadata)
                            EventSerializer.fromSerializedEvent(
                                    buffer.getNioBufferReadable(), getClass().getClassLoader());
            // 更新层级存储中最后缓存的状态映射，包括是否是部分记录以及恢复元数据的最终子分区ID
            lastBufferStatusMapInTieredStore.put(
                    inputChannel.getChannelIndex(),
                    Tuple2.of(
                            buffer.getDataType().isPartialRecord(),
                            recoveryMetadata.getFinalBufferSubpartitionId()));
        }
        // 返回包含缓存的Optional对象
        return Optional.of(bufferAndAvailability.buffer());
    }

    private Optional<Buffer> readBufferFromTieredStore(InputChannel inputChannel)
            throws IOException {
        TieredStorageConsumerSpec tieredStorageConsumerSpec =
                checkNotNull(tieredStorageConsumerSpecs).get(inputChannel.getChannelIndex());
        Tuple2<Boolean, Integer> lastBufferStatus =
                lastBufferStatusMapInTieredStore.computeIfAbsent(
                        inputChannel.getChannelIndex(), key -> Tuple2.of(false, -1));
        boolean isLastBufferPartialRecord = lastBufferStatus.f0;
        int lastSubpartitionId = lastBufferStatus.f1;

        while (true) {
            int subpartitionId;
            if (isLastBufferPartialRecord) {
                subpartitionId = lastSubpartitionId;
            } else {
                subpartitionId =
                        checkNotNull(tieredStorageConsumerClient)
                                .peekNextBufferSubpartitionId(
                                        tieredStorageConsumerSpec.getPartitionId(),
                                        tieredStorageConsumerSpec.getSubpartitionIds());
            }

            if (subpartitionId < 0) {
                return Optional.empty();
            }

            // If the data is available in the specific partition and subpartition, read buffer
            // through consumer client.
            Optional<Buffer> buffer =
                    checkNotNull(tieredStorageConsumerClient)
                            .getNextBuffer(
                                    tieredStorageConsumerSpec.getPartitionId(),
                                    new TieredStorageSubpartitionId(subpartitionId));

            if (buffer.isPresent()) {
                if (!(inputChannel instanceof RecoveredInputChannel)) {
                    queueChannel(checkNotNull(inputChannel), null, false);
                }
                lastBufferStatusMapInTieredStore.put(
                        inputChannel.getChannelIndex(),
                        Tuple2.of(buffer.get().getDataType().isPartialRecord(), subpartitionId));
            } else {
                if (!isLastBufferPartialRecord
                        && inputChannel.getConsumedSubpartitionIndexSet().size() > 1) {
                    // Continue to check other subpartitions that have been marked as
                    // available.
                    continue;
                }
            }

            return buffer;
        }
    }

    private boolean enabledTieredStorage() {
        return tieredStorageConsumerClient != null;
    }

    private void checkUnavailability() {
        assert Thread.holdsLock(inputChannelsWithData);

        if (inputChannelsWithData.isEmpty()) {
            availabilityHelper.resetUnavailable();
        }
    }

    private BufferOrEvent transformToBufferOrEvent(
            Buffer buffer,
            boolean moreAvailable,
            InputChannel currentChannel,
            boolean morePriorityEvents)
            throws IOException, InterruptedException {
        if (buffer.isBuffer()) {
            return transformBuffer(buffer, moreAvailable, currentChannel, morePriorityEvents);
        } else {
            return transformEvent(buffer, moreAvailable, currentChannel, morePriorityEvents);
        }
    }

    private BufferOrEvent transformBuffer(
            Buffer buffer,
            boolean moreAvailable,
            InputChannel currentChannel,
            boolean morePriorityEvents) {
        return new BufferOrEvent(
                decompressBufferIfNeeded(buffer),
                currentChannel.getChannelInfo(),
                moreAvailable,
                morePriorityEvents);
    }

    private BufferOrEvent transformEvent(
            Buffer buffer,
            boolean moreAvailable,
            InputChannel currentChannel,
            boolean morePriorityEvents)
            throws IOException, InterruptedException {
        final AbstractEvent event;
        try {
            event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
        } finally {
            buffer.recycleBuffer();
        }

        if (event.getClass() == EndOfPartitionEvent.class) {
            synchronized (inputChannelsWithData) {
                checkState(!channelsWithEndOfPartitionEvents.get(currentChannel.getChannelIndex()));
                channelsWithEndOfPartitionEvents.set(currentChannel.getChannelIndex());
                hasReceivedAllEndOfPartitionEvents =
                        channelsWithEndOfPartitionEvents.cardinality() == numberOfInputChannels;

                enqueuedInputChannelsWithData.clear(currentChannel.getChannelIndex());
                if (inputChannelsWithData.contains(currentChannel)) {
                    inputChannelsWithData.getAndRemove(channel -> channel == currentChannel);
                }
            }
            if (hasReceivedAllEndOfPartitionEvents) {
                // Because of race condition between:
                // 1. releasing inputChannelsWithData lock in this method and reaching this place
                // 2. empty data notification that re-enqueues a channel we can end up with
                // moreAvailable flag set to true, while we expect no more data.
                checkState(!moreAvailable || !pollNext().isPresent());
                moreAvailable = false;
                markAvailable();
            }

            currentChannel.releaseAllResources();
        } else if (event.getClass() == EndOfData.class) {
            synchronized (inputChannelsWithData) {
                checkState(!channelsWithEndOfUserRecords.get(currentChannel.getChannelIndex()));
                channelsWithEndOfUserRecords.set(currentChannel.getChannelIndex());
                hasReceivedEndOfData =
                        channelsWithEndOfUserRecords.cardinality() == numberOfInputChannels;
                shouldDrainOnEndOfData &= ((EndOfData) event).getStopMode() == StopMode.DRAIN;
            }
        }

        return new BufferOrEvent(
                event,
                buffer.getDataType().hasPriority(),
                currentChannel.getChannelInfo(),
                moreAvailable,
                buffer.getSize(),
                morePriorityEvents);
    }

    private Buffer decompressBufferIfNeeded(Buffer buffer) {
        if (buffer.isCompressed()) {
            try {
                checkNotNull(bufferDecompressor, "Buffer decompressor not set.");
                return bufferDecompressor.decompressToIntermediateBuffer(buffer);
            } finally {
                buffer.recycleBuffer();
            }
        }
        return buffer;
    }

    private void markAvailable() {
        CompletableFuture<?> toNotify;
        synchronized (inputChannelsWithData) {
            toNotify = availabilityHelper.getUnavailableToResetAvailable();
        }
        toNotify.complete(null);
    }

    @Override
    public void sendTaskEvent(TaskEvent event) throws IOException {
        synchronized (requestLock) {
            for (InputChannel inputChannel : inputChannels()) {
                inputChannel.sendTaskEvent(event);
            }

            if (numberOfUninitializedChannels > 0) {
                pendingEvents.add(event);
            }
        }
    }

    @Override
    public void resumeConsumption(InputChannelInfo channelInfo) throws IOException {
        checkState(!isFinished(), "InputGate already finished.");
        // BEWARE: consumption resumption only happens for streaming jobs in which all slots
        // are allocated together so there should be no UnknownInputChannel. As a result, it
        // is safe to not synchronize the requestLock here. We will refactor the code to not
        // rely on this assumption in the future.
        channels[channelInfo.getInputChannelIdx()].resumeConsumption();
    }

    @Override
    public void acknowledgeAllRecordsProcessed(InputChannelInfo channelInfo) throws IOException {
        checkState(!isFinished(), "InputGate already finished.");
        if (!enabledTieredStorage()) {
            channels[channelInfo.getInputChannelIdx()].acknowledgeAllRecordsProcessed();
        }
    }

    // ------------------------------------------------------------------------
    // Channel notifications
    // ------------------------------------------------------------------------

    void notifyChannelNonEmpty(InputChannel channel) {
        if (enabledTieredStorage()) {
            TieredStorageConsumerSpec tieredStorageConsumerSpec =
                    checkNotNull(tieredStorageConsumerSpecs).get(channel.getChannelIndex());
            checkNotNull(availabilityNotifier)
                    .notifyAvailable(
                            tieredStorageConsumerSpec.getPartitionId(),
                            tieredStorageConsumerSpec.getInputChannelId());
        } else {
            queueChannel(checkNotNull(channel), null, false);
        }
    }

    /**
     * Notifies that the respective channel has a priority event at the head for the given buffer
     * number.
     *
     * <p>The buffer number limits the notification to the respective buffer and voids the whole
     * notification in case that the buffer has been polled in the meantime. That is, if task thread
     * polls the enqueued priority buffer before this notification occurs (notification is not
     * performed under lock), this buffer number allows {@link #queueChannel(InputChannel, Integer,
     * boolean)} to avoid spurious priority wake-ups.
     */
    void notifyPriorityEvent(InputChannel inputChannel, int prioritySequenceNumber) {
        queueChannel(checkNotNull(inputChannel), prioritySequenceNumber, false);
    }

    void notifyPriorityEventForce(InputChannel inputChannel) {
        queueChannel(checkNotNull(inputChannel), null, true);
    }

    void triggerPartitionStateCheck(
            ResultPartitionID partitionId, InputChannelInfo inputChannelInfo) {
        partitionProducerStateProvider.requestPartitionProducerState(
                consumedResultId,
                partitionId,
                ((PartitionProducerStateProvider.ResponseHandle responseHandle) -> {
                    boolean isProducingState =
                            new RemoteChannelStateChecker(partitionId, owningTaskName)
                                    .isProducerReadyOrAbortConsumption(responseHandle);
                    if (isProducingState) {
                        try {
                            retriggerPartitionRequest(
                                    partitionId.getPartitionId(), inputChannelInfo);
                        } catch (IOException t) {
                            responseHandle.failConsumption(t);
                        }
                    }
                }));
    }

    private void queueChannel(
            InputChannel channel, @Nullable Integer prioritySequenceNumber, boolean forcePriority) {
        try (GateNotificationHelper notification =
                new GateNotificationHelper(this, inputChannelsWithData)) {
            synchronized (inputChannelsWithData) {
                boolean priority = prioritySequenceNumber != null || forcePriority;

                if (!forcePriority
                        && priority
                        && isOutdated(
                                prioritySequenceNumber,
                                lastPrioritySequenceNumber[channel.getChannelIndex()])) {
                    // priority event at the given offset already polled (notification is not atomic
                    // in respect to
                    // buffer enqueuing), so just ignore the notification
                    return;
                }

                if (!queueChannelUnsafe(channel, priority)) {
                    return;
                }

                if (priority && inputChannelsWithData.getNumPriorityElements() == 1) {
                    notification.notifyPriority();
                }
                if (inputChannelsWithData.size() == 1) {
                    notification.notifyDataAvailable();
                }
            }
        }
    }

    private boolean isOutdated(int sequenceNumber, int lastSequenceNumber) {
        if ((lastSequenceNumber < 0) != (sequenceNumber < 0)
                && Math.max(lastSequenceNumber, sequenceNumber) > Integer.MAX_VALUE / 2) {
            // probably overflow of one of the two numbers, the negative one is greater then
            return lastSequenceNumber < 0;
        }
        return lastSequenceNumber >= sequenceNumber;
    }

    /**
     * Queues the channel if not already enqueued and not received EndOfPartition, potentially
     * raising the priority.
     *
     * @return true iff it has been enqueued/prioritized = some change to {@link
     *     #inputChannelsWithData} happened
     */
    private boolean queueChannelUnsafe(InputChannel channel, boolean priority) {
        assert Thread.holdsLock(inputChannelsWithData);
        if (channelsWithEndOfPartitionEvents.get(channel.getChannelIndex())) {
            return false;
        }

        final boolean alreadyEnqueued =
                enqueuedInputChannelsWithData.get(channel.getChannelIndex());
        if (alreadyEnqueued
                && (!priority || inputChannelsWithData.containsPriorityElement(channel))) {
            // already notified / prioritized (double notification), ignore
            return false;
        }

        inputChannelsWithData.add(channel, priority, alreadyEnqueued);
        if (!alreadyEnqueued) {
            enqueuedInputChannelsWithData.set(channel.getChannelIndex());
        }
        return true;
    }

    private Optional<InputChannel> getChannel(boolean blocking) throws InterruptedException {
        assert Thread.holdsLock(inputChannelsWithData);

        while (inputChannelsWithData.isEmpty()) {
            if (closeFuture.isDone()) {
                throw new IllegalStateException("Released");
            }

            if (blocking) {
                inputChannelsWithData.wait();
            } else {
                availabilityHelper.resetUnavailable();
                return Optional.empty();
            }
        }

        InputChannel inputChannel = inputChannelsWithData.poll();
        enqueuedInputChannelsWithData.clear(inputChannel.getChannelIndex());

        return Optional.of(inputChannel);
    }

    private void setupTieredStorageNettyService(
            TieredStorageNettyServiceImpl nettyService,
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs) {
        List<Supplier<InputChannel>> channelSuppliers = new ArrayList<>();
        for (int index = 0; index < channels.length; ++index) {
            int channelIndex = index;
            channelSuppliers.add(() -> channels[channelIndex]);
        }
        nettyService.setupInputChannels(tieredStorageConsumerSpecs, channelSuppliers);
    }

    /** The default implementation of {@link AvailabilityNotifier}. */
    private class AvailabilityNotifierImpl implements AvailabilityNotifier {

        private final Map<TieredStoragePartitionId, Map<TieredStorageSubpartitionId, Integer>>
                subpartitionIdMap;

        private final Map<TieredStoragePartitionId, Map<TieredStorageInputChannelId, Integer>>
                channelIdMap;

        private AvailabilityNotifierImpl() {
            this.subpartitionIdMap = new HashMap<>();
            this.channelIdMap = new HashMap<>();
            for (int index = 0; index < checkNotNull(tieredStorageConsumerSpecs).size(); index++) {
                TieredStorageConsumerSpec spec = tieredStorageConsumerSpecs.get(index);
                for (int subpartitionId : spec.getSubpartitionIds().values()) {
                    subpartitionIdMap
                            .computeIfAbsent(spec.getPartitionId(), ignore -> new HashMap<>())
                            .put(new TieredStorageSubpartitionId(subpartitionId), index);
                }
                channelIdMap
                        .computeIfAbsent(spec.getPartitionId(), ignore -> new HashMap<>())
                        .put(spec.getInputChannelId(), index);
            }
        }

        @Override
        public void notifyAvailable(
                TieredStoragePartitionId partitionId, TieredStorageSubpartitionId subpartitionId) {
            queueChannel(
                    channels[subpartitionIdMap.get(partitionId).get(subpartitionId)], null, false);
        }

        @Override
        public void notifyAvailable(
                TieredStoragePartitionId partitionId, TieredStorageInputChannelId inputChannelId) {
            queueChannel(channels[channelIdMap.get(partitionId).get(inputChannelId)], null, false);
        }
    }

    // ------------------------------------------------------------------------

    @VisibleForTesting
    public Map<Tuple2<IntermediateResultPartitionID, InputChannelInfo>, InputChannel>
            getInputChannels() {
        Map<Tuple2<IntermediateResultPartitionID, InputChannelInfo>, InputChannel> result =
                new HashMap<>();
        for (Map.Entry<IntermediateResultPartitionID, Map<InputChannelInfo, InputChannel>>
                mapEntry : inputChannels.entrySet()) {
            for (Map.Entry<InputChannelInfo, InputChannel> entry : mapEntry.getValue().entrySet()) {
                result.put(Tuple2.of(mapEntry.getKey(), entry.getKey()), entry.getValue());
            }
        }
        return result;
    }

    public Iterable<InputChannel> inputChannels() {
        return () ->
                new Iterator<InputChannel>() {
                    private final Iterator<Map<InputChannelInfo, InputChannel>> mapIterator =
                            inputChannels.values().iterator();

                    private Iterator<InputChannel> iterator = null;

                    @Override
                    public boolean hasNext() {
                        return (iterator != null && iterator.hasNext()) || mapIterator.hasNext();
                    }

                    @Override
                    public InputChannel next() {
                        if ((iterator == null || !iterator.hasNext()) && mapIterator.hasNext()) {
                            iterator = mapIterator.next().values().iterator();
                        }

                        if (iterator == null || !iterator.hasNext()) {
                            return null;
                        }

                        return iterator.next();
                    }
                };
    }
}
