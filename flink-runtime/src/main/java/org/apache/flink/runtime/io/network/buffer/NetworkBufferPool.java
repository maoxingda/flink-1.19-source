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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The NetworkBufferPool is a fixed size pool of {@link MemorySegment} instances for the network
 * stack.
 *
 * <p>The NetworkBufferPool creates {@link LocalBufferPool}s from which the individual tasks draw
 * the buffers for the network data transfer. When new local buffer pools are created, the
 * NetworkBufferPool dynamically redistributes the buffers between the pools.
 */
public class NetworkBufferPool
        implements BufferPoolFactory, MemorySegmentProvider, AvailabilityProvider {

    public static final int UNBOUNDED_POOL_SIZE = Integer.MAX_VALUE;

    private static final int USAGE_WARNING_THRESHOLD = 100;

    private static final Logger LOG = LoggerFactory.getLogger(NetworkBufferPool.class);

    private final int totalNumberOfMemorySegments;

    private final int memorySegmentSize;

    private final ArrayDeque<MemorySegment> availableMemorySegments;

    private volatile boolean isDestroyed;

    // ---- Managed buffer pools ----------------------------------------------

    private final Object factoryLock = new Object();

    private final Set<LocalBufferPool> allBufferPools = new HashSet<>();

    private final Set<LocalBufferPool> resizableBufferPools = new HashSet<>();

    private int numTotalRequiredBuffers;

    private final Duration requestSegmentsTimeout;

    private final AvailabilityHelper availabilityHelper = new AvailabilityHelper();

    private int lastCheckedUsage = -1;

    @VisibleForTesting
    public NetworkBufferPool(int numberOfSegmentsToAllocate, int segmentSize) {
        this(numberOfSegmentsToAllocate, segmentSize, Duration.ofMillis(Integer.MAX_VALUE));
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * @param numberOfSegmentsToAllocate 4096
     * @param segmentSize 32768
     * @param requestSegmentsTimeout PT30S
    */
    /** Allocates all {@link MemorySegment} instances managed by this pool. */
    public NetworkBufferPool(
            int numberOfSegmentsToAllocate, int segmentSize, Duration requestSegmentsTimeout) {
        // todo 设置需要分配的内存段总数
        this.totalNumberOfMemorySegments = numberOfSegmentsToAllocate;
        // todo 设置每个内存段的大小
        this.memorySegmentSize = segmentSize;
        // 检查requestSegmentsTimeout是否为空
        Preconditions.checkNotNull(requestSegmentsTimeout);
        // 检查requestSegmentsTimeout是否为正数
        checkArgument(
                requestSegmentsTimeout.toMillis() > 0,
                "The timeout for requesting exclusive buffers should be positive.");
        // 设置请求Segments缓冲区的超时时间
        this.requestSegmentsTimeout = requestSegmentsTimeout;

        final long sizeInLong = (long) segmentSize;

        try {
            // todo 初始化一个ArrayDeque来存储可用的内存段，大小为需要分配的内存段总数
            this.availableMemorySegments = new ArrayDeque<>(numberOfSegmentsToAllocate);
        } catch (OutOfMemoryError err) {
            //失败抛出异常
            throw new OutOfMemoryError(
                    "Could not allocate buffer queue of length "
                            + numberOfSegmentsToAllocate
                            + " - "
                            + err.getMessage());
        }

        try {
            // todo 循环分配内存段，直到达到需要的数量
            for (int i = 0; i < numberOfSegmentsToAllocate; i++) {
                // todo 使用MemorySegmentFactory分配堆外内存，并添加到可用内存段队列中
                availableMemorySegments.add(
                        MemorySegmentFactory.allocateUnpooledOffHeapMemory(segmentSize, null));
            }
        } catch (OutOfMemoryError err) {
            // 如果在分配内存段时发生OutOfMemoryError，则释放已分配的内存段，并抛出带有详细信息的OutOfMemoryError
            int allocated = availableMemorySegments.size();

            // free some memory
            availableMemorySegments.clear();

            long requiredMb = (sizeInLong * numberOfSegmentsToAllocate) >> 20;
            long allocatedMb = (sizeInLong * allocated) >> 20;
            long missingMb = requiredMb - allocatedMb;

            throw new OutOfMemoryError(
                    "Could not allocate enough memory segments for NetworkBufferPool "
                            + "(required (MB): "
                            + requiredMb
                            + ", allocated (MB): "
                            + allocatedMb
                            + ", missing (MB): "
                            + missingMb
                            + "). Cause: "
                            + err.getMessage());
        }
        //todo 重置可用内存段的可用性状态
        availabilityHelper.resetAvailable();
        // todo 计算并打印已分配的内存大小（MB）
        long allocatedMb = (sizeInLong * availableMemorySegments.size()) >> 20;

        LOG.info(
                "Allocated {} MB for network buffer pool (number of memory segments: {}, bytes per segment: {}).",
                allocatedMb,
                availableMemorySegments.size(),
                segmentSize);
    }

    /**
     * Different from {@link #requestUnpooledMemorySegments} for unpooled segments allocation. This
     * method and the below {@link #requestPooledMemorySegmentsBlocking} method are designed to be
     * used from {@link LocalBufferPool} for pooled memory segments allocation. Note that these
     * methods for pooled memory segments requesting and recycling are prohibited from acquiring the
     * factoryLock to avoid deadlock.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 请求池化的内存段
     * 此方法会同步访问可用内存段列表，并调用内部方法来请求内存段。
    */
    @Nullable
    public MemorySegment requestPooledMemorySegment() {
        // 同步块，确保对availableMemorySegments的线程安全访问
        synchronized (availableMemorySegments) {
            // 调用内部方法请求内存段
            return internalRequestMemorySegment();
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 阻塞式请求池化的内存段
     *
     * 尝试从池中阻塞式地请求指定数量的内存段。
     * 如果无法获取到足够数量的内存段，则当前线程会被阻塞，直到有足够数量的内存段可用。
     *
     * @param numberOfSegmentsToRequest 要请求的内存段数量
     * @return 返回请求到的内存段列表，每个列表项都是一个MemorySegment对象
     * @throws IOException 如果在请求内存段时发生I/O错误
    */
    public List<MemorySegment> requestPooledMemorySegmentsBlocking(int numberOfSegmentsToRequest)
            throws IOException {
        return internalRequestMemorySegments(numberOfSegmentsToRequest);
    }

    /**
     * Corresponding to {@link #requestPooledMemorySegmentsBlocking} and {@link
     * #requestPooledMemorySegment}, this method is for pooled memory segments recycling.
     */
    public void recyclePooledMemorySegment(MemorySegment segment) {
        // Adds the segment back to the queue, which does not immediately free the memory
        // however, since this happens when references to the global pool are also released,
        // making the availableMemorySegments queue and its contained object reclaimable
        internalRecycleMemorySegments(Collections.singleton(checkNotNull(segment)));
    }

    /**
     * Unpooled memory segments are requested directly from {@link NetworkBufferPool}, as opposed to
     * pooled segments, that are requested through {@link BufferPool} that was created from this
     * {@link NetworkBufferPool} (see {@link #createBufferPool}). They are used for example for
     * exclusive {@link RemoteInputChannel} credits, that are permanently assigned to that channel,
     * and never returned to any {@link BufferPool}. As opposed to pooled segments, when requested,
     * unpooled segments needs to be accounted against {@link #numTotalRequiredBuffers}, which might
     * require redistribution of the segments.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 请求非池化的内存段
     *
     * 尝试请求指定数量的非池化内存段。这些内存段不是从现有的内存段池中获取的，而是可能通过新分配或其他方式获取。
     * @param numberOfSegmentsToRequest 要请求的内存段数量，必须是非负数
     * @return 返回请求到的内存段列表，每个列表项都是一个MemorySegment对象
     * @throws IOException 如果在请求内存段时发生I/O错误
     * @throws IllegalStateException 如果网络缓冲区池已经被销毁
    */
    @Override
    public List<MemorySegment> requestUnpooledMemorySegments(int numberOfSegmentsToRequest)
            throws IOException {
        // 检查参数是否有效，请求的内存段数量必须是非负数
        checkArgument(
                numberOfSegmentsToRequest >= 0,
                "Number of buffers to request must be non-negative.");
        // 使用工厂锁确保同步访问，避免在缓冲区重新分配和销毁时发生并发问题
        synchronized (factoryLock) {
            // 检查网络缓冲区池是否已经被销毁
            if (isDestroyed) {
                throw new IllegalStateException("Network buffer pool has already been destroyed.");
            }
            // 如果请求的内存段数量为0，则直接返回空列表
            if (numberOfSegmentsToRequest == 0) {
                return Collections.emptyList();
            }
            // 尝试重新分配缓冲区以满足请求
            tryRedistributeBuffers(numberOfSegmentsToRequest);
        }

        try {
            // 调用内部方法尝试请求内存段
            return internalRequestMemorySegments(numberOfSegmentsToRequest);
        } catch (IOException exception) {
            // 如果在请求过程中发生I/O异常，则回滚已请求的内存段数量
            revertRequiredBuffers(numberOfSegmentsToRequest);
            ExceptionUtils.rethrowIOException(exception);
            return null;
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *
    */
    private List<MemorySegment> internalRequestMemorySegments(int numberOfSegmentsToRequest)
            throws IOException {
        // 创建一个新的列表来存储请求的MemorySegment对象，初始容量为所需数量
        final List<MemorySegment> segments = new ArrayList<>(numberOfSegmentsToRequest);
        try {
            // 设置一个超时时间，从当前时间开始
            final Deadline deadline = Deadline.fromNow(requestSegmentsTimeout);
            // 循环直到满足以下任一条件：
            // 1. 获取了所需数量的MemorySegment
            // 2. 缓冲区池被销毁
            // 3. 请求超时
            while (true) {
                // 检查缓冲区池是否已被销毁
                if (isDestroyed) {
                    throw new IllegalStateException("Buffer pool is destroyed.");
                }

                MemorySegment segment;
                // 同步块，确保线程安全地访问availableMemorySegments
                synchronized (availableMemorySegments) {
                    // 尝试从availableMemorySegments队列中获取一个MemorySegment
                    // 如果队列为空，则当前线程会在此处等待，直到被唤醒或超时
                    if ((segment = internalRequestMemorySegment()) == null) {
                        // 如果没有可用的MemorySegment，则等待2000毫秒
                        availableMemorySegments.wait(2000);
                    }
                }
                // 如果成功获取了MemorySegment，则将其添加到segments列表中
                if (segment != null) {
                    segments.add(segment);
                }
                // 如果已经获取了所需数量的MemorySegment，则跳出循环
                if (segments.size() >= numberOfSegmentsToRequest) {
                    break;
                }
                // 检查是否超时
                if (!deadline.hasTimeLeft()) {
                    throw new IOException(
                            String.format(
                                    "Timeout triggered when requesting exclusive buffers: %s, "
                                            + " or you may increase the timeout which is %dms by setting the key '%s'.",
                                    getConfigDescription(),
                                    requestSegmentsTimeout.toMillis(),
                                    NettyShuffleEnvironmentOptions
                                            .NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS
                                            .key()));
                }
            }
        } catch (Throwable e) {
            // 如果在请求MemorySegment过程中发生异常，则回收已经获取的MemorySegment
            internalRecycleMemorySegments(segments);
            // 重新抛出IOException
            ExceptionUtils.rethrowIOException(e);
        }
        // 返回获取到的MemorySegment列表
        return segments;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 请求MemorySegments
    */
    @Nullable
    private MemorySegment internalRequestMemorySegment() {
        assert Thread.holdsLock(availableMemorySegments);

        //从队列中获取 MemorySegment
        final MemorySegment segment = availableMemorySegments.poll();
        //检查队列是否为空
        if (availableMemorySegments.isEmpty() && segment != null) {
            //判断将当前可用状态重置为不可用
            availabilityHelper.resetUnavailable();
        }
        //返回内存段
        return segment;
    }

    /**
     * Corresponding to {@link #requestUnpooledMemorySegments}, this method is for unpooled memory
     * segments recycling.
     */
    @Override
    public void recycleUnpooledMemorySegments(Collection<MemorySegment> segments) {
        internalRecycleMemorySegments(segments);
        revertRequiredBuffers(segments.size());
    }

    private void revertRequiredBuffers(int size) {
        synchronized (factoryLock) {
            numTotalRequiredBuffers -= size;

            // note: if this fails, we're fine for the buffer pool since we already recycled the
            // segments
            redistributeBuffers();
        }
    }

    private void internalRecycleMemorySegments(Collection<MemorySegment> segments) {
        CompletableFuture<?> toNotify = null;
        synchronized (availableMemorySegments) {
            if (availableMemorySegments.isEmpty() && !segments.isEmpty()) {
                toNotify = availabilityHelper.getUnavailableToResetAvailable();
            }
            availableMemorySegments.addAll(segments);
            availableMemorySegments.notifyAll();
        }

        if (toNotify != null) {
            toNotify.complete(null);
        }
    }

    public void destroy() {
        synchronized (factoryLock) {
            isDestroyed = true;
        }

        synchronized (availableMemorySegments) {
            MemorySegment segment;
            while ((segment = availableMemorySegments.poll()) != null) {
                segment.free();
            }
        }
    }

    public boolean isDestroyed() {
        return isDestroyed;
    }

    public int getTotalNumberOfMemorySegments() {
        return isDestroyed() ? 0 : totalNumberOfMemorySegments;
    }

    public long getTotalMemory() {
        return (long) getTotalNumberOfMemorySegments() * memorySegmentSize;
    }

    public int getNumberOfAvailableMemorySegments() {
        synchronized (availableMemorySegments) {
            return availableMemorySegments.size();
        }
    }

    public long getAvailableMemory() {
        return (long) getNumberOfAvailableMemorySegments() * memorySegmentSize;
    }

    public int getNumberOfUsedMemorySegments() {
        return getTotalNumberOfMemorySegments() - getNumberOfAvailableMemorySegments();
    }

    public long getUsedMemory() {
        return (long) getNumberOfUsedMemorySegments() * memorySegmentSize;
    }

    public int getNumberOfRegisteredBufferPools() {
        synchronized (factoryLock) {
            return allBufferPools.size();
        }
    }

    public long getEstimatedNumberOfRequestedMemorySegments() {
        long requestedSegments = 0;
        synchronized (factoryLock) {
            for (LocalBufferPool bufferPool : allBufferPools) {
                requestedSegments += bufferPool.getEstimatedNumberOfRequestedMemorySegments();
            }
        }
        return requestedSegments;
    }

    public long getEstimatedRequestedMemory() {
        return getEstimatedNumberOfRequestedMemorySegments() * memorySegmentSize;
    }

    public int getEstimatedRequestedSegmentsUsage() {
        int totalNumberOfMemorySegments = getTotalNumberOfMemorySegments();
        return totalNumberOfMemorySegments == 0
                ? 0
                : Math.toIntExact(
                        100L
                                * getEstimatedNumberOfRequestedMemorySegments()
                                / totalNumberOfMemorySegments);
    }

    @VisibleForTesting
    Optional<String> getUsageWarning() {
        int currentUsage = getEstimatedRequestedSegmentsUsage();
        Optional<String> message = Optional.empty();
        // do not log warning if the value hasn't changed to avoid spamming warnings.
        if (currentUsage >= USAGE_WARNING_THRESHOLD && lastCheckedUsage != currentUsage) {
            long totalMemory = getTotalMemory();
            long requestedMemory = getEstimatedRequestedMemory();
            long missingMemory = requestedMemory - totalMemory;
            message =
                    Optional.of(
                            String.format(
                                    "Memory usage [%d%%] is too high to satisfy all of the requests. "
                                            + "This can severely impact network throughput. "
                                            + "Please consider increasing available network memory, "
                                            + "or decreasing configured size of network buffer pools. "
                                            + "(totalMemory=%s, requestedMemory=%s, missingMemory=%s)",
                                    currentUsage,
                                    new MemorySize(totalMemory).toHumanReadableString(),
                                    new MemorySize(requestedMemory).toHumanReadableString(),
                                    new MemorySize(missingMemory).toHumanReadableString()));
        } else if (currentUsage < USAGE_WARNING_THRESHOLD
                && lastCheckedUsage >= USAGE_WARNING_THRESHOLD) {
            message =
                    Optional.of(
                            String.format("Memory usage [%s%%] went back to normal", currentUsage));
        }
        lastCheckedUsage = currentUsage;
        return message;
    }

    public void maybeLogUsageWarning() {
        Optional<String> usageWarning = getUsageWarning();
        if (usageWarning.isPresent()) {
            LOG.warn(usageWarning.get());
        }
    }

    public int countBuffers() {
        int buffers = 0;

        synchronized (factoryLock) {
            for (BufferPool bp : allBufferPools) {
                buffers += bp.getNumBuffers();
            }
        }

        return buffers;
    }

    /** Returns a future that is completed when there are free segments in this pool. */
    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return availabilityHelper.getAvailableFuture();
    }

    // ------------------------------------------------------------------------
    // BufferPoolFactory
    // ------------------------------------------------------------------------
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 方法重载调用internalCreateBufferPool
    */
    @Override
    public BufferPool createBufferPool(int numRequiredBuffers, int maxUsedBuffers)
            throws IOException {
        return internalCreateBufferPool(
                numRequiredBuffers, maxUsedBuffers, 0, Integer.MAX_VALUE, 0);
    }

    @Override
    public BufferPool createBufferPool(
            int numRequiredBuffers,
            int maxUsedBuffers,
            int numSubpartitions,
            int maxBuffersPerChannel,
            int maxOverdraftBuffersPerGate)
            throws IOException {
        return internalCreateBufferPool(
                numRequiredBuffers,
                maxUsedBuffers,
                numSubpartitions,
                maxBuffersPerChannel,
                maxOverdraftBuffersPerGate);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建一个新的BufferPool对象
     * @param numRequiredBuffers,        所需的缓冲区数量
     * @param maxUsedBuffers,            最大使用的缓冲区数量
     * @param numSubpartitions,          子分区的数量
     * @param maxBuffersPerChannel,      每个通道的最大缓冲区数量
     * @param maxOverdraftBuffersPerGate 每个门限的最大透支缓冲区数量
     * @param
    */
    private BufferPool internalCreateBufferPool(
            int numRequiredBuffers,
            int maxUsedBuffers,
            int numSubpartitions,
            int maxBuffersPerChannel,
            int maxOverdraftBuffersPerGate)
            throws IOException {

        // It is necessary to use a separate lock from the one used for buffer
        // requests to ensure deadlock freedom for failure cases.
        // 使用与缓冲区请求不同的锁，以确保在失败情况下避免死锁
        // 使用factoryLock作为同步锁
        synchronized (factoryLock) {
            if (isDestroyed) {
                // 如果网络缓冲区池已经被销毁，则抛出异常
                throw new IllegalStateException("Network buffer pool has already been destroyed.");
            }

            // Ensure that the number of required buffers can be satisfied.
            // With dynamic memory management this should become obsolete.
            // 确保所需的缓冲区数量可以得到满足
            // 注意：随着动态内存管理的使用，此检查可能会变得不再必要
            if (numTotalRequiredBuffers + numRequiredBuffers > totalNumberOfMemorySegments) {
                // 如果所需的总缓冲区数量超过可用的内存段数量，则抛出异常
                throw new IOException(
                        String.format(
                                "Insufficient number of network buffers: "
                                        + "required %d, but only %d available. %s.",
                                numRequiredBuffers,
                                totalNumberOfMemorySegments - numTotalRequiredBuffers,
                                getConfigDescription()));
            }
            // 更新所需的总缓冲区数量
            this.numTotalRequiredBuffers += numRequiredBuffers;

            // We are good to go, create a new buffer pool and redistribute
            // non-fixed size buffers.
            // 创建新的本地缓冲区池
            LocalBufferPool localBufferPool =
                    new LocalBufferPool(
                            this,
                            numRequiredBuffers,
                            maxUsedBuffers,
                            numSubpartitions,
                            maxBuffersPerChannel,
                            maxOverdraftBuffersPerGate);
            // 将新创建的本地缓冲区池添加到所有缓冲区池的列表中
            allBufferPools.add(localBufferPool);

            // 如果所需的缓冲区数量小于最大使用的缓冲区数量，则将其添加到可调整大小的缓冲区池列表中
            if (numRequiredBuffers < maxUsedBuffers) {
                resizableBufferPools.add(localBufferPool);
            }
            // 重新计算非固定大小的缓冲区
            redistributeBuffers();
            // 返回新创建的本地缓冲区池
            return localBufferPool;
        }
    }

    @Override
    public void destroyBufferPool(BufferPool bufferPool) {
        if (!(bufferPool instanceof LocalBufferPool)) {
            throw new IllegalArgumentException("bufferPool is no LocalBufferPool");
        }

        synchronized (factoryLock) {
            if (allBufferPools.remove(bufferPool)) {
                numTotalRequiredBuffers -= bufferPool.getNumberOfRequiredMemorySegments();
                resizableBufferPools.remove(bufferPool);

                redistributeBuffers();
            }
        }
    }

    /**
     * Destroys all buffer pools that allocate their buffers from this buffer pool (created via
     * {@link #createBufferPool(int, int)}).
     */
    public void destroyAllBufferPools() {
        synchronized (factoryLock) {
            // create a copy to avoid concurrent modification exceptions
            LocalBufferPool[] poolsCopy =
                    allBufferPools.toArray(new LocalBufferPool[allBufferPools.size()]);

            for (LocalBufferPool pool : poolsCopy) {
                pool.lazyDestroy();
            }

            // some sanity checks
            if (allBufferPools.size() > 0
                    || numTotalRequiredBuffers > 0
                    || resizableBufferPools.size() > 0) {
                throw new IllegalStateException(
                        "NetworkBufferPool is not empty after destroying all LocalBufferPools");
            }
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 尝试重新分配缓冲区
     *
     * 尝试根据请求的数量重新分配内存段，确保有足够的资源来满足当前请求。
     * @param numberOfSegmentsToRequest 需要请求的内存段数量
     * @throws IOException 如果当前可用的内存段数量不足以满足请求，则抛出此异常
    */
    // Must be called from synchronized block
    private void tryRedistributeBuffers(int numberOfSegmentsToRequest) throws IOException {
        // 断言当前线程必须持有 factoryLock 锁
        assert Thread.holdsLock(factoryLock);
        // 如果已请求的总缓冲区数量加上此次请求的数量超过了总的内存段数量
        if (numTotalRequiredBuffers + numberOfSegmentsToRequest > totalNumberOfMemorySegments) {
            // 抛出IOException，说明网络缓冲区不足
            throw new IOException(
                    String.format(
                            "Insufficient number of network buffers: "
                                    + "required %d, but only %d available. %s.",
                            numberOfSegmentsToRequest,
                            totalNumberOfMemorySegments - numTotalRequiredBuffers,
                            getConfigDescription()));
        }
        // 增加当前已请求的总缓冲区数量
        this.numTotalRequiredBuffers += numberOfSegmentsToRequest;

        try {
            // 尝试重新分配缓冲区
            redistributeBuffers();
        } catch (Throwable t) {
            // 如果在重新分配过程中出现异常，回退已请求的总缓冲区数量
            this.numTotalRequiredBuffers -= numberOfSegmentsToRequest;
            // 尝试再次重新分配缓冲区（可能是为了保持状态一致）
            redistributeBuffers();
            // 重新抛出捕获的异常
            ExceptionUtils.rethrow(t);
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 重新计算非固定大小的缓冲区
    */
    // Must be called from synchronized block
    private void redistributeBuffers() {
        assert Thread.holdsLock(factoryLock);
        // 如果可调整大小的Buffer池为空，则直接返回
        if (resizableBufferPools.isEmpty()) {
            return;
        }

        // All buffers, which are not among the required ones
        // 计算可用的内存段数量，即总内存段数量减去所需的总Buffer数量
        final int numAvailableMemorySegment = totalNumberOfMemorySegments - numTotalRequiredBuffers;

        // 如果没有可用的内存段，则重新分配Buffer，使得每个池都获得其所需的最小数量
        if (numAvailableMemorySegment == 0) {
            // in this case, we need to redistribute buffers so that every pool gets its minimum
            for (LocalBufferPool bufferPool : resizableBufferPools) {
                bufferPool.setNumBuffers(bufferPool.getNumberOfRequiredMemorySegments());
            }
            return;
        }

        /*
         * With buffer pools being potentially limited, let's distribute the available memory
         * segments based on the capacity of each buffer pool, i.e. the maximum number of segments
         * an unlimited buffer pool can take is numAvailableMemorySegment, for limited buffer pools
         * it may be less. Based on this and the sum of all these values (totalCapacity), we build
         * a ratio that we use to distribute the buffers.
         */
        // 计算所有可调整大小的Buffer池的总容量（基于其最大容量和所需容量之间的差值）
        long totalCapacity = 0; // long to avoid int overflow
        for (LocalBufferPool bufferPool : resizableBufferPools) {
            // 计算每个池的最大容量与所需容量的差值（即额外的最大容量）
            int excessMax =
                    bufferPool.getMaxNumberOfMemorySegments()
                            - bufferPool.getNumberOfRequiredMemorySegments();
            // 将差值与可用内存段数量进行比较，取较小值，并累加到总容量中
            totalCapacity += Math.min(numAvailableMemorySegment, excessMax);
        }

        // 如果没有容量接收额外的Buffer，则直接返回，避免后续出现除以零的情况
        // no capacity to receive additional buffers?
        if (totalCapacity == 0) {
            return; // necessary to avoid div by zero when nothing to re-distribute
        }

        // since one of the arguments of 'min(a,b)' is a positive int, this is actually
        // guaranteed to be within the 'int' domain
        // (we use a checked downCast to handle possible bugs more gracefully).
        // 分配内存段到各个Buffer池，根据每个池的容量占总容量的比例进行分配
        // 注意：由于'min(a,b)'的一个参数是正整数，所以结果一定在'int'范围内
        final int memorySegmentsToDistribute =
                MathUtils.checkedDownCast(Math.min(numAvailableMemorySegment, totalCapacity));

        // 已使用的总容量部分
        long totalPartsUsed = 0; // of totalCapacity
        // 已分配的内存段数量
        int numDistributedMemorySegment = 0;
        // 遍历可调整大小的缓冲池列表
        for (LocalBufferPool bufferPool : resizableBufferPools) {
            // 计算当前缓冲池中超过所需数量的内存段数量
            int excessMax =
                    bufferPool.getMaxNumberOfMemorySegments()
                            - bufferPool.getNumberOfRequiredMemorySegments();

            // 如果没有多余的内存段，则跳过此次循环
            // shortcut
            if (excessMax == 0) {
                continue;
            }
            // 将可用的内存段数量与超出所需的内存段数量中的较小值累加到已使用的总部分中
            totalPartsUsed += Math.min(numAvailableMemorySegment, excessMax);

            // avoid remaining buffers by looking at the total capacity that should have been
            // re-distributed up until here
            // the downcast will always succeed, because both arguments of the subtraction are in
            // the 'int' domain

            // 避免剩余缓冲段，通过查看到目前为止应重新分配的总容量来计算
            // 由于两个减法参数都在'int'范围内，所以向下转型总是成功的
            final int mySize =
                    MathUtils.checkedDownCast(
                            memorySegmentsToDistribute * totalPartsUsed / totalCapacity
                                    - numDistributedMemorySegment);
            // 更新已分配的内存段数量
            numDistributedMemorySegment += mySize;
            // 更新缓冲池中的缓冲段数量
            bufferPool.setNumBuffers(bufferPool.getNumberOfRequiredMemorySegments() + mySize);
        }
         // 断言，确保已使用的总部分等于总容量
        assert (totalPartsUsed == totalCapacity);
        // 断言，确保已分配的内存段数量等于需要分配的内存段总数
        assert (numDistributedMemorySegment == memorySegmentsToDistribute);
    }

    private String getConfigDescription() {
        return String.format(
                "The total number of network buffers is currently set to %d of %d bytes each. "
                        + "You can increase this number by setting the configuration keys '%s', '%s', and '%s'",
                totalNumberOfMemorySegments,
                memorySegmentSize,
                TaskManagerOptions.NETWORK_MEMORY_FRACTION.key(),
                TaskManagerOptions.NETWORK_MEMORY_MIN.key(),
                TaskManagerOptions.NETWORK_MEMORY_MAX.key());
    }
}
