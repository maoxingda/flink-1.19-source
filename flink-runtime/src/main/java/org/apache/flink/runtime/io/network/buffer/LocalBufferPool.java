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
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.flink.util.concurrent.FutureUtils.assertNoException;

/**
 * A buffer pool used to manage a number of {@link Buffer} instances from the {@link
 * NetworkBufferPool}.
 *
 * <p>Buffer requests are mediated to the network buffer pool to ensure deadlock free operation of
 * the network stack by limiting the number of buffers per local buffer pool. It also implements the
 * default mechanism for buffer recycling, which ensures that every buffer is ultimately returned to
 * the network buffer pool.
 *
 * <p>The size of this pool can be dynamically changed at runtime ({@link #setNumBuffers(int)}. It
 * will then lazily return the required number of buffers to the {@link NetworkBufferPool} to match
 * its new size.
 *
 * <p>New buffers can be requested only when {@code numberOfRequestedMemorySegments <
 * currentPoolSize + maxOverdraftBuffersPerGate}. In other words, all buffers exceeding the
 * currentPoolSize will be dynamically regarded as overdraft buffers.
 *
 * <p>Availability is defined as returning a non-overdraft segment on a subsequent {@link
 * #requestBuffer()}/ {@link #requestBufferBuilder()} and heaving a non-blocking {@link
 * #requestBufferBuilderBlocking(int)}. In particular,
 *
 * <ul>
 *   <li>There is at least one {@link #availableMemorySegments}.
 *   <li>No subpartitions has reached {@link #maxBuffersPerChannel}.
 * </ul>
 *
 * <p>To ensure this contract, the implementation eagerly fetches additional memory segments from
 * {@link NetworkBufferPool} as long as it hasn't reached {@link #maxNumberOfMemorySegments} or one
 * subpartition reached the quota.
 */
public class LocalBufferPool implements BufferPool {
    private static final Logger LOG = LoggerFactory.getLogger(LocalBufferPool.class);

    private static final int UNKNOWN_CHANNEL = -1;

    /** Global network buffer pool to get buffers from. */
    private final NetworkBufferPool networkBufferPool;

    /** The minimum number of required segments for this pool. */
    private final int numberOfRequiredMemorySegments;

    /**
     * The currently available memory segments. These are segments, which have been requested from
     * the network buffer pool and are currently not handed out as Buffer instances.
     *
     * <p><strong>BEWARE:</strong> Take special care with the interactions between this lock and
     * locks acquired before entering this class vs. locks being acquired during calls to external
     * code inside this class, e.g. with {@code
     * org.apache.flink.runtime.io.network.partition.consumer.BufferManager#bufferQueue} via the
     * {@link #registeredListeners} callback.
     */
    private final ArrayDeque<MemorySegment> availableMemorySegments = new ArrayDeque<>();

    /**
     * Buffer availability listeners, which need to be notified when a Buffer becomes available.
     * Listeners can only be registered at a time/state where no Buffer instance was available.
     */
    private final ArrayDeque<BufferListener> registeredListeners = new ArrayDeque<>();

    /** Maximum number of network buffers to allocate. */
    private final int maxNumberOfMemorySegments;

    /** The current size of this pool. */
    @GuardedBy("availableMemorySegments")
    private int currentPoolSize;

    /**
     * Number of all memory segments, which have been requested from the network buffer pool and are
     * somehow referenced through this pool (e.g. wrapped in Buffer instances or as available
     * segments).
     */
    /** */
    @GuardedBy("availableMemorySegments")
    private int numberOfRequestedMemorySegments;

    private final int maxBuffersPerChannel;

    @GuardedBy("availableMemorySegments")
    private final int[] subpartitionBuffersCount;

    private final BufferRecycler[] subpartitionBufferRecyclers;

    @GuardedBy("availableMemorySegments")
    private int unavailableSubpartitionsCount = 0;

    private int maxOverdraftBuffersPerGate;

    @GuardedBy("availableMemorySegments")
    private boolean isDestroyed;

    @GuardedBy("availableMemorySegments")
    private final AvailabilityHelper availabilityHelper = new AvailabilityHelper();

    /**
     * Indicates whether this {@link LocalBufferPool} has requested to be notified on the next time
     * that global pool becoming available, so it can then request buffer from the global pool.
     */
    @GuardedBy("availableMemorySegments")
    private boolean requestingNotificationOfGlobalPoolAvailable;

    /**
     * Local buffer pool based on the given <tt>networkBufferPool</tt> with a minimal number of
     * network buffers being available.
     *
     * @param networkBufferPool global network buffer pool to get buffers from
     * @param numberOfRequiredMemorySegments minimum number of network buffers
     */
    LocalBufferPool(NetworkBufferPool networkBufferPool, int numberOfRequiredMemorySegments) {
        this(
                networkBufferPool,
                numberOfRequiredMemorySegments,
                Integer.MAX_VALUE,
                0,
                Integer.MAX_VALUE,
                0);
    }

    /**
     * Local buffer pool based on the given <tt>networkBufferPool</tt> with a minimal and maximal
     * number of network buffers being available.
     *
     * @param networkBufferPool global network buffer pool to get buffers from
     * @param numberOfRequiredMemorySegments minimum number of network buffers
     * @param maxNumberOfMemorySegments maximum number of network buffers to allocate
     */
    LocalBufferPool(
            NetworkBufferPool networkBufferPool,
            int numberOfRequiredMemorySegments,
            int maxNumberOfMemorySegments) {
        this(
                networkBufferPool,
                numberOfRequiredMemorySegments,
                maxNumberOfMemorySegments,
                0,
                Integer.MAX_VALUE,
                0);
    }

    /**
     * Local buffer pool based on the given <tt>networkBufferPool</tt> and <tt>bufferPoolOwner</tt>
     * with a minimal and maximal number of network buffers being available.
     *
     * @param networkBufferPool global network buffer pool to get buffers from
     * @param numberOfRequiredMemorySegments minimum number of network buffers
     * @param maxNumberOfMemorySegments maximum number of network buffers to allocate
     * @param numberOfSubpartitions number of subpartitions
     * @param maxBuffersPerChannel maximum number of buffers to use for each channel
     * @param maxOverdraftBuffersPerGate maximum number of overdraft buffers to use for each gate
     */
    LocalBufferPool(
            NetworkBufferPool networkBufferPool,
            int numberOfRequiredMemorySegments,
            int maxNumberOfMemorySegments,
            int numberOfSubpartitions,
            int maxBuffersPerChannel,
            int maxOverdraftBuffersPerGate) {
        checkArgument(
                numberOfRequiredMemorySegments > 0,
                "Required number of memory segments (%s) should be larger than 0.",
                numberOfRequiredMemorySegments);

        checkArgument(
                maxNumberOfMemorySegments >= numberOfRequiredMemorySegments,
                "Maximum number of memory segments (%s) should not be smaller than minimum (%s).",
                maxNumberOfMemorySegments,
                numberOfRequiredMemorySegments);

        LOG.debug(
                "Using a local buffer pool with {}-{} buffers",
                numberOfRequiredMemorySegments,
                maxNumberOfMemorySegments);

        this.networkBufferPool = networkBufferPool;
        this.numberOfRequiredMemorySegments = numberOfRequiredMemorySegments;
        this.currentPoolSize = numberOfRequiredMemorySegments;
        this.maxNumberOfMemorySegments = maxNumberOfMemorySegments;

        if (numberOfSubpartitions > 0) {
            checkArgument(
                    maxBuffersPerChannel > 0,
                    "Maximum number of buffers for each channel (%s) should be larger than 0.",
                    maxBuffersPerChannel);
            checkArgument(
                    maxOverdraftBuffersPerGate >= 0,
                    "Maximum number of overdraft buffers for each gate (%s) should not be less than 0.",
                    maxOverdraftBuffersPerGate);
        }

        this.subpartitionBuffersCount = new int[numberOfSubpartitions];
        subpartitionBufferRecyclers = new BufferRecycler[numberOfSubpartitions];
        for (int i = 0; i < subpartitionBufferRecyclers.length; i++) {
            subpartitionBufferRecyclers[i] = new SubpartitionBufferRecycler(i, this);
        }
        this.maxBuffersPerChannel = maxBuffersPerChannel;
        this.maxOverdraftBuffersPerGate = maxOverdraftBuffersPerGate;

        // Lock is only taken, because #checkAndUpdateAvailability asserts it. It's a small penalty
        // for thread safety.
        synchronized (this.availableMemorySegments) {
            checkAndUpdateAvailability();
        }
    }

    // ------------------------------------------------------------------------
    // Properties
    // ------------------------------------------------------------------------

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 预留内存段
     *
     * 预留指定数量的内存段。如果预留的数量超过所需内存段的总数，则会抛出异常。
     * @param numberOfSegmentsToReserve 要预留的内存段数量
    */
    @Override
    public void reserveSegments(int numberOfSegmentsToReserve) throws IOException {
        // 检查参数是否有效，预留的内存段数量不能超过所需内存段的总数
        checkArgument(
                numberOfSegmentsToReserve <= numberOfRequiredMemorySegments,
                "Can not reserve more segments than number of required segments.");

        // 异步通知对象，用于在预留完成后通知等待线程
        CompletableFuture<?> toNotify = null;
        // 同步块，确保对availableMemorySegments的线程安全访问
        synchronized (availableMemorySegments) {
            // 检查LocalBufferPool缓冲区池是否已被销毁
            checkDestroyed();

            // 如果已请求的内存段数量小于要预留的数量
            if (numberOfRequestedMemorySegments < numberOfSegmentsToReserve) {
                // 从网络缓冲区池中阻塞式地请求剩余所需的内存段数量
                // 并将其添加到可用内存段列表中
                availableMemorySegments.addAll(
                        networkBufferPool.requestPooledMemorySegmentsBlocking(
                                numberOfSegmentsToReserve - numberOfRequestedMemorySegments));
                toNotify = availabilityHelper.getUnavailableToResetAvailable();
            }
        }
        // 获取一个用于在内存段可用时重置的异步通知对象
        mayNotifyAvailable(toNotify);
    }

    @Override
    public boolean isDestroyed() {
        synchronized (availableMemorySegments) {
            return isDestroyed;
        }
    }

    @Override
    public int getNumberOfRequiredMemorySegments() {
        return numberOfRequiredMemorySegments;
    }

    @Override
    public int getMaxNumberOfMemorySegments() {
        return maxNumberOfMemorySegments;
    }

    /**
     * Estimates the number of requested buffers.
     *
     * @return the same value as {@link #getMaxNumberOfMemorySegments()} for bounded pools. For
     *     unbounded pools it returns an approximation based upon {@link
     *     #getNumberOfRequiredMemorySegments()}
     */
    public int getEstimatedNumberOfRequestedMemorySegments() {
        if (maxNumberOfMemorySegments < NetworkBufferPool.UNBOUNDED_POOL_SIZE) {
            return maxNumberOfMemorySegments;
        } else {
            return getNumberOfRequiredMemorySegments() * 2;
        }
    }

    @VisibleForTesting
    public int getNumberOfRequestedMemorySegments() {
        synchronized (availableMemorySegments) {
            return numberOfRequestedMemorySegments;
        }
    }

    @Override
    public int getNumberOfAvailableMemorySegments() {
        synchronized (availableMemorySegments) {
            return availableMemorySegments.size();
        }
    }

    @Override
    public int getNumBuffers() {
        synchronized (availableMemorySegments) {
            return currentPoolSize;
        }
    }

    // suppress the FieldAccessNotGuarded warning as this method is unsafe by design.
    @SuppressWarnings("FieldAccessNotGuarded")
    @Override
    public int bestEffortGetNumOfUsedBuffers() {
        return Math.max(0, numberOfRequestedMemorySegments - availableMemorySegments.size());
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 请求一个缓冲区
     *
     * 调用此方法会请求一个新的内存段（Memory Segment），并将其转换为一个Buffer对象。
     * 如果请求成功，则返回该Buffer对象；如果请求失败或遇到其他问题，则返回null（或抛出异常，取决于具体实现）。
    */
    @Override
    public Buffer requestBuffer() {
        //申请的MemorySegment转换为Buffer
        return toBuffer(requestMemorySegment());
    }

    @Override
    public BufferBuilder requestBufferBuilder() {
        return toBufferBuilder(requestMemorySegment(UNKNOWN_CHANNEL), UNKNOWN_CHANNEL);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建一个针对指定目标通道的BufferBuilder对象
     * @param targetChannel 目标通道编号
     * @return 返回一个BufferBuilder对象，该对象用于在指定通道上构建缓冲区
    */
    @Override
    public BufferBuilder requestBufferBuilder(int targetChannel) {
        // 调用requestMemorySegment方法请求指定通道的内存段
        // 然后将得到的内存段以及目标通道作为参数传递给toBufferBuilder方法
        // toBufferBuilder方法将内存段转化为一个BufferBuilder对象并返回
        return toBufferBuilder(requestMemorySegment(targetChannel), targetChannel);
    }

    @Override
    public BufferBuilder requestBufferBuilderBlocking() throws InterruptedException {
        return toBufferBuilder(requestMemorySegmentBlocking(), UNKNOWN_CHANNEL);
    }

    @Override
    public MemorySegment requestMemorySegmentBlocking() throws InterruptedException {
        return requestMemorySegmentBlocking(UNKNOWN_CHANNEL);
    }

    @Override
    public BufferBuilder requestBufferBuilderBlocking(int targetChannel)
            throws InterruptedException {
        return toBufferBuilder(requestMemorySegmentBlocking(targetChannel), targetChannel);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将给定的MemorySegment转换为Buffer对象。
     *
     * @param memorySegment 要转换的MemorySegment对象。如果为null，则返回null。
     * @return 转换后的Buffer对象（类型为NetworkBuffer），如果memorySegment为null则返回null。
     */
    private Buffer toBuffer(MemorySegment memorySegment) {
        // 如果传入的MemorySegment为null，则直接返回null
        if (memorySegment == null) {
            return null;
        }
        // 使用传入的MemorySegment和当前对象（this）来创建一个新的NetworkBuffer对象
        return new NetworkBuffer(memorySegment, this);
    }

    private BufferBuilder toBufferBuilder(MemorySegment memorySegment, int targetChannel) {
        if (memorySegment == null) {
            return null;
        }

        if (targetChannel == UNKNOWN_CHANNEL) {
            return new BufferBuilder(memorySegment, this);
        } else {
            return new BufferBuilder(memorySegment, subpartitionBufferRecyclers[targetChannel]);
        }
    }

    private MemorySegment requestMemorySegmentBlocking(int targetChannel)
            throws InterruptedException {
        MemorySegment segment;
        while ((segment = requestMemorySegment(targetChannel)) == null) {
            try {
                // wait until available
                getAvailableFuture().get();
            } catch (ExecutionException e) {
                LOG.error("The available future is completed exceptionally.", e);
                ExceptionUtils.rethrow(e);
            }
        }
        return segment;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 请求指定目标通道的内存段
     *
     * @param targetChannel 目标通道编号
     * @return 返回请求到的内存段，如果无法获取则返回null
    */
    @Nullable
    private MemorySegment requestMemorySegment(int targetChannel) {
        //声明MemorySegment类型变量
        MemorySegment segment = null;
        // 同步块，确保对availableMemorySegments的线程安全访问
        synchronized (availableMemorySegments) {
            // 检查LocalBufferPool对象是否已被销毁
            checkDestroyed();
            // 如果可用内存段列表不为空
            if (!availableMemorySegments.isEmpty()) {
                // 从可用内存段列表中获取并移除一个内存段
                segment = availableMemorySegments.poll();
            } else if (isRequestedSizeReached()) {
                // Only when the buffer request reaches the upper limit(i.e. current pool size),
                // requests an overdraft buffer.
                // 如果已经到达请求的内存段上限（即当前内存池大小）
                // 则从全局中请求透支内存段
                // 仅当缓冲区请求达到上限时，请求透支缓冲区
                segment = requestOverdraftMemorySegmentFromGlobal();
            }
            // 如果segment仍然为null，则返回null
            if (segment == null) {
                return null;
            }

            // 如果目标通道不是UNKNOWN_CHANNEL
            if (targetChannel != UNKNOWN_CHANNEL) {
                // 增加对应目标通道的子分区缓冲区计数
                if (++subpartitionBuffersCount[targetChannel] == maxBuffersPerChannel) {
                    // 如果达到每个通道的最大缓冲区数，则增加不可用子分区计数
                    unavailableSubpartitionsCount++;
                }
            }
            // 检查并更新可用性状态
            checkAndUpdateAvailability();
        }
        // 返回请求到的内存段
        return segment;
    }

    @GuardedBy("availableMemorySegments")
    private void checkDestroyed() {
        if (isDestroyed) {
            throw new CancelTaskException("Buffer pool has already been destroyed.");
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 申请MemorySegment
     *
    */
    @Override
    public MemorySegment requestMemorySegment() {
        //申请MemorySegment
        return requestMemorySegment(UNKNOWN_CHANNEL);
    }

    @GuardedBy("availableMemorySegments")
    private boolean requestMemorySegmentFromGlobal() {
        assert Thread.holdsLock(availableMemorySegments);

        if (isRequestedSizeReached()) {
            return false;
        }

        MemorySegment segment = requestPooledMemorySegment();
        if (segment != null) {
            availableMemorySegments.add(segment);
            return true;
        }
        return false;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 从全局请求透支内存段
     *
     * @return 返回请求到的透支内存段，如果无法获取则返回null
     *
    */
    @GuardedBy("availableMemorySegments")
    private MemorySegment requestOverdraftMemorySegmentFromGlobal() {
        // 断言确保调用此方法时已经持有了availableMemorySegments的锁
        assert Thread.holdsLock(availableMemorySegments);

        // if overdraft buffers(i.e. buffers exceeding poolSize) is greater than or equal to
        // maxOverdraftBuffersPerGate, no new buffer can be requested.
        // 如果透支的内存段（即超过池大小的缓冲区）数量大于或等于每个门（gate）的最大透支内存段数量，
        // 则不能请求新的内存段。
        if (numberOfRequestedMemorySegments - currentPoolSize >= maxOverdraftBuffersPerGate) {
            return null;
        }
        // 请求池化的内存段
        return requestPooledMemorySegment();
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 请求池化的内存段
     * @return 返回请求到的池化内存段，如果无法获取则返回null
    */
    @Nullable
    @GuardedBy("availableMemorySegments")
    private MemorySegment requestPooledMemorySegment() {
        // 检查池是否已被销毁
        // 如果已销毁，则不应该再获取内存段，因为这会导致缓冲区泄漏
        checkState(
                !isDestroyed,
                "Destroyed buffer pools should never acquire segments - this will lead to buffer leaks.");
        // 从网络缓冲区池中请求一个池化的内存段
        MemorySegment segment = networkBufferPool.requestPooledMemorySegment();
        // 如果成功获取到内存段
        if (segment != null) {
            // 增加已请求的内存段数量
            numberOfRequestedMemorySegments++;
        }
        // 返回请求到的内存段
        return segment;
    }

    /**
     * Tries to obtain a buffer from global pool as soon as one pool is available. Note that
     * multiple {@link LocalBufferPool}s might wait on the future of the global pool, hence this
     * method double-check if a new buffer is really needed at the time it becomes available.
     */
    @GuardedBy("availableMemorySegments")
    private void requestMemorySegmentFromGlobalWhenAvailable() {
        assert Thread.holdsLock(availableMemorySegments);

        checkState(
                !requestingNotificationOfGlobalPoolAvailable,
                "local buffer pool is already in the state of requesting memory segment from global when it is available.");
        requestingNotificationOfGlobalPoolAvailable = true;
        assertNoException(
                networkBufferPool.getAvailableFuture().thenRun(this::onGlobalPoolAvailable));
    }

    private void onGlobalPoolAvailable() {
        CompletableFuture<?> toNotify;
        synchronized (availableMemorySegments) {
            requestingNotificationOfGlobalPoolAvailable = false;
            if (isDestroyed || availabilityHelper.isApproximatelyAvailable()) {
                // there is currently no benefit to obtain buffer from global; give other pools
                // precedent
                return;
            }

            // Check availability and potentially request the memory segment. The call may also
            // result in invoking
            // #requestMemorySegmentFromGlobalWhenAvailable again if no segment could be fetched
            // because of
            // concurrent requests from different LocalBufferPools.
            toNotify = checkAndUpdateAvailability();
        }
        mayNotifyAvailable(toNotify);
    }

    @GuardedBy("availableMemorySegments")
    private boolean shouldBeAvailable() {
        assert Thread.holdsLock(availableMemorySegments);

        return !availableMemorySegments.isEmpty() && unavailableSubpartitionsCount == 0;
    }

    @GuardedBy("availableMemorySegments")
    private CompletableFuture<?> checkAndUpdateAvailability() {
        assert Thread.holdsLock(availableMemorySegments);

        CompletableFuture<?> toNotify = null;

        AvailabilityStatus availabilityStatus = checkAvailability();
        if (availabilityStatus.isAvailable()) {
            toNotify = availabilityHelper.getUnavailableToResetAvailable();
        } else {
            availabilityHelper.resetUnavailable();
        }
        if (availabilityStatus.isNeedRequestingNotificationOfGlobalPoolAvailable()) {
            requestMemorySegmentFromGlobalWhenAvailable();
        }

        checkConsistentAvailability();
        return toNotify;
    }

    @GuardedBy("availableMemorySegments")
    private AvailabilityStatus checkAvailability() {
        assert Thread.holdsLock(availableMemorySegments);

        if (!availableMemorySegments.isEmpty()) {
            return AvailabilityStatus.from(shouldBeAvailable(), false);
        }
        if (isRequestedSizeReached()) {
            return AvailabilityStatus.UNAVAILABLE_NEED_NOT_REQUESTING_NOTIFICATION;
        }
        boolean needRequestingNotificationOfGlobalPoolAvailable = false;
        // There aren't availableMemorySegments, and we continue to request new memory segment from
        // global pool.
        if (!requestMemorySegmentFromGlobal()) {
            // If we can not get a buffer from global pool, we should request from it when it
            // becomes available. It should be noted that if we are already in this status, do not
            // need to repeat the request.
            needRequestingNotificationOfGlobalPoolAvailable =
                    !requestingNotificationOfGlobalPoolAvailable;
        }
        return AvailabilityStatus.from(
                shouldBeAvailable(), needRequestingNotificationOfGlobalPoolAvailable);
    }

    @GuardedBy("availableMemorySegments")
    private void checkConsistentAvailability() {
        assert Thread.holdsLock(availableMemorySegments);

        final boolean shouldBeAvailable = shouldBeAvailable();
        checkState(
                availabilityHelper.isApproximatelyAvailable() == shouldBeAvailable,
                "Inconsistent availability: expected " + shouldBeAvailable);
    }

    @Override
    public void recycle(MemorySegment segment) {
        recycle(segment, UNKNOWN_CHANNEL);
    }

    private void recycle(MemorySegment segment, int channel) {
        BufferListener listener;
        CompletableFuture<?> toNotify = null;
        do {
            synchronized (availableMemorySegments) {
                if (channel != UNKNOWN_CHANNEL) {
                    if (subpartitionBuffersCount[channel]-- == maxBuffersPerChannel) {
                        unavailableSubpartitionsCount--;
                    }
                }

                if (isDestroyed || hasExcessBuffers()) {
                    returnMemorySegment(segment);
                    return;
                } else {
                    listener = registeredListeners.poll();
                    if (listener == null) {
                        availableMemorySegments.add(segment);
                        if (!availabilityHelper.isApproximatelyAvailable() && shouldBeAvailable()) {
                            toNotify = availabilityHelper.getUnavailableToResetAvailable();
                        }
                        break;
                    }
                }

                checkConsistentAvailability();
            }
        } while (!fireBufferAvailableNotification(listener, segment));

        mayNotifyAvailable(toNotify);
    }

    private boolean fireBufferAvailableNotification(
            BufferListener listener, MemorySegment segment) {
        // We do not know which locks have been acquired before the recycle() or are needed in the
        // notification and which other threads also access them.
        // -> call notifyBufferAvailable() outside the synchronized block to avoid a deadlock
        // (FLINK-9676)
        return listener.notifyBufferAvailable(new NetworkBuffer(segment, this));
    }

    /** Destroy is called after the produce or consume phase of a task finishes. */
    @Override
    public void lazyDestroy() {
        // NOTE: if you change this logic, be sure to update recycle() as well!
        CompletableFuture<?> toNotify = null;
        synchronized (availableMemorySegments) {
            if (!isDestroyed) {
                MemorySegment segment;
                while ((segment = availableMemorySegments.poll()) != null) {
                    returnMemorySegment(segment);
                }

                BufferListener listener;
                while ((listener = registeredListeners.poll()) != null) {
                    listener.notifyBufferDestroyed();
                }

                if (!isAvailable()) {
                    toNotify = availabilityHelper.getAvailableFuture();
                }

                isDestroyed = true;
            }
        }

        mayNotifyAvailable(toNotify);

        networkBufferPool.destroyBufferPool(this);
    }

    @Override
    public boolean addBufferListener(BufferListener listener) {
        synchronized (availableMemorySegments) {
            if (!availableMemorySegments.isEmpty() || isDestroyed) {
                return false;
            }

            registeredListeners.add(listener);
            return true;
        }
    }

    @Override
    public void setNumBuffers(int numBuffers) {
        CompletableFuture<?> toNotify;
        synchronized (availableMemorySegments) {
            checkArgument(
                    numBuffers >= numberOfRequiredMemorySegments,
                    "Buffer pool needs at least %s buffers, but tried to set to %s",
                    numberOfRequiredMemorySegments,
                    numBuffers);

            currentPoolSize = Math.min(numBuffers, maxNumberOfMemorySegments);

            returnExcessMemorySegments();

            if (isDestroyed) {
                // FLINK-19964: when two local buffer pools are released concurrently, one of them
                // gets buffers assigned
                // make sure that checkAndUpdateAvailability is not called as it would proactively
                // acquire one buffer from NetworkBufferPool.
                return;
            }

            toNotify = checkAndUpdateAvailability();
        }

        mayNotifyAvailable(toNotify);
    }

    public void setMaxOverdraftBuffersPerGate(int maxOverdraftBuffersPerGate) {
        this.maxOverdraftBuffersPerGate = maxOverdraftBuffersPerGate;
    }

    public int getMaxOverdraftBuffersPerGate() {
        return maxOverdraftBuffersPerGate;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return availabilityHelper.getAvailableFuture();
    }

    @Override
    public String toString() {
        synchronized (availableMemorySegments) {
            return String.format(
                    "[size: %d, required: %d, requested: %d, available: %d, max: %d, listeners: %d,"
                            + "subpartitions: %d, maxBuffersPerChannel: %d, destroyed: %s]",
                    currentPoolSize,
                    numberOfRequiredMemorySegments,
                    numberOfRequestedMemorySegments,
                    availableMemorySegments.size(),
                    maxNumberOfMemorySegments,
                    registeredListeners.size(),
                    subpartitionBuffersCount.length,
                    maxBuffersPerChannel,
                    isDestroyed);
        }
    }

    // ------------------------------------------------------------------------

    /**
     * Notifies the potential segment consumer of the new available segments by completing the
     * previous uncompleted future.
     */
    private void mayNotifyAvailable(@Nullable CompletableFuture<?> toNotify) {
        if (toNotify != null) {
            toNotify.complete(null);
        }
    }

    @GuardedBy("availableMemorySegments")
    private void returnMemorySegment(MemorySegment segment) {
        assert Thread.holdsLock(availableMemorySegments);

        numberOfRequestedMemorySegments--;
        networkBufferPool.recyclePooledMemorySegment(segment);
    }

    @GuardedBy("availableMemorySegments")
    private void returnExcessMemorySegments() {
        assert Thread.holdsLock(availableMemorySegments);

        while (hasExcessBuffers()) {
            MemorySegment segment = availableMemorySegments.poll();
            if (segment == null) {
                return;
            }

            returnMemorySegment(segment);
        }
    }

    @GuardedBy("availableMemorySegments")
    private boolean hasExcessBuffers() {
        return numberOfRequestedMemorySegments > currentPoolSize;
    }

    @GuardedBy("availableMemorySegments")
    private boolean isRequestedSizeReached() {
        return numberOfRequestedMemorySegments >= currentPoolSize;
    }

    private static class SubpartitionBufferRecycler implements BufferRecycler {

        private final int channel;
        private final LocalBufferPool bufferPool;

        SubpartitionBufferRecycler(int channel, LocalBufferPool bufferPool) {
            this.channel = channel;
            this.bufferPool = bufferPool;
        }

        @Override
        public void recycle(MemorySegment memorySegment) {
            bufferPool.recycle(memorySegment, channel);
        }
    }

    /**
     * This class represents the buffer pool's current ground-truth availability and whether to
     * request buffer from global pool when it is available.
     */
    private enum AvailabilityStatus {
        AVAILABLE(true, false),
        UNAVAILABLE_NEED_REQUESTING_NOTIFICATION(false, true),
        UNAVAILABLE_NEED_NOT_REQUESTING_NOTIFICATION(false, false);

        /** Indicates whether the {@link LocalBufferPool} is currently available. */
        private final boolean available;

        /**
         * Indicates whether to requesting notification of global pool when it becomes available.
         */
        private final boolean needRequestingNotificationOfGlobalPoolAvailable;

        AvailabilityStatus(
                boolean available, boolean needRequestingNotificationOfGlobalPoolAvailable) {
            this.available = available;
            this.needRequestingNotificationOfGlobalPoolAvailable =
                    needRequestingNotificationOfGlobalPoolAvailable;
        }

        public boolean isAvailable() {
            return available;
        }

        public boolean isNeedRequestingNotificationOfGlobalPoolAvailable() {
            return needRequestingNotificationOfGlobalPoolAvailable;
        }

        public static AvailabilityStatus from(
                boolean available, boolean needRequestingNotificationOfGlobalPoolAvailable) {
            if (available) {
                checkState(
                        !needRequestingNotificationOfGlobalPoolAvailable,
                        "available local buffer pool should not request from global.");
                return AVAILABLE;
            } else if (needRequestingNotificationOfGlobalPoolAvailable) {
                return UNAVAILABLE_NEED_REQUESTING_NOTIFICATION;
            } else {
                return UNAVAILABLE_NEED_NOT_REQUESTING_NOTIFICATION;
            }
        }
    }
}
