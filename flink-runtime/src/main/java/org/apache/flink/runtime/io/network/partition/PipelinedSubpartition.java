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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumerWithPartialRecordLength;
import org.apache.flink.runtime.io.network.logger.NetworkActionsLogger;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pipelined in-memory only subpartition, which can be consumed once.
 *
 * <p>Whenever {@link ResultSubpartition#add(BufferConsumer)} adds a finished {@link BufferConsumer}
 * or a second {@link BufferConsumer} (in which case we will assume the first one finished), we will
 * {@link PipelinedSubpartitionView#notifyDataAvailable() notify} a read view created via {@link
 * ResultSubpartition#createReadView(BufferAvailabilityListener)} of new data availability. Except
 * by calling {@link #flush()} explicitly, we always only notify when the first finished buffer
 * turns up and then, the reader has to drain the buffers via {@link #pollBuffer()} until its return
 * value shows no more buffers being available. This results in a buffer queue which is either empty
 * or has an unfinished {@link BufferConsumer} left from which the notifications will eventually
 * start again.
 *
 * <p>Explicit calls to {@link #flush()} will force this {@link
 * PipelinedSubpartitionView#notifyDataAvailable() notification} for any {@link BufferConsumer}
 * present in the queue.
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 一种只在内存中使用流水线的子分区，可以使用一次。
*/
public class PipelinedSubpartition extends ResultSubpartition implements ChannelStateHolder {

    private static final Logger LOG = LoggerFactory.getLogger(PipelinedSubpartition.class);

    private static final int DEFAULT_PRIORITY_SEQUENCE_NUMBER = -1;

    // ------------------------------------------------------------------------

    /**
     * Number of exclusive credits per input channel at the downstream tasks configured by {@link
     * org.apache.flink.configuration.NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_PER_CHANNEL}.
     */
    private final int receiverExclusiveBuffersPerChannel;

    /** All buffers of this subpartition. Access to the buffers is synchronized on this object. */
    /** 此子分区的所有缓冲区。对缓冲区的访问在此对象上同步。 */
    final PrioritizedDeque<BufferConsumerWithPartialRecordLength> buffers =
            new PrioritizedDeque<>();

    /** The number of non-event buffers currently in this subpartition. */
    @GuardedBy("buffers")
    private int buffersInBacklog;

    /** The read view to consume this subpartition. */
    PipelinedSubpartitionView readView;

    /** Flag indicating whether the subpartition has been finished. */
    private boolean isFinished;

    @GuardedBy("buffers")
    private boolean flushRequested;

    /** Flag indicating whether the subpartition has been released. */
    volatile boolean isReleased;

    /** The total number of buffers (both data and event buffers). */
    private long totalNumberOfBuffers;

    /** The total number of bytes (both data and event buffers). */
    private long totalNumberOfBytes;

    /** Writes in-flight data. */
    private ChannelStateWriter channelStateWriter;

    private int bufferSize = Integer.MAX_VALUE;

    /** The channelState Future of unaligned checkpoint. */
    @GuardedBy("buffers")
    private CompletableFuture<List<Buffer>> channelStateFuture;

    /**
     * It is the checkpointId corresponding to channelStateFuture. And It should be always update
     * with {@link #channelStateFuture}.
     */
    @GuardedBy("buffers")
    private long channelStateCheckpointId;

    /**
     * Whether this subpartition is blocked (e.g. by exactly once checkpoint) and is waiting for
     * resumption.
     */
    @GuardedBy("buffers")
    boolean isBlocked = false;

    int sequenceNumber = 0;

    // ------------------------------------------------------------------------

    PipelinedSubpartition(
            int index, int receiverExclusiveBuffersPerChannel, ResultPartition parent) {
        super(index, parent);

        checkArgument(
                receiverExclusiveBuffersPerChannel >= 0,
                "Buffers per channel must be non-negative.");
        this.receiverExclusiveBuffersPerChannel = receiverExclusiveBuffersPerChannel;
    }

    @Override
    public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
        checkState(this.channelStateWriter == null, "Already initialized");
        this.channelStateWriter = checkNotNull(channelStateWriter);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 调用方法重载将BufferConsumer对象添加到队列
    */
    @Override
    public int add(BufferConsumer bufferConsumer, int partialRecordLength) {
        //调用内部add方法将来BufferConsumer放入队列中
        return add(bufferConsumer, partialRecordLength, false);
    }

    public boolean isSupportChannelStateRecover() {
        return true;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 标记数据分区消费完成并返回已写入的字节数。
    */
    @Override
    public int finish() throws IOException {
        // 创建一个用于写入分区结束事件的BufferConsumer
        BufferConsumer eventBufferConsumer =
                EventSerializer.toBufferConsumer(EndOfPartitionEvent.INSTANCE, false);
        // 调用add方法，将序列化后的EndOfPartitionEvent添加到某个地方（可能是队列或缓冲区）
        add(eventBufferConsumer, 0, true);
        //打印日志
        LOG.debug("{}: Finished {}.", parent.getOwningTaskName(), this);
        // 返回BufferConsumer中已写入的字节数
        return eventBufferConsumer.getWrittenBytes();
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 向某个缓冲区消费者添加数据，并返回新的缓冲区大小
    */
    private int add(BufferConsumer bufferConsumer, int partialRecordLength, boolean finish) {
        // 检查缓冲区消费者是否为空，如果为空则抛出异常
        checkNotNull(bufferConsumer);
        // 初始化一个变量，用于标识是否需要通知数据可用
        final boolean notifyDataAvailable;
        // 默认优先级序列号
        int prioritySequenceNumber = DEFAULT_PRIORITY_SEQUENCE_NUMBER;
        // 新的缓冲区大小
        int newBufferSize;
        // 使用buffers对象作为锁，确保线程安全
        synchronized (buffers) {
            // 如果当前操作已完成或已释放，则关闭缓冲区消费者并返回错误码
            if (isFinished || isReleased) {
                bufferConsumer.close();
                return ADD_BUFFER_ERROR_CODE;
            }

            // Add the bufferConsumer and update the stats
            // 尝试将缓冲区消费者添加到缓冲区，并更新统计信息
            // 如果添加成功，则更新优先级序列号
            if (addBuffer(bufferConsumer, partialRecordLength)) {
                prioritySequenceNumber = sequenceNumber;
            }
            // 更新缓冲区消费者的统计信息
            updateStatistics(bufferConsumer);
            // 增加在队列中等待的缓冲区数量
            increaseBuffersInBacklog(bufferConsumer);
            // 根据是否完成或应该通知数据可用，设置notifyDataAvailable的值
            notifyDataAvailable = finish || shouldNotifyDataAvailable();

            // 如果finish为true，则设置isFinished为true
            isFinished |= finish;
            // 获取当前缓冲区的大小
            newBufferSize = bufferSize;
        }
        // 通知优先级事件
        notifyPriorityEvent(prioritySequenceNumber);
        // 如果需要通知数据可用，则执行通知操作
        if (notifyDataAvailable) {
            notifyDataAvailable();
        }
        // 返回新的缓冲区大小
        return newBufferSize;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 方法用于向缓冲区中添加一个BufferConsumer对象，并可能处理具有特殊数据类型的BufferConsumer
    */
    @GuardedBy("buffers")
    private boolean addBuffer(BufferConsumer bufferConsumer, int partialRecordLength) {
        // 断言当前线程是否持有buffers对象的锁，确保线程安全
        assert Thread.holdsLock(buffers);
        // 检查BufferConsumer的数据类型是否具有优先级
        if (bufferConsumer.getDataType().hasPriority()) {
            // 如果数据类型具有优先级，则调用processPriorityBuffer方法来处理
            return processPriorityBuffer(bufferConsumer, partialRecordLength);
            // 如果数据类型不是优先级类型，但是是TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER类型
        } else if (Buffer.DataType.TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER
                == bufferConsumer.getDataType()) {
            // 调用processTimeoutableCheckpointBarrier方法来处理这种特殊类型的BufferConsumer
            processTimeoutableCheckpointBarrier(bufferConsumer);
        }
        // 如果BufferConsumer的数据类型既不是优先级类型也不是TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER类型
        // 则将其与partialRecordLength一起封装为一个新的BufferConsumerWithPartialRecordLength对象，并添加到buffers列表中
        buffers.add(new BufferConsumerWithPartialRecordLength(bufferConsumer, partialRecordLength));
        return false;
    }

    @GuardedBy("buffers")
    private boolean processPriorityBuffer(BufferConsumer bufferConsumer, int partialRecordLength) {
        buffers.addPriorityElement(
                new BufferConsumerWithPartialRecordLength(bufferConsumer, partialRecordLength));
        final int numPriorityElements = buffers.getNumPriorityElements();

        CheckpointBarrier barrier = parseCheckpointBarrier(bufferConsumer);
        if (barrier != null) {
            checkState(
                    barrier.getCheckpointOptions().isUnalignedCheckpoint(),
                    "Only unaligned checkpoints should be priority events");
            final Iterator<BufferConsumerWithPartialRecordLength> iterator = buffers.iterator();
            Iterators.advance(iterator, numPriorityElements);
            List<Buffer> inflightBuffers = new ArrayList<>();
            while (iterator.hasNext()) {
                BufferConsumer buffer = iterator.next().getBufferConsumer();

                if (buffer.isBuffer()) {
                    try (BufferConsumer bc = buffer.copy()) {
                        inflightBuffers.add(bc.build());
                    }
                }
            }
            if (!inflightBuffers.isEmpty()) {
                channelStateWriter.addOutputData(
                        barrier.getId(),
                        subpartitionInfo,
                        ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
                        inflightBuffers.toArray(new Buffer[0]));
            }
        }
        return needNotifyPriorityEvent();
    }

    // It is just called after add priorityEvent.
    @GuardedBy("buffers")
    private boolean needNotifyPriorityEvent() {
        assert Thread.holdsLock(buffers);
        // if subpartition is blocked then downstream doesn't expect any notifications
        return buffers.getNumPriorityElements() == 1 && !isBlocked;
    }

    @GuardedBy("buffers")
    private void processTimeoutableCheckpointBarrier(BufferConsumer bufferConsumer) {
        CheckpointBarrier barrier = parseAndCheckTimeoutableCheckpointBarrier(bufferConsumer);
        channelStateWriter.addOutputDataFuture(
                barrier.getId(),
                subpartitionInfo,
                ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
                createChannelStateFuture(barrier.getId()));
    }

    @GuardedBy("buffers")
    private CompletableFuture<List<Buffer>> createChannelStateFuture(long checkpointId) {
        assert Thread.holdsLock(buffers);
        if (channelStateFuture != null) {
            completeChannelStateFuture(
                    null,
                    new IllegalStateException(
                            String.format(
                                    "%s has uncompleted channelStateFuture of checkpointId=%s, but it received "
                                            + "a new timeoutable checkpoint barrier of checkpointId=%s, it maybe "
                                            + "a bug due to currently not supported concurrent unaligned checkpoint.",
                                    this, channelStateCheckpointId, checkpointId)));
        }
        channelStateFuture = new CompletableFuture<>();
        channelStateCheckpointId = checkpointId;
        return channelStateFuture;
    }

    @GuardedBy("buffers")
    private void completeChannelStateFuture(List<Buffer> channelResult, Throwable e) {
        assert Thread.holdsLock(buffers);
        if (e != null) {
            channelStateFuture.completeExceptionally(e);
        } else {
            channelStateFuture.complete(channelResult);
        }
        channelStateFuture = null;
    }

    @GuardedBy("buffers")
    private boolean isChannelStateFutureAvailable(long checkpointId) {
        assert Thread.holdsLock(buffers);
        return channelStateFuture != null && channelStateCheckpointId == checkpointId;
    }

    private CheckpointBarrier parseAndCheckTimeoutableCheckpointBarrier(
            BufferConsumer bufferConsumer) {
        CheckpointBarrier barrier = parseCheckpointBarrier(bufferConsumer);
        checkArgument(barrier != null, "Parse the timeoutable Checkpoint Barrier failed.");
        checkState(
                barrier.getCheckpointOptions().isTimeoutable()
                        && Buffer.DataType.TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER
                                == bufferConsumer.getDataType());
        return barrier;
    }

    @Override
    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        int prioritySequenceNumber = DEFAULT_PRIORITY_SEQUENCE_NUMBER;
        synchronized (buffers) {
            // The checkpoint barrier has sent to downstream, so nothing to do.
            if (!isChannelStateFutureAvailable(checkpointId)) {
                return;
            }

            // 1. find inflightBuffers and timeout the aligned barrier to unaligned barrier
            List<Buffer> inflightBuffers = new ArrayList<>();
            try {
                if (findInflightBuffersAndMakeBarrierToPriority(checkpointId, inflightBuffers)) {
                    prioritySequenceNumber = sequenceNumber;
                }
            } catch (IOException e) {
                inflightBuffers.forEach(Buffer::recycleBuffer);
                completeChannelStateFuture(null, e);
                throw e;
            }

            // 2. complete the channelStateFuture
            completeChannelStateFuture(inflightBuffers, null);
        }

        // 3. notify downstream read barrier, it must be called outside the buffers_lock to avoid
        // the deadlock.
        notifyPriorityEvent(prioritySequenceNumber);
    }

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        synchronized (buffers) {
            if (isChannelStateFutureAvailable(checkpointId)) {
                completeChannelStateFuture(null, cause);
            }
        }
    }

    @GuardedBy("buffers")
    private boolean findInflightBuffersAndMakeBarrierToPriority(
            long checkpointId, List<Buffer> inflightBuffers) throws IOException {
        // 1. record the buffers before barrier as inflightBuffers
        final int numPriorityElements = buffers.getNumPriorityElements();
        final Iterator<BufferConsumerWithPartialRecordLength> iterator = buffers.iterator();
        Iterators.advance(iterator, numPriorityElements);

        BufferConsumerWithPartialRecordLength element = null;
        CheckpointBarrier barrier = null;
        while (iterator.hasNext()) {
            BufferConsumerWithPartialRecordLength next = iterator.next();
            BufferConsumer bufferConsumer = next.getBufferConsumer();

            if (Buffer.DataType.TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER
                    == bufferConsumer.getDataType()) {
                barrier = parseAndCheckTimeoutableCheckpointBarrier(bufferConsumer);
                // It may be an aborted barrier
                if (barrier.getId() != checkpointId) {
                    continue;
                }
                element = next;
                break;
            } else if (bufferConsumer.isBuffer()) {
                try (BufferConsumer bc = bufferConsumer.copy()) {
                    inflightBuffers.add(bc.build());
                }
            }
        }

        // 2. Make the barrier to be priority
        checkNotNull(
                element, "The checkpoint barrier=%d don't find in %s.", checkpointId, toString());
        makeBarrierToPriority(element, barrier);

        return needNotifyPriorityEvent();
    }

    private void makeBarrierToPriority(
            BufferConsumerWithPartialRecordLength oldElement, CheckpointBarrier barrier)
            throws IOException {
        buffers.getAndRemove(oldElement::equals);
        buffers.addPriorityElement(
                new BufferConsumerWithPartialRecordLength(
                        EventSerializer.toBufferConsumer(barrier.asUnaligned(), true), 0));
    }

    @Nullable
    private CheckpointBarrier parseCheckpointBarrier(BufferConsumer bufferConsumer) {
        CheckpointBarrier barrier;
        try (BufferConsumer bc = bufferConsumer.copy()) {
            Buffer buffer = bc.build();
            try {
                final AbstractEvent event =
                        EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
                barrier = event instanceof CheckpointBarrier ? (CheckpointBarrier) event : null;
            } catch (IOException e) {
                throw new IllegalStateException(
                        "Should always be able to deserialize in-memory event", e);
            } finally {
                buffer.recycleBuffer();
            }
        }
        return barrier;
    }

    @Override
    public void release() {
        // view reference accessible outside the lock, but assigned inside the locked scope
        final PipelinedSubpartitionView view;

        synchronized (buffers) {
            if (isReleased) {
                return;
            }

            // Release all available buffers
            for (BufferConsumerWithPartialRecordLength buffer : buffers) {
                buffer.getBufferConsumer().close();
            }
            buffers.clear();

            if (channelStateFuture != null) {
                IllegalStateException exception =
                        new IllegalStateException("The PipelinedSubpartition is released");
                completeChannelStateFuture(null, exception);
            }

            view = readView;
            readView = null;

            // Make sure that no further buffers are added to the subpartition
            isReleased = true;
        }

        LOG.debug("{}: Released {}.", parent.getOwningTaskName(), this);

        if (view != null) {
            view.releaseAllResources();
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 从缓冲区队列中轮询获取一个Buffer
    */
    @Nullable
    BufferAndBacklog pollBuffer() {
        // 同步块，确保在多线程环境下对buffers的访问是线程安全的
        synchronized (buffers) {
            // 如果当前线程被阻塞，则直接返回null
            if (isBlocked) {
                return null;
            }

            Buffer buffer = null;
            // 如果缓冲区队列为空，则清除flushRequested标志
            if (buffers.isEmpty()) {
                flushRequested = false;
            }
            // 当缓冲区队列不为空时，循环检查队列中的每个BufferConsumer
            while (!buffers.isEmpty()) {
                // 从队列头部取出一个包含部分记录长度的BufferConsumer
                BufferConsumerWithPartialRecordLength bufferConsumerWithPartialRecordLength =
                        buffers.peek();
                BufferConsumer bufferConsumer =
                        bufferConsumerWithPartialRecordLength.getBufferConsumer();
                // 如果数据类型是TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER，则完成相关的超时检查点屏障
                if (Buffer.DataType.TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER
                        == bufferConsumer.getDataType()) {
                    completeTimeoutableCheckpointBarrier(bufferConsumer);
                }
                // 从BufferConsumer中构建并获取一个SliceBuffer
                buffer = buildSliceBuffer(bufferConsumerWithPartialRecordLength);
                // 检查当前BufferConsumer是否已完成处理或队列中只剩下一个Buffer
                // 如果不是，则抛出异常
                checkState(
                        bufferConsumer.isFinished() || buffers.size() == 1,
                        "When there are multiple buffers, an unfinished bufferConsumer can not be at the head of the buffers queue.");
                // 如果队列中只剩下一个Buffer，则关闭flushRequested标志
                if (buffers.size() == 1) {
                    // turn off flushRequested flag if we drained all the available data
                    // 如果我们排空了所有可用的数据，则关闭flushRequested标志
                    flushRequested = false;
                }
                // 如果BufferConsumer已完成，则关闭并移除它，同时更新backlog中的缓冲区数量
                if (bufferConsumer.isFinished()) {
                    requireNonNull(buffers.poll()).getBufferConsumer().close();
                    decreaseBuffersInBacklogUnsafe(bufferConsumer.isBuffer());
                }

                // if we have an empty finished buffer and the exclusive credit is 0, we just return
                // the empty buffer so that the downstream task can release the allocated credit for
                // this empty buffer, this happens in two main scenarios currently:
                // 1. all data of a buffer builder has been read and after that the buffer builder
                // is finished
                // 2. in approximate recovery mode, a partial record takes a whole buffer builder
                // 如果我们有一个空的已完成的Buffer，并且独占的credit为0，我们直接返回这个空的Buffer
                // 这样下游任务可以释放为这个空Buffer分配的credit
                // 目前这主要发生在以下两种情况下：
                // 1. 一个buffer builder的所有数据都已被读取，并且之后buffer builder被标记为已完成
                // 2. 在近似恢复模式下，一个partialRecord占用了整个buffer builder
                if (receiverExclusiveBuffersPerChannel == 0 && bufferConsumer.isFinished()) {
                    break;
                }
                // 如果当前Buffer有可读数据，则跳出循环
                if (buffer.readableBytes() > 0) {
                    break;
                }
                // 如果没有可读数据，回收当前Buffer并重置为null
                buffer.recycleBuffer();
                buffer = null;
                // 如果BufferConsumer未完成，也跳出循环
                if (!bufferConsumer.isFinished()) {
                    break;
                }
            }
            // 如果没有找到有效的Buffer，则返回null
            if (buffer == null) {
                return null;
            }
            // 如果Buffer的数据类型会阻塞上游，则设置阻塞标志
            if (buffer.getDataType().isBlockingUpstream()) {
                isBlocked = true;
            }
            // 更新统计信息
            updateStatistics(buffer);
            // Do not report last remaining buffer on buffers as available to read (assuming it's
            // unfinished).
            // It will be reported for reading either on flush or when the number of buffers in the
            // queue
            // will be 2 or more.
            NetworkActionsLogger.traceOutput(
                    "PipelinedSubpartition#pollBuffer",
                    buffer,
                    parent.getOwningTaskName(),
                    subpartitionInfo);
            //返回BufferAndBacklog
            return new BufferAndBacklog(
                    buffer,
                    getBuffersInBacklogUnsafe(),
                    isDataAvailableUnsafe() ? getNextBufferTypeUnsafe() : Buffer.DataType.NONE,
                    sequenceNumber++);
        }
    }

    @GuardedBy("buffers")
    private void completeTimeoutableCheckpointBarrier(BufferConsumer bufferConsumer) {
        CheckpointBarrier barrier = parseAndCheckTimeoutableCheckpointBarrier(bufferConsumer);
        if (!isChannelStateFutureAvailable(barrier.getId())) {
            // It happens on a previously aborted checkpoint.
            return;
        }
        completeChannelStateFuture(Collections.emptyList(), null);
    }

    void resumeConsumption() {
        synchronized (buffers) {
            checkState(isBlocked, "Should be blocked by checkpoint.");

            isBlocked = false;
        }
    }

    public void acknowledgeAllDataProcessed() {
        parent.onSubpartitionAllDataProcessed(subpartitionInfo.getSubPartitionIdx());
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建一个PipelinedSubpartitionView对象，该对象用于读取此子分区的数据。
     * 这个方法只能在子分区未被释放且尚未被消费的情况下调用。
     * Pipelined子分区只能被消费一次。
     *
     * @param availabilityListener 缓冲区可用性监听器
     * @return PipelinedSubpartitionView对象，表示该子分区的读取视图
     * @throws IllegalStateException 如果子分区已经被释放或已被消费
    */
    @Override
    public PipelinedSubpartitionView createReadView(
            BufferAvailabilityListener availabilityListener) {
        // 使用buffers对象作为锁，确保线程安全
        synchronized (buffers) {
            // 检查子分区是否已经被释放
            checkState(!isReleased);
            checkState(
                    readView == null,
                    "Subpartition %s of is being (or already has been) consumed, "
                            + "but pipelined subpartitions can only be consumed once.",
                    getSubPartitionIndex(),
                    parent.getPartitionId());

            LOG.debug(
                    "{}: Creating read view for subpartition {} of partition {}.",
                    parent.getOwningTaskName(),
                    getSubPartitionIndex(),
                    parent.getPartitionId());
            // 创建一个新的PipelinedSubpartitionView对象，并将当前对象和监听器作为参数传入
            readView = new PipelinedSubpartitionView(this, availabilityListener);
        }
        // 返回创建的读取视图
        return readView;
    }

    public ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog(
            boolean isCreditAvailable) {
        synchronized (buffers) {
            boolean isAvailable;
            if (isCreditAvailable) {
                isAvailable = isDataAvailableUnsafe();
            } else {
                isAvailable = getNextBufferTypeUnsafe().isEvent();
            }
            return new ResultSubpartitionView.AvailabilityWithBacklog(
                    isAvailable, getBuffersInBacklogUnsafe());
        }
    }

    @GuardedBy("buffers")
    private boolean isDataAvailableUnsafe() {
        assert Thread.holdsLock(buffers);

        return !isBlocked && (flushRequested || getNumberOfFinishedBuffers() > 0);
    }

    private Buffer.DataType getNextBufferTypeUnsafe() {
        assert Thread.holdsLock(buffers);

        final BufferConsumerWithPartialRecordLength first = buffers.peek();
        return first != null ? first.getBufferConsumer().getDataType() : Buffer.DataType.NONE;
    }

    // ------------------------------------------------------------------------

    @Override
    public int getNumberOfQueuedBuffers() {
        synchronized (buffers) {
            return buffers.size();
        }
    }

    @Override
    public void bufferSize(int desirableNewBufferSize) {
        if (desirableNewBufferSize < 0) {
            throw new IllegalArgumentException("New buffer size can not be less than zero");
        }
        synchronized (buffers) {
            bufferSize = desirableNewBufferSize;
        }
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        final long numBuffers;
        final long numBytes;
        final boolean finished;
        final boolean hasReadView;

        synchronized (buffers) {
            numBuffers = getTotalNumberOfBuffersUnsafe();
            numBytes = getTotalNumberOfBytesUnsafe();
            finished = isFinished;
            hasReadView = readView != null;
        }

        return String.format(
                "%s#%d [number of buffers: %d (%d bytes), number of buffers in backlog: %d, finished? %s, read view? %s]",
                this.getClass().getSimpleName(),
                getSubPartitionIndex(),
                numBuffers,
                numBytes,
                getBuffersInBacklogUnsafe(),
                finished,
                hasReadView);
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        // since we do not synchronize, the size may actually be lower than 0!
        return Math.max(buffers.size(), 0);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 刷新缓冲区中的数据。
     *
     * 此方法尝试刷新缓冲区中的数据，如果条件满足，会通知读取者数据可用。
     * 使用synchronized块同步对buffers的访问，确保线程安全。
    */
    @Override
    public void flush() {
        final boolean notifyDataAvailable;
        synchronized (buffers) {
            // 如果缓冲区为空或者已经请求过刷新，则直接返回
            if (buffers.isEmpty() || flushRequested) {
                return;
            }
            // if there is more than 1 buffer, we already notified the reader
            // (at the latest when adding the second buffer)
            // 如果缓冲区中只有一个，并且这个缓冲区的数据对于消费者来说是可用的
            // 我们认为存在未完成的缓冲区中有数据可用（因为添加第二个缓冲区时，我们已经通知过读取者）
            boolean isDataAvailableInUnfinishedBuffer =
                    buffers.size() == 1 && buffers.peek().getBufferConsumer().isDataAvailable();
            notifyDataAvailable = !isBlocked && isDataAvailableInUnfinishedBuffer;
            flushRequested = buffers.size() > 1 || isDataAvailableInUnfinishedBuffer;
        }
        // 通知读取者数据可用，如果未阻塞并且存在未完成的缓冲区中有数据可用
        if (notifyDataAvailable) {
            // 如果缓冲区中有多个数据或者未完成的缓冲区中有数据可用，则标记为已请求刷新
            notifyDataAvailable();
        }
    }

    @Override
    protected long getTotalNumberOfBuffersUnsafe() {
        return totalNumberOfBuffers;
    }

    @Override
    protected long getTotalNumberOfBytesUnsafe() {
        return totalNumberOfBytes;
    }

    Throwable getFailureCause() {
        return parent.getFailureCause();
    }

    private void updateStatistics(BufferConsumer buffer) {
        totalNumberOfBuffers++;
    }

    private void updateStatistics(Buffer buffer) {
        totalNumberOfBytes += buffer.getSize();
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 减少待处理缓冲区（backlog）的数量
    */
    @GuardedBy("buffers")
    private void decreaseBuffersInBacklogUnsafe(boolean isBuffer) {
        // 断言，确保当前线程持有buffers对象的锁，如果没有则抛出AssertionError
        assert Thread.holdsLock(buffers);
        // 如果传入的isBuffer参数为true
        if (isBuffer) {
            // 则将buffersInBacklog变量（可能是表示待处理缓冲区数量的计数器）减一
            buffersInBacklog--;
        }
    }

    /**
     * Increases the number of non-event buffers by one after adding a non-event buffer into this
     * subpartition.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 1.将非事件缓冲区添加到此子分区后
     * 2.将非事件缓冲器的数量增加一。
    */
    @GuardedBy("buffers")
    private void increaseBuffersInBacklog(BufferConsumer buffer) {
        //确保获取的buffers队列的对象锁
        assert Thread.holdsLock(buffers);
        //判断数据是否是缓冲，也就是非事件类型
        if (buffer != null && buffer.isBuffer()) {
            //数量加1
            buffersInBacklog++;
        }
    }

    /** Gets the number of non-event buffers in this subpartition. */
    @SuppressWarnings("FieldAccessNotGuarded")
    @Override
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 获取此子分区中非事件缓冲区的数量。
    */
    public int getBuffersInBacklogUnsafe() {
        // 如果子分区被阻塞或者缓冲区队列为空，则返回0
        if (isBlocked || buffers.isEmpty()) {
            return 0;
        }
        // 如果以下任一条件为真，则返回buffersInBacklog的当前值：
        // 1. 刷新请求被触发
        // 2. 子分区已完成
        // 3. 队列尾部的缓冲区不是事件缓冲区（即非事件缓冲区）
        if (flushRequested
                || isFinished
                || !checkNotNull(buffers.peekLast()).getBufferConsumer().isBuffer()) {
            //直接返回数量
            return buffersInBacklog;
        } else {
            // 否则，如果buffersInBacklog大于0，则返回buffersInBacklog减1的值，
            // 因为当前队列尾部的缓冲区（假设是事件缓冲区）不应计入非事件缓冲区的数量
            // 如果buffersInBacklog小于或等于0，则返回0
            return Math.max(buffersInBacklog - 1, 0);
        }
    }

    @GuardedBy("buffers")
    private boolean shouldNotifyDataAvailable() {
        // Notify only when we added first finished buffer.
        return readView != null
                && !flushRequested
                && !isBlocked
                && getNumberOfFinishedBuffers() == 1;
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 通知数据可用
    */
    private void notifyDataAvailable() {
        final PipelinedSubpartitionView readView = this.readView;
        if (readView != null) {
            // 如果读取视图不为空，则通知其数据已经可用
            readView.notifyDataAvailable();
        }
    }

    private void notifyPriorityEvent(int prioritySequenceNumber) {
        final PipelinedSubpartitionView readView = this.readView;
        if (readView != null && prioritySequenceNumber != DEFAULT_PRIORITY_SEQUENCE_NUMBER) {
            readView.notifyPriorityEvent(prioritySequenceNumber);
        }
    }

    private int getNumberOfFinishedBuffers() {
        assert Thread.holdsLock(buffers);

        // NOTE: isFinished() is not guaranteed to provide the most up-to-date state here
        // worst-case: a single finished buffer sits around until the next flush() call
        // (but we do not offer stronger guarantees anyway)
        final int numBuffers = buffers.size();
        if (numBuffers == 1 && buffers.peekLast().getBufferConsumer().isFinished()) {
            return 1;
        }

        // We assume that only last buffer is not finished.
        return Math.max(0, numBuffers - 1);
    }

    Buffer buildSliceBuffer(BufferConsumerWithPartialRecordLength buffer) {
        return buffer.build();
    }

    /** for testing only. */
    @VisibleForTesting
    BufferConsumerWithPartialRecordLength getNextBuffer() {
        return buffers.poll();
    }

    /** for testing only. */
    // suppress this warning as it is only for testing.
    @SuppressWarnings("FieldAccessNotGuarded")
    @VisibleForTesting
    CompletableFuture<List<Buffer>> getChannelStateFuture() {
        return channelStateFuture;
    }

    // suppress this warning as it is only for testing.
    @SuppressWarnings("FieldAccessNotGuarded")
    @VisibleForTesting
    public long getChannelStateCheckpointId() {
        return channelStateCheckpointId;
    }
}
