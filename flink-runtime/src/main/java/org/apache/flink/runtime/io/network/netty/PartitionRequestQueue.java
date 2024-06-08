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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.PartitionRequestListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A nonEmptyReader of partition queues, which listens for channel writability changed events before
 * writing and flushing {@link Buffer} instances.
 */
class PartitionRequestQueue extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestQueue.class);

    private final ChannelFutureListener writeListener =
            new WriteAndFlushNextMessageIfPossibleListener();

    /** The readers which are already enqueued available for transferring data. */
    private final ArrayDeque<NetworkSequenceViewReader> availableReaders = new ArrayDeque<>();

    /** All the readers created for the consumers' partition requests. */
    private final ConcurrentMap<InputChannelID, NetworkSequenceViewReader> allReaders =
            new ConcurrentHashMap<>();

    private boolean fatalError;

    private ChannelHandlerContext ctx;

    @Override
    public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
        if (this.ctx == null) {
            this.ctx = ctx;
        }

        super.channelRegistered(ctx);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 通知读者队列非空
     *
     * @param reader 网络序列视图读者对象
    */
    void notifyReaderNonEmpty(final NetworkSequenceViewReader reader) {
        // The notification might come from the same thread. For the initial writes this
        // might happen before the reader has set its reference to the view, because
        // creating the queue and the initial notification happen in the same method call.
        // This can be resolved by separating the creation of the view and allowing
        // notifications.

        // TODO This could potentially have a bad performance impact as in the
        // worst case (network consumes faster than the producer) each buffer
        // will trigger a separate event loop task being scheduled.
        // 使用上下文中的执行器来执行一个任务，该任务触发一个用户事件，并将reader作为参数传递
        // 这里的ctx很可能是某种上下文对象，它提供了访问事件循环和管道的方法
        // ctx.executor() 获取与该上下文关联的执行器，用于异步执行任务
        // ctx.pipeline() 获取与该上下文关联的管道，用于在管道中传播事件
        // fireUserEventTriggered 是在管道中触发用户定义的事件的方法
        ctx.executor().execute(() -> ctx.pipeline().fireUserEventTriggered(reader));
    }

    /**
     * Try to enqueue the reader once receiving credit notification from the consumer or receiving
     * non-empty reader notification from the producer.
     *
     * <p>NOTE: Only one thread would trigger the actual enqueue after checking the reader's
     * availability, so there is no race condition here.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将可用的NetworkSequenceViewReader加入队列以进行消费。
     *
     * @param reader 要加入队列的NetworkSequenceViewReader实例
     * @throws Exception 如果在入队或触发写操作时发生异常
    */
    private void enqueueAvailableReader(final NetworkSequenceViewReader reader) throws Exception {
        // 如果该reader已经被标记为可用，则直接返回，无需重复处理
        if (reader.isRegisteredAsAvailable()) {
            return;
        }
        // 获取reader的可用性和积压数据情况
        ResultSubpartitionView.AvailabilityWithBacklog availabilityWithBacklog =
                reader.getAvailabilityAndBacklog();
        // 如果reader当前不可用
        if (!availabilityWithBacklog.isAvailable()) {
            // 获取积压的数据量
            int backlog = availabilityWithBacklog.getBacklog();
            // 如果积压的数据量大于0，并且需要通告积压数据量
            if (backlog > 0 && reader.needAnnounceBacklog()) {
                // 通告积压数据量
                //在可用数据通知或数据消费恢复后，向消费者宣布剩余积压工作。
                announceBacklog(reader, backlog);
            }
            // 既然reader不可用，则直接返回
            return;
        }

        // Queue an available reader for consumption. If the queue is empty,
        // we try trigger the actual write. Otherwise this will be handled by
        // the writeAndFlushNextMessageIfPossible calls.
        // 如果reader是可用的，将其加入可用的reader队列中
        // 如果队列为空，则尝试触发实际的写操作
        // 否则，写操作将在后续通过writeAndFlushNextMessageIfPossible调用中处理
        // 记录是否需要触发写操作（如果队列为空）
        boolean triggerWrite = availableReaders.isEmpty();
        // 将reader注册为可用
        registerAvailableReader(reader);
        // 如果需要触发写操作（即队列原本为空），则执行写操作
        if (triggerWrite) {
            writeAndFlushNextMessageIfPossible(ctx.channel());
        }
    }

    /**
     * Accesses internal state to verify reader registration in the unit tests.
     *
     * <p><strong>Do not use anywhere else!</strong>
     *
     * @return readers which are enqueued available for transferring data
     */
    @VisibleForTesting
    ArrayDeque<NetworkSequenceViewReader> getAvailableReaders() {
        return availableReaders;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将NetworkSequenceViewReader维护到Map结构
    */
    public void notifyReaderCreated(final NetworkSequenceViewReader reader) {
        // 当一个新的（reader）被创建时，调用此方法
        // 将读者（reader）对象添加到allReaders集合中，以读者的接收器ID（receiverId）作为键
        allReaders.put(reader.getReceiverId(), reader);
    }

    public void cancel(InputChannelID receiverId) {
        ctx.pipeline().fireUserEventTriggered(receiverId);
    }

    public void close() throws IOException {
        if (ctx != null) {
            ctx.channel().close();
        }

        releaseAllResources();
    }

    /**
     * Adds unannounced credits from the consumer or resumes data consumption after an exactly-once
     * checkpoint and enqueues the corresponding reader for this consumer (if not enqueued yet).
     *
     * @param receiverId The input channel id to identify the consumer.
     * @param operation The operation to be performed (add credit or resume data consumption).
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 添加来自消费者的未公告的信用额度，或在恰好一次的检查点之后恢复数据消费，
     * 并将此消费者的对应读取器加入队列（如果尚未加入队列）。
     *
     * @param receiverId 用于标识消费者的输入通道ID。
     * @param operation 要执行的操作（添加信用额度或恢复数据消费）。
     * @throws Exception 如果在操作过程中发生错误。
    */
    void addCreditOrResumeConsumption(
            InputChannelID receiverId, Consumer<NetworkSequenceViewReader> operation)
            throws Exception {
        // 如果存在致命错误，则直接返回，不进行后续操作
        if (fatalError) {
            return;
        }
        // 根据给定的receiverId获取NetworkSequenceViewReader对象
        NetworkSequenceViewReader reader = obtainReader(receiverId);
        // 对获取的reader执行传入的operation操作（例如添加信用或恢复消费）
        operation.accept(reader);
        // 将可用的reader加入队列（如果尚未加入）
        enqueueAvailableReader(reader);
    }

    void acknowledgeAllRecordsProcessed(InputChannelID receiverId) {
        if (fatalError) {
            return;
        }

        obtainReader(receiverId).acknowledgeAllRecordsProcessed();
    }

    void notifyNewBufferSize(InputChannelID receiverId, int newBufferSize) {
        if (fatalError) {
            return;
        }

        // It is possible to receive new buffer size before the reader would be created since the
        // downstream task could calculate buffer size even using the data from one channel but it
        // sends new buffer size into all upstream even if they don't ready yet. In this case, just
        // ignore the new buffer size.
        NetworkSequenceViewReader reader = allReaders.get(receiverId);
        if (reader != null) {
            reader.notifyNewBufferSize(newBufferSize);
        }
    }

    /**
     * Notify the id of required segment from the consumer.
     *
     * @param receiverId The input channel id to identify the consumer.
     * @param subpartitionId The id of the corresponding subpartition.
     * @param segmentId The id of required segment.
     */
    void notifyRequiredSegmentId(InputChannelID receiverId, int subpartitionId, int segmentId) {
        if (fatalError) {
            return;
        }
        NetworkSequenceViewReader reader = allReaders.get(receiverId);
        if (reader != null) {
            reader.notifyRequiredSegmentId(subpartitionId, segmentId);
        }
    }

    NetworkSequenceViewReader obtainReader(InputChannelID receiverId) {
        NetworkSequenceViewReader reader = allReaders.get(receiverId);
        if (reader == null) {
            throw new IllegalStateException(
                    "No reader for receiverId = " + receiverId + " exists.");
        }

        return reader;
    }

    /**
     * Announces remaining backlog to the consumer after the available data notification or data
     * consumption resumption.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 在可用数据通知或数据消费恢复后，向消费者宣布剩余积压工作。
    */
    private void announceBacklog(NetworkSequenceViewReader reader, int backlog) {
        checkArgument(backlog > 0, "Backlog must be positive.");

        NettyMessage.BacklogAnnouncement announcement =
                new NettyMessage.BacklogAnnouncement(backlog, reader.getReceiverId());
        ctx.channel()
                .writeAndFlush(announcement)
                .addListener(
                        (ChannelFutureListener)
                                future -> {
                                    if (!future.isSuccess()) {
                                        onChannelFutureFailure(future);
                                    }
                                });
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * Netty框架中的事件处理回调方法，用于处理用户自定义事件。
    */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
        // The user event triggered event loop callback is used for thread-safe
        // hand over of reader queues and cancelled producers.
        // 检查传入的事件消息类型
        if (msg instanceof NetworkSequenceViewReader) {
            // 如果消息是NetworkSequenceViewReader类型，将其添加到可用的读取器队列中
            // enqueueAvailableReader方法可能用于管理一个内部队列，以便在需要时分配读取器
            enqueueAvailableReader((NetworkSequenceViewReader) msg);
        } else if (msg.getClass() == InputChannelID.class) {
            // Release partition view that get a cancel request.
            // 如果消息是InputChannelID类型，表示收到了取消请求
            // 释放与该取消请求相关联的分区视图
            InputChannelID toCancel = (InputChannelID) msg;

            // remove reader from queue of available readers
            // 从可用读取器队列中移除匹配的读取器
            availableReaders.removeIf(reader -> reader.getReceiverId().equals(toCancel));

            // remove reader from queue of all readers and release its resource
            // 从所有读取器队列中移除匹配的读取器，并释放其资源
            final NetworkSequenceViewReader toRelease = allReaders.remove(toCancel);
            if (toRelease != null) {
                // 释放读取器资源
                releaseViewReader(toRelease);
            }
            // 如果消息是PartitionRequestListener类型，表示某个分区请求监听器超时
            // 当监听器超时时，向下游任务发送分区未找到的消息
        } else if (msg instanceof PartitionRequestListener) {
            PartitionRequestListener partitionRequestListener = (PartitionRequestListener) msg;

            // Send partition not found message to the downstream task when the listener is timeout.
            // 获取分区ID和接收者ID
            final ResultPartitionID resultPartitionId =
                    partitionRequestListener.getResultPartitionId();
            final InputChannelID inputChannelId = partitionRequestListener.getReceiverId();
            // 从可用读取器队列中移除与分区请求监听器关联的读取器
            availableReaders.remove(partitionRequestListener.getViewReader());
            // 从所有读取器队列中移除与接收者ID关联的读取器
            allReaders.remove(inputChannelId);
            try {
                // 向接收者ID发送包含分区未找到异常的Netty消息
                ctx.writeAndFlush(
                        new NettyMessage.ErrorResponse(
                                new PartitionNotFoundException(resultPartitionId), inputChannelId));
            } catch (Exception e) {
                // 如果发送失败，记录警告日志
                LOG.warn(
                        "Write partition not found exception to {} for result partition {} fail",
                        inputChannelId,
                        resultPartitionId,
                        e);
            }
        } else {
            // 如果消息类型未知，继续传递事件到下一个处理器
            ctx.fireUserEventTriggered(msg);
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        writeAndFlushNextMessageIfPossible(ctx.channel());
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 写入到对应Channel
    */
    private void writeAndFlushNextMessageIfPossible(final Channel channel) throws IOException {
        // 如果发生致命错误或通道不可写，则直接返回
        if (fatalError || !channel.isWritable()) {
            return;
        }

        // The logic here is very similar to the combined input gate and local
        // input channel logic. You can think of this class acting as the input
        // gate and the consumed views as the local input channels.

        BufferAndAvailability next = null;
        int nextSubpartitionId = -1;
        try {
            // availableReaders队列中轮询可用的读取器  NetworkSequenceViewReader
            while (true) {
                // 获取NetworkSequenceViewReader
                NetworkSequenceViewReader reader = pollAvailableReader();

                // No queue with available data. We allow this here, because
                // of the write callbacks that are executed after each write.
                //如果为null,则直接返回
                if (reader == null) {
                    return;
                }
                 // 尝试从reader中预览下一个子分区的ID
                nextSubpartitionId = reader.peekNextBufferSubpartitionId();
                // 尝试从reader中获取下一个缓冲区
                next = reader.getNextBuffer();
                // 如果next为null（即没有更多的缓冲区可供读取）
                if (next == null) {
                    // 如果reader没有被释放
                    if (!reader.isReleased()) {
                        // 继续下一次循环，
                        continue;
                    }
                    // 如果reader被释放了，检查是否有失败的原因
                    Throwable cause = reader.getFailureCause();
                    if (cause != null) {
                        // 如果有失败的原因，创建一个错误响应，并将它发送到接收者
                        ErrorResponse msg = new ErrorResponse(cause, reader.getReceiverId());
                        // 将错误响应写入并刷新到上下文中的Channel
                        ctx.writeAndFlush(msg);
                    }
                } else {
                    // This channel was now removed from the available reader queue.
                    // We re-add it into the queue if it is still available
                    // 如果成功获取到缓冲区next
                    // 如果这个通道仍然有更多的缓冲区可供读取
                    if (next.moreAvailable()) {
                        registerAvailableReader(reader);
                    }
                    // 创建一个包含缓冲区数据的响应
                    BufferResponse msg =
                            new BufferResponse(
                                    next.buffer(),// 缓冲区数据
                                    next.getSequenceNumber(),// 序列号
                                    reader.getReceiverId(),// 接收者ID
                                    nextSubpartitionId,// 子分区ID
                                    next.buffersInBacklog());// 后备缓冲区数量

                    // Write and flush and wait until this is done before
                    // trying to continue with the next buffer.
                    // 将响应写入并刷新到通道，并添加一个监听器来等待操作完成
                    // 在继续处理下一个缓冲区之前，确保这个操作已经完成
                    channel.writeAndFlush(msg).addListener(writeListener);
                    // 退出当前循环
                    return;
                }
            }
        } catch (Throwable t) {
            // 如果已经获取了next缓冲区，则回收其缓冲区资源
            if (next != null) {
                next.buffer().recycleBuffer();
            }
            // 将异常包装为IOException并重新抛出
            throw new IOException(t.getMessage(), t);
        }
    }

    private void registerAvailableReader(NetworkSequenceViewReader reader) {
        availableReaders.add(reader);
        reader.setRegisteredAsAvailable(true);
    }

    @Nullable
    private NetworkSequenceViewReader pollAvailableReader() {
        NetworkSequenceViewReader reader = availableReaders.poll();
        if (reader != null) {
            reader.setRegisteredAsAvailable(false);
        }
        return reader;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        releaseAllResources();

        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        handleException(ctx.channel(), cause);
    }

    private void handleException(Channel channel, Throwable cause) throws IOException {
        LOG.error(
                "Encountered error while consuming partitions (connection to {})",
                channel.remoteAddress(),
                cause);

        fatalError = true;
        releaseAllResources();

        if (channel.isActive()) {
            channel.writeAndFlush(new ErrorResponse(cause))
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }

    private void releaseAllResources() throws IOException {
        // note: this is only ever executed by one thread: the Netty IO thread!
        for (NetworkSequenceViewReader reader : allReaders.values()) {
            releaseViewReader(reader);
        }

        availableReaders.clear();
        allReaders.clear();
    }

    private void releaseViewReader(NetworkSequenceViewReader reader) throws IOException {
        reader.setRegisteredAsAvailable(false);
        reader.releaseAllResources();
    }

    private void onChannelFutureFailure(ChannelFuture future) throws Exception {
        if (future.cause() != null) {
            handleException(future.channel(), future.cause());
        } else {
            handleException(
                    future.channel(), new IllegalStateException("Sending cancelled by user."));
        }
    }

    public void notifyPartitionRequestTimeout(PartitionRequestListener partitionRequestListener) {
        ctx.pipeline().fireUserEventTriggered(partitionRequestListener);
    }

    // This listener is called after an element of the current nonEmptyReader has been
    // flushed. If successful, the listener triggers further processing of the
    // queues.
    private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            try {
                if (future.isSuccess()) {
                    writeAndFlushNextMessageIfPossible(future.channel());
                } else {
                    onChannelFutureFailure(future);
                }
            } catch (Throwable t) {
                handleException(future.channel(), t);
            }
        }
    }
}
