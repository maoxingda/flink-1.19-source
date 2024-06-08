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

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionRequest;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.TaskEventRequest;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Partition request client for remote partition requests.
 *
 * <p>This client is shared by all remote input channels, which request a partition from the same
 * {@link ConnectionID}.
 */
public class NettyPartitionRequestClient implements PartitionRequestClient {

    private static final Logger LOG = LoggerFactory.getLogger(NettyPartitionRequestClient.class);

    private final Channel tcpChannel;

    private final NetworkClientHandler clientHandler;

    private final ConnectionID connectionId;

    private final PartitionRequestClientFactory clientFactory;

    /** If zero, the underlying TCP channel can be safely closed. */
    private final AtomicInteger closeReferenceCounter = new AtomicInteger(0);

    private final AtomicBoolean closed = new AtomicBoolean(false);

    NettyPartitionRequestClient(
            Channel tcpChannel,
            NetworkClientHandler clientHandler,
            ConnectionID connectionId,
            PartitionRequestClientFactory clientFactory) {

        this.tcpChannel = checkNotNull(tcpChannel);
        this.clientHandler = checkNotNull(clientHandler);
        this.connectionId = checkNotNull(connectionId);
        this.clientFactory = checkNotNull(clientFactory);
        clientHandler.setConnectionId(connectionId);
    }

    boolean canBeDisposed() {
        return closeReferenceCounter.get() == 0 && !canBeReused();
    }

    /**
     * Validate the client and increment the reference counter.
     *
     * <p>Note: the reference counter has to be incremented before returning the instance of this
     * client to ensure correct closing logic.
     *
     * @return whether this client can be used.
     */
    boolean validateClientAndIncrementReferenceCounter() {
        if (!clientHandler.hasChannelError()) {
            return closeReferenceCounter.incrementAndGet() > 0;
        }
        return false;
    }

    /**
     * Requests a remote intermediate result partition queue.
     *
     * <p>The request goes to the remote producer, for which this partition request client instance
     * has been created.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 请求ResultPartition的子分区。
     *
     * @param partitionId 结果分区的ID
     * @param subpartitionIndexSet 子分区索引集合
     * @param inputChannel 远程输入通道
     * @param delayMs 延迟毫秒数
     * @throws IOException 如果发生I/O错误
    */
    @Override
    public void requestSubpartition(
            final ResultPartitionID partitionId,
            final ResultSubpartitionIndexSet subpartitionIndexSet,
            final RemoteInputChannel inputChannel,
            int delayMs)
            throws IOException {
        // 检查当前对象是否已关闭
        checkNotClosed();
        // 日志记录，调试级别，记录请求的详细信息
        LOG.debug(
                "Requesting subpartition {} of partition {} with {} ms delay.",
                subpartitionIndexSet,
                partitionId,
                delayMs);
        //调用clientHandler.addInputChannel(inputChannel)方法，将当前的InputChannel添加到NetworkClientHandler中。
        clientHandler.addInputChannel(inputChannel);
        // 创建一个PartitionRequest对象，该对象包含分区ID、子分区索引集、输入通道的ID和初始信用额度
        final PartitionRequest request =
                new PartitionRequest(
                        partitionId,
                        subpartitionIndexSet,
                        inputChannel.getInputChannelId(),
                        inputChannel.getInitialCredit());
        // 创建一个ChannelFutureListener监听器，当请求完成（成功或失败）时触发
        final ChannelFutureListener listener =
                future -> {
                    // 如果请求失败，则从clientHandler中移除该InputChannel
                    if (!future.isSuccess()) {
                        clientHandler.removeInputChannel(inputChannel);
                        // 在InputChannel上报告错误，并创建一个LocalTransportException异常
                        // 该异常描述了向特定连接ID发送分区请求失败的情况
                        inputChannel.onError(
                                new LocalTransportException(
                                        String.format(
                                                "Sending the partition request to '%s [%s] (#%d)' failed.",
                                                connectionId.getAddress(),
                                                connectionId
                                                        .getResourceID()
                                                        .getStringWithMetadata(),
                                                connectionId.getConnectionIndex()),
                                        future.channel().localAddress(),
                                        future.cause()));
                        //发送错误异常
                        sendToChannel(
                                new ConnectionErrorMessage(
                                        future.cause() == null
                                                ? new RuntimeException(
                                                        "Cannot send partition request.")
                                                : future.cause()));
                    }
                };
        // 判断是否立即发送请求，或者需要延迟发送
        if (delayMs == 0) {
            // 如果 delayMs 为 0，表示需要立即发送请求
            // 使用 tcpChannel 发送请求并立即刷新到网络
            ChannelFuture f = tcpChannel.writeAndFlush(request);
            // 给发送操作添加一个监听器，用于处理发送成功或失败等事件
            f.addListener(listener);
        } else {
            // 如果 delayMs 不为 0，表示需要延迟发送请求
            // 创建一个 ChannelFuture 数组，用于在延迟执行的任务中存储发送的结果
            final ChannelFuture[] f = new ChannelFuture[1];
            // 使用 tcpChannel 所在的 EventLoop 来调度一个延迟任务
            // 当延迟时间到达后，执行 lambda 表达式中的代码
            tcpChannel
                    .eventLoop()
                    .schedule(
                            () -> {
                                f[0] = tcpChannel.writeAndFlush(request);
                                f[0].addListener(listener);
                            },
                            delayMs,
                            TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Sends a task event backwards to an intermediate result partition producer.
     *
     * <p>Backwards task events flow between readers and writers and therefore will only work when
     * both are running at the same time, which is only guaranteed to be the case when both the
     * respective producer and consumer task run pipelined.
     */
    @Override
    public void sendTaskEvent(
            ResultPartitionID partitionId, TaskEvent event, final RemoteInputChannel inputChannel)
            throws IOException {
        checkNotClosed();

        tcpChannel
                .writeAndFlush(
                        new TaskEventRequest(event, partitionId, inputChannel.getInputChannelId()))
                .addListener(
                        (ChannelFutureListener)
                                future -> {
                                    if (!future.isSuccess()) {
                                        inputChannel.onError(
                                                new LocalTransportException(
                                                        String.format(
                                                                "Sending the task event to '%s [%s] (#%d)' failed.",
                                                                connectionId.getAddress(),
                                                                connectionId
                                                                        .getResourceID()
                                                                        .getStringWithMetadata(),
                                                                connectionId.getConnectionIndex()),
                                                        future.channel().localAddress(),
                                                        future.cause()));
                                        sendToChannel(
                                                new ConnectionErrorMessage(
                                                        future.cause() == null
                                                                ? new RuntimeException(
                                                                        "Cannot send task event.")
                                                                : future.cause()));
                                    }
                                });
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 发送信用额度信息
    */
    @Override
    public void notifyCreditAvailable(RemoteInputChannel inputChannel) {
        // 创建一个新的 AddCreditMessage 对象，并将传入的 inputChannel 作为参数传递给它。
        // AddCreditMessage 可能是一个自定义的消息类型，用于通知接收方现在有更多的信用额度可以使用。
        sendToChannel(new AddCreditMessage(inputChannel));
    }

    @Override
    public void notifyNewBufferSize(RemoteInputChannel inputChannel, int bufferSize) {
        sendToChannel(new NewBufferSizeMessage(inputChannel, bufferSize));
    }

    @Override
    public void notifyRequiredSegmentId(
            RemoteInputChannel inputChannel, int subpartitionIndex, int segmentId) {
        sendToChannel(new SegmentIdMessage(inputChannel, subpartitionIndex, segmentId));
    }

    @Override
    public void resumeConsumption(RemoteInputChannel inputChannel) {
        sendToChannel(new ResumeConsumptionMessage(inputChannel));
    }

    @Override
    public void acknowledgeAllRecordsProcessed(RemoteInputChannel inputChannel) {
        sendToChannel(new AcknowledgeAllRecordsProcessedMessage(inputChannel));
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 触发ChannelPipeline中的UserEvent事件
    */
    private void sendToChannel(Object message) {
        tcpChannel.eventLoop().execute(() -> tcpChannel.pipeline().fireUserEventTriggered(message));
    }

    @Override
    public void close(RemoteInputChannel inputChannel) throws IOException {

        clientHandler.removeInputChannel(inputChannel);

        if (closeReferenceCounter.updateAndGet(count -> Math.max(count - 1, 0)) == 0
                && !canBeReused()) {
            closeConnection();
        } else {
            clientHandler.cancelRequestFor(inputChannel.getInputChannelId());
        }
    }

    public void closeConnection() {
        Preconditions.checkState(
                canBeDisposed(), "The connection should not be closed before disposed.");
        if (closed.getAndSet(true)) {
            // Do not close connection repeatedly
            return;
        }
        // Close the TCP connection. Send a close request msg to ensure
        // that outstanding backwards task events are not discarded.
        tcpChannel
                .writeAndFlush(new NettyMessage.CloseRequest())
                .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        // Make sure to remove the client from the factory
        clientFactory.destroyPartitionRequestClient(connectionId, this);
    }

    private boolean canBeReused() {
        return clientFactory.isConnectionReuseEnabled() && !clientHandler.hasChannelError();
    }

    private void checkNotClosed() throws IOException {
        if (closed.get()) {
            final SocketAddress localAddr = tcpChannel.localAddress();
            final SocketAddress remoteAddr = tcpChannel.remoteAddress();
            throw new LocalTransportException(
                    String.format(
                            "Channel to '%s [%s]' closed.",
                            remoteAddr, connectionId.getResourceID().getStringWithMetadata()),
                    localAddr);
        }
    }

    private static class AddCreditMessage extends ClientOutboundMessage {

        private AddCreditMessage(RemoteInputChannel inputChannel) {
            super(checkNotNull(inputChannel));
        }

        @Override
        Object buildMessage() {
            int credits = inputChannel.getAndResetUnannouncedCredit();
            return credits > 0
                    ? new NettyMessage.AddCredit(credits, inputChannel.getInputChannelId())
                    : null;
        }
    }

    private static class NewBufferSizeMessage extends ClientOutboundMessage {
        private final int bufferSize;

        private NewBufferSizeMessage(RemoteInputChannel inputChannel, int bufferSize) {
            super(checkNotNull(inputChannel));
            this.bufferSize = bufferSize;
        }

        @Override
        Object buildMessage() {
            return new NettyMessage.NewBufferSize(bufferSize, inputChannel.getInputChannelId());
        }
    }

    private static class ResumeConsumptionMessage extends ClientOutboundMessage {

        private ResumeConsumptionMessage(RemoteInputChannel inputChannel) {
            super(checkNotNull(inputChannel));
        }

        @Override
        Object buildMessage() {
            return new NettyMessage.ResumeConsumption(inputChannel.getInputChannelId());
        }
    }

    private static class AcknowledgeAllRecordsProcessedMessage extends ClientOutboundMessage {

        private AcknowledgeAllRecordsProcessedMessage(RemoteInputChannel inputChannel) {
            super(checkNotNull(inputChannel));
        }

        @Override
        Object buildMessage() {
            return new NettyMessage.AckAllUserRecordsProcessed(inputChannel.getInputChannelId());
        }
    }

    private static class SegmentIdMessage extends ClientOutboundMessage {

        private final int segmentId;

        private final int subpartitionIndex;

        private SegmentIdMessage(
                RemoteInputChannel inputChannel, int subpartitionIndex, int segmentId) {
            super(checkNotNull(inputChannel));
            this.subpartitionIndex = subpartitionIndex;
            this.segmentId = segmentId;
        }

        @Override
        Object buildMessage() {
            return new NettyMessage.SegmentId(
                    subpartitionIndex, segmentId, inputChannel.getInputChannelId());
        }
    }
}
