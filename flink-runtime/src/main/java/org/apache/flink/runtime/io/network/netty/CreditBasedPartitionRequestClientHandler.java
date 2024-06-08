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
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.runtime.io.network.netty.exception.RemoteTransportException;
import org.apache.flink.runtime.io.network.netty.exception.TransportException;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Channel handler to read the messages of buffer response or error response from the producer, to
 * write and flush the unannounced credits for the producer.
 *
 * <p>It is used in the new network credit-based mode.
 */

class CreditBasedPartitionRequestClientHandler extends ChannelInboundHandlerAdapter
        implements NetworkClientHandler {

    private static final Logger LOG =
            LoggerFactory.getLogger(CreditBasedPartitionRequestClientHandler.class);

    /** Channels, which already requested partitions from the producers. */
    private final ConcurrentMap<InputChannelID, RemoteInputChannel> inputChannels =
            new ConcurrentHashMap<>();

    /** Messages to be sent to the producers (credit announcement or resume consumption request). */
    private final ArrayDeque<ClientOutboundMessage> clientOutboundMessages = new ArrayDeque<>();

    private final AtomicReference<Throwable> channelError = new AtomicReference<>();

    private final ChannelFutureListener writeListener =
            new WriteAndFlushNextMessageIfPossibleListener();

    /**
     * The channel handler context is initialized in channel active event by netty thread, the
     * context may also be accessed by task thread or canceler thread to cancel partition request
     * during releasing resources.
     */
    private volatile ChannelHandlerContext ctx;

    private ConnectionID connectionID;

    // ------------------------------------------------------------------------
    // Input channel/receiver registration
    // ------------------------------------------------------------------------

    @Override
    public void addInputChannel(RemoteInputChannel listener) throws IOException {
        checkError();

        inputChannels.putIfAbsent(listener.getInputChannelId(), listener);
    }

    @Override
    public void removeInputChannel(RemoteInputChannel listener) {
        inputChannels.remove(listener.getInputChannelId());
    }

    @Override
    public RemoteInputChannel getInputChannel(InputChannelID inputChannelId) {
        return inputChannels.get(inputChannelId);
    }

    @Override
    public void cancelRequestFor(InputChannelID inputChannelId) {
        if (inputChannelId == null || ctx == null) {
            return;
        }

        ctx.writeAndFlush(new NettyMessage.CancelPartitionRequest(inputChannelId));
    }

    // ------------------------------------------------------------------------
    // Network events
    // ------------------------------------------------------------------------

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        if (this.ctx == null) {
            this.ctx = ctx;
        }

        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        final SocketAddress remoteAddr = ctx.channel().remoteAddress();

        notifyAllChannelsOfErrorAndClose(
                new RemoteTransportException(
                        "Connection unexpectedly closed by remote task manager '"
                                + remoteAddr
                                + " [ "
                                + connectionID.getResourceID().getStringWithMetadata()
                                + " ] "
                                + "'. "
                                + "This might indicate that the remote task manager was lost.",
                        remoteAddr));

        super.channelInactive(ctx);
    }

    /**
     * Called on exceptions in the client handler pipeline.
     *
     * <p>Remote exceptions are received as regular payload.
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof TransportException) {
            notifyAllChannelsOfErrorAndClose(cause);
        } else {
            final SocketAddress remoteAddr = ctx.channel().remoteAddress();

            final TransportException tex;

            // Improve on the connection reset by peer error message
            if (cause.getMessage() != null
                    && cause.getMessage().contains("Connection reset by peer")) {
                tex =
                        new RemoteTransportException(
                                "Lost connection to task manager '"
                                        + remoteAddr
                                        + " [ "
                                        + connectionID.getResourceID().getStringWithMetadata()
                                        + " ] "
                                        + "'. "
                                        + "This indicates that the remote task manager was lost.",
                                remoteAddr,
                                cause);
            } else {
                final SocketAddress localAddr = ctx.channel().localAddress();
                tex =
                        new LocalTransportException(
                                String.format(
                                        "%s (connection to '%s [%s]')",
                                        cause.getMessage(),
                                        remoteAddr,
                                        connectionID.getResourceID().getStringWithMetadata()),
                                localAddr,
                                cause);
            }

            notifyAllChannelsOfErrorAndClose(tex);
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 当从Channel读取到数据时，此方法会被调用。
     *
     * @param ctx 上下文对象，包含了Channel、ChannelPipeline等信息
     * @param msg 从Channel接收到的消息
     * @throws Exception 如果解码过程中发生异常，则会抛出异常
    */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        try {
            // 尝试解码接收到的消息
            decodeMsg(msg);
        } catch (Throwable t) {
            // 如果解码过程中出现异常，则通知所有通道发生了错误并关闭这些通道
            notifyAllChannelsOfErrorAndClose(t);
        }
    }

    /**
     * Triggered by notifying credit available in the client handler pipeline.
     *
     * <p>Enqueues the input channel and will trigger write&flush unannounced credits for this input
     * channel if it is the first one in the queue.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 处理ClientOutboundMessage类型的了消息
    */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
        // 判断传入的事件消息是否是ClientOutboundMessage类型
        if (msg instanceof ClientOutboundMessage) {
            // 如果clientOutboundMessages队列为空，则设置triggerWrite为true
            // 这一步是为了判断是否需要立即触发消息写入操作
            boolean triggerWrite = clientOutboundMessages.isEmpty();
            // 将ClientOutboundMessage类型的消息添加到clientOutboundMessages队列中
            clientOutboundMessages.add((ClientOutboundMessage) msg);

            // 如果在添加消息之前队列是空的（即triggerWrite为true），则调用writeAndFlushNextMessageIfPossible方法
            // 尝试发送队列中的第一个消息
            if (triggerWrite) {
                writeAndFlushNextMessageIfPossible(ctx.channel());
            }
            // 判断传入的事件消息是否是ConnectionErrorMessage类型
        } else if (msg instanceof ConnectionErrorMessage) {
            // 调用notifyAllChannelsOfErrorAndClose方法，通知所有通道发生错误并关闭它们
            // 这里假设该方法会处理错误原因，并将错误通知给所有相关的通道，然后关闭它们
            notifyAllChannelsOfErrorAndClose(((ConnectionErrorMessage) msg).getCause());
        } else {
            // 如果传入的事件消息既不是ClientOutboundMessage也不是ConnectionErrorMessage类型
            // 将事件消息继续向下传递给ChannelPipeline中的下一个ChannelInboundHandler
            ctx.fireUserEventTriggered(msg);
        }
    }

    @Override
    public boolean hasChannelError() {
        return channelError.get() != null;
    }

    @Override
    public void setConnectionId(ConnectionID connectionId) {
        this.connectionID = checkNotNull(connectionId);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        writeAndFlushNextMessageIfPossible(ctx.channel());
    }

    @VisibleForTesting
    void notifyAllChannelsOfErrorAndClose(Throwable cause) {
        if (channelError.compareAndSet(null, cause)) {
            try {
                for (RemoteInputChannel inputChannel : inputChannels.values()) {
                    inputChannel.onError(cause);
                }
            } catch (Throwable t) {
                // We can only swallow the Exception at this point. :(
                LOG.warn(
                        "An Exception was thrown during error notification of a remote input channel.",
                        t);
            } finally {
                inputChannels.clear();
                clientOutboundMessages.clear();

                if (ctx != null) {
                    ctx.close();
                }
            }
        }
    }

    // ------------------------------------------------------------------------

    /** Checks for an error and rethrows it if one was reported. */
    @VisibleForTesting
    void checkError() throws IOException {
        final Throwable t = channelError.get();

        if (t != null) {
            if (t instanceof IOException) {
                throw (IOException) t;
            } else {
                throw new IOException("There has been an error in the channel.", t);
            }
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 解码接收到的消息
     *
     * @param msg 从网络接收到的消息
    */
    private void decodeMsg(Object msg) {
        // 获取消息的类型
        final Class<?> msgClazz = msg.getClass();

        // ---- Buffer --------------------------------------------------------
        // 如果消息类型是 BufferResponse
        if (msgClazz == NettyMessage.BufferResponse.class) {
            // 强制转换为 BufferResponse 类型
            NettyMessage.BufferResponse bufferOrEvent = (NettyMessage.BufferResponse) msg;
            // 如果输入通道不存在或已被释放
            RemoteInputChannel inputChannel = inputChannels.get(bufferOrEvent.receiverId);
            if (inputChannel == null || inputChannel.isReleased()) {
                // 释放消息中的缓冲区
                bufferOrEvent.releaseBuffer();
                // 取消对应接收者ID的请求
                cancelRequestFor(bufferOrEvent.receiverId);
                // 退出当前处理
                return;
            }

            try {
                // 解码消息中的缓冲区或事件
                decodeBufferOrEvent(inputChannel, bufferOrEvent);
            } catch (Throwable t) {
                // 如果在解码过程中发生异常，通知输入通道发生了错误
                inputChannel.onError(t);
            }
            // 如果消息类型是 ErrorResponse
        } else if (msgClazz == NettyMessage.ErrorResponse.class) {
            // ---- Error ---------------------------------------------------------
            // 强制转换为 ErrorResponse 类型
            NettyMessage.ErrorResponse error = (NettyMessage.ErrorResponse) msg;
            // 获取远程地址
            SocketAddress remoteAddr = ctx.channel().remoteAddress();
            // 如果错误是致命的
            if (error.isFatalError()) {
                // 通知所有通道发生了致命错误并关闭它们
                notifyAllChannelsOfErrorAndClose(
                        new RemoteTransportException(
                                "Fatal error at remote task manager '"
                                        + remoteAddr
                                        + " [ "
                                        + connectionID.getResourceID().getStringWithMetadata()
                                        + " ] "
                                        + "'.",
                                remoteAddr,
                                error.cause));
            } else {
                // 根据接收者ID获取对应的输入通道
                RemoteInputChannel inputChannel = inputChannels.get(error.receiverId);
                // 如果输入通道存在
                if (inputChannel != null) {
                    // 如果错误的原因是找不到分区
                    if (error.cause.getClass() == PartitionNotFoundException.class) {
                        // 调用输入通道的 onFailedPartitionRequest 方法来处理分区找不到的情况
                        inputChannel.onFailedPartitionRequest();
                    } else {
                        // 对于其他类型的错误，构造一个 RemoteTransportException 并调用输入通道的 onError 方法
                        inputChannel.onError(
                                new RemoteTransportException(
                                        "Error at remote task manager '"
                                                + remoteAddr
                                                + " [ "
                                                + connectionID
                                                        .getResourceID()
                                                        .getStringWithMetadata()
                                                + " ] "
                                                + "'.",
                                        remoteAddr,
                                        error.cause));
                    }
                }
            }
            // 如果消息类型是 BacklogAnnouncement
        } else if (msgClazz == NettyMessage.BacklogAnnouncement.class) {
            // 强制转换为 BacklogAnnouncement 类型
            NettyMessage.BacklogAnnouncement announcement = (NettyMessage.BacklogAnnouncement) msg;
            // 根据接收者ID获取对应的输入通道
            RemoteInputChannel inputChannel = inputChannels.get(announcement.receiverId);
            // 如果输入通道不存在或已被释放
            if (inputChannel == null || inputChannel.isReleased()) {
                // 取消对应接收者ID的请求
                cancelRequestFor(announcement.receiverId);
                // 退出当前处理
                return;
            }

            try {
                // 调用输入通道的 onSenderBacklog 方法来处理发送者的积压情况
                inputChannel.onSenderBacklog(announcement.backlog);
            } catch (Throwable throwable) {
                // 如果在处理积压情况时发生异常，通知输入通道发生了错误
                inputChannel.onError(throwable);
            }
        } else {
            throw new IllegalStateException(
                    "Received unknown message from producer: " + msg.getClass());
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 解码从远程输入通道接收到的缓冲区或事件。
     *
     * @param inputChannel 远程输入通道
     * @param bufferOrEvent 缓冲区或事件的响应消息
     * @throws Throwable 如果在解码过程中发生任何异常，将抛出此异常
    */
    private void decodeBufferOrEvent(
            RemoteInputChannel inputChannel, NettyMessage.BufferResponse bufferOrEvent)
            throws Throwable {
        // 如果消息是缓冲区类型且缓冲区大小为0
        if (bufferOrEvent.isBuffer() && bufferOrEvent.bufferSize == 0) {
            // 调用输入通道的 onEmptyBuffer 方法，通知它收到了一个空的缓冲区
            // 并传递序列号和积压量（backlog）作为参数
            inputChannel.onEmptyBuffer(bufferOrEvent.sequenceNumber, bufferOrEvent.backlog);
            // 如果消息包含一个非空的缓冲区
        } else if (bufferOrEvent.getBuffer() != null) {
            // 调用输入通道的 onBuffer 方法，通知它收到了一个缓冲区
            // 并传递缓冲区、序列号、积压量和子分区ID（subpartitionId）作为参数
            inputChannel.onBuffer(
                    bufferOrEvent.getBuffer(),
                    bufferOrEvent.sequenceNumber,
                    bufferOrEvent.backlog,
                    bufferOrEvent.subpartitionId);
            // 如果以上条件都不满足（即消息既不是空缓冲区也没有包含缓冲区）
        } else {
            // 抛出状态异常，因为基于信用的输入通道中读取的缓冲区不应为null
            throw new IllegalStateException(
                    "The read buffer is null in credit-based input channel.");
        }
    }

    /**
     * Tries to write&flush unannounced credits for the next input channel in queue.
     *
     * <p>This method may be called by the first input channel enqueuing, or the complete future's
     * callback in previous input channel, or the channel writability changed event.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 尝试为队列中的下一个输入通道写入并刷新未声明的信用额度。
     *
     * <p>此方法可能由第一个入队的输入通道调用，或由前一个输入通道的完成回调调用，
     * 或由通道可写性改变事件触发。
     *
     * @param channel 当前操作的通道
     */
    private void writeAndFlushNextMessageIfPossible(Channel channel) {
        // 如果存在通道错误或通道不可写，则直接返回
        if (channelError.get() != null || !channel.isWritable()) {
            return;
        }
        // 循环尝试从队列中获取消息
        while (true) {
            // 从队列中尝试获取一个待发送的客户端出站消息
            ClientOutboundMessage outboundMessage = clientOutboundMessages.poll();

            // The input channel may be null because of the write callbacks
            // that are executed after each write.
            // 如果队列中没有消息，可能是因为之前的写操作回调已经执行完毕
            // 并且没有新的消息入队，所以直接返回
            if (outboundMessage == null) {
                return;
            }

            // It is no need to notify credit or resume data consumption for the released channel.
            // 如果消息的输入通道已经被释放，则无需通知信用额度或恢复数据消费
            // 因此，继续从队列中取下一个消息
            if (!outboundMessage.inputChannel.isReleased()) {
                // 构建待发送的消息
                Object msg = outboundMessage.buildMessage();
                // 如果构建的消息为空，则继续从队列中取下一个消息
                if (msg == null) {
                    continue;
                }

                // Write and flush and wait until this is done before
                // trying to continue with the next input channel.
                // 写入并刷新消息，并在完成前等待
                // 这样可以确保在下一个输入通道处理前，该消息已经发送并刷新完成
                channel.writeAndFlush(msg).addListener(writeListener);
                // 由于消息已经发送并等待刷新完成，所以退出循环
                return;
            }
        }
    }

    private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            try {
                if (future.isSuccess()) {
                    writeAndFlushNextMessageIfPossible(future.channel());
                } else if (future.cause() != null) {
                    notifyAllChannelsOfErrorAndClose(future.cause());
                } else {
                    notifyAllChannelsOfErrorAndClose(
                            new IllegalStateException("Sending cancelled by user."));
                }
            } catch (Throwable t) {
                notifyAllChannelsOfErrorAndClose(t);
            }
        }
    }
}
