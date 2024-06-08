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
import org.apache.flink.util.FatalExitExceptionHandler;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.Epoll;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

class NettyServer {

    private static final ThreadFactoryBuilder THREAD_FACTORY_BUILDER =
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE);

    private static final Logger LOG = LoggerFactory.getLogger(NettyServer.class);

    private final NettyConfig config;

    private ServerBootstrap bootstrap;

    private ChannelFuture bindFuture;

    private InetSocketAddress localAddress;

    NettyServer(NettyConfig config) {
        this.config = checkNotNull(config);
        localAddress = null;
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 初始化方法，用于配置Netty服务器端的通道和相关资源。
     *
     * @param protocol            Netty协议类型，用于指定服务器使用的通信协议。
     * @param nettyBufferPool     Netty缓冲区池，用于优化内存使用和提高性能。
     * @return                    返回初始化结果（此处假设为int类型，但通常可能是一个更具体的类型或void）。
     * @throws IOException         如果初始化过程中发生I/O错误，则抛出此异常。
    */
    int init(final NettyProtocol protocol, NettyBufferPool nettyBufferPool) throws IOException {
        // 调用另一个重载的init方法，传入NettyBufferPool和一个lambda表达式作为参数。
        // lambda表达式中，根据传入的protocol和SSL处理工厂（由Netty自动创建）来创建一个ServerChannelInitializer实例。
        return init(
                nettyBufferPool,
                sslHandlerFactory -> new ServerChannelInitializer(protocol, sslHandlerFactory));
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 初始化和NettyServer
    */
    int init(
            NettyBufferPool nettyBufferPool,
            Function<SSLHandlerFactory, ServerChannelInitializer> channelInitializer)
            throws IOException {
        // 检查Netty服务器是否已经初始化过，如果已经初始化过则抛出状态异常
        checkState(bootstrap == null, "Netty server has already been initialized.");

        final long start = System.nanoTime();
        // 创建一个新的ServerBootstrap实例，用于配置新的服务器
        bootstrap = new ServerBootstrap();

        // --------------------------------------------------------------------
        // Transport-specific configuration
        // --------------------------------------------------------------------
        // 传输特定配置
        // 根据配置选择NIO、EPOLL或自动选择
        switch (config.getTransportType()) {
            case NIO:
                // 初始化NIO模式的ServerBootstrap
                initNioBootstrap();
                break;

            case EPOLL:
                // 初始化EPOLL模式的ServerBootstrap
                initEpollBootstrap();
                break;

            case AUTO:
                // 如果EPOLL可用，则使用EPOLL
                if (Epoll.isAvailable()) {
                    initEpollBootstrap();
                    LOG.info("Transport type 'auto': using EPOLL.");
                } else {
                    initNioBootstrap();
                    LOG.info("Transport type 'auto': using NIO.");
                }
        }

        // --------------------------------------------------------------------
        // Configuration
        // --------------------------------------------------------------------

        // Pooled allocators for Netty's ByteBuf instances
        //Netty的ByteBuf实例的池分配器 用来netty 读取数据时分配内存单元
        bootstrap.option(ChannelOption.ALLOCATOR, nettyBufferPool);
        bootstrap.childOption(ChannelOption.ALLOCATOR, nettyBufferPool);
        // 如果设置了服务器连接backlog（等待队列长度），则设置到ServerBootstrap
        if (config.getServerConnectBacklog() > 0) {
            bootstrap.option(ChannelOption.SO_BACKLOG, config.getServerConnectBacklog());
        }

        // Receive and send buffer size
        // 设置接收和发送缓冲区大小
        int receiveAndSendBufferSize = config.getSendAndReceiveBufferSize();
        if (receiveAndSendBufferSize > 0) {
            // 设置子通道（客户端连接）的发送缓冲区大小
            bootstrap.childOption(ChannelOption.SO_SNDBUF, receiveAndSendBufferSize);
            // 设置子通道（客户端连接）的接收缓冲区大小
            bootstrap.childOption(ChannelOption.SO_RCVBUF, receiveAndSendBufferSize);
        }

        // SSL related configuration
        // 尝试根据配置创建SSL工厂
        final SSLHandlerFactory sslHandlerFactory;
        try {
            sslHandlerFactory = config.createServerSSLEngineFactory();
        } catch (Exception e) {
            throw new IOException("Failed to initialize SSL Context for the Netty Server", e);
        }

        // --------------------------------------------------------------------
        // Child channel pipeline for accepted connections
        // --------------------------------------------------------------------
        // 初始化子处理器，将其应用于SSL处理器工厂
        bootstrap.childHandler(channelInitializer.apply(sslHandlerFactory));

        // --------------------------------------------------------------------
        // Start Server
        // --------------------------------------------------------------------
        // 调试日志，输出正在尝试初始化的Netty服务器地址和端口范围
        LOG.debug(
                "Trying to initialize Netty server on address: {} and port range {}",
                config.getServerAddress(),
                config.getServerPortRange());

        // 获取端口范围的迭代器
        Iterator<Integer> portsIterator = config.getServerPortRange().getPortsIterator();
        // 尝试绑定服务器到配置的端口范围内的一个可用端口
        while (portsIterator.hasNext() && bindFuture == null) {
            Integer port = portsIterator.next();
            LOG.debug("Trying to bind Netty server to port: {}", port);
            // 设置服务器的本地地址和端口
            bootstrap.localAddress(config.getServerAddress(), port);
            try {
                // 同步绑定服务器到指定的地址和端口，并等待直到绑定完成
                bindFuture = bootstrap.bind().syncUninterruptibly();
            } catch (Exception e) {
                // 如果异常不是由于端口被占用引起的，则直接抛出异常
                LOG.debug("Failed to bind Netty server", e);
                // syncUninterruptibly() throws checked exceptions via Unsafe
                // continue if the exception is due to the port being in use, fail early
                // otherwise
                if (!(e instanceof java.net.BindException)) {
                    throw e;
                }
            }
        }
        // 如果没有成功绑定到任何端口，则抛出BindException异常
        if (bindFuture == null) {
            throw new BindException(
                    "Could not start rest endpoint on any port in port range "
                            + config.getServerPortRange());
        }
        // 获取绑定成功的本地地址
        localAddress = (InetSocketAddress) bindFuture.channel().localAddress();
        // 计算并输出服务器初始化所需的时间（毫秒）以及正在监听的SocketAddress
        final long duration = (System.nanoTime() - start) / 1_000_000;
        LOG.info(
                "Successful initialization (took {} ms). Listening on SocketAddress {}.",
                duration,
                localAddress);
        // 返回成功绑定的端口号
        return localAddress.getPort();
    }

    NettyConfig getConfig() {
        return config;
    }

    ServerBootstrap getBootstrap() {
        return bootstrap;
    }

    Integer getListeningPort() {
        return localAddress == null ? null : localAddress.getPort();
    }

    void shutdown() {
        final long start = System.nanoTime();
        if (bindFuture != null) {
            bindFuture.channel().close().awaitUninterruptibly();
            bindFuture = null;
        }

        if (bootstrap != null) {
            if (bootstrap.config().group() != null) {
                bootstrap.config().group().shutdownGracefully();
            }
            bootstrap = null;
        }
        final long duration = (System.nanoTime() - start) / 1_000_000;
        LOG.info("Successful shutdown (took {} ms).", duration);
    }

    private void initNioBootstrap() {
        // Add the server port number to the name in order to distinguish
        // multiple servers running on the same host.
        String name =
                NettyConfig.SERVER_THREAD_GROUP_NAME + " (" + config.getServerPortRange() + ")";

        NioEventLoopGroup nioGroup =
                new NioEventLoopGroup(config.getServerNumThreads(), getNamedThreadFactory(name));
        bootstrap.group(nioGroup).channel(NioServerSocketChannel.class);
    }

    private void initEpollBootstrap() {
        // Add the server port number to the name in order to distinguish
        // multiple servers running on the same host.
        String name =
                NettyConfig.SERVER_THREAD_GROUP_NAME + " (" + config.getServerPortRange() + ")";

        EpollEventLoopGroup epollGroup =
                new EpollEventLoopGroup(config.getServerNumThreads(), getNamedThreadFactory(name));
        bootstrap.group(epollGroup).channel(EpollServerSocketChannel.class);
    }

    public static ThreadFactory getNamedThreadFactory(String name) {
        return THREAD_FACTORY_BUILDER.setNameFormat(name + " Thread %d").build();
    }

    @VisibleForTesting
    static class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {
        private final NettyProtocol protocol;
        private final SSLHandlerFactory sslHandlerFactory;

        public ServerChannelInitializer(
                NettyProtocol protocol, SSLHandlerFactory sslHandlerFactory) {
            this.protocol = protocol;
            this.sslHandlerFactory = sslHandlerFactory;
        }

        @Override
        public void initChannel(SocketChannel channel) throws Exception {
            if (sslHandlerFactory != null) {
                channel.pipeline()
                        .addLast("ssl", sslHandlerFactory.createNettySSLHandler(channel.alloc()));
            }

            channel.pipeline().addLast(protocol.getServerChannelHandlers());
        }
    }
}
