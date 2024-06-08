package com.source.netty;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LogLevel;
import org.apache.flink.shaded.netty4.io.netty.handler.logging.LoggingHandler;

public class NettyServer {
    public static void main(String[] args) throws Exception {
        // 配置服务端NIO线程组
        EventLoopGroup bossGroup = new NioEventLoopGroup(); // 接受客户端的连接
        EventLoopGroup workerGroup = new NioEventLoopGroup(); // 处理已经接收的连接，真正负责I/O读写操作

        try {
            ServerBootstrap b = new ServerBootstrap(); // 启动NIO服务的辅助启动类
            b.group(bossGroup, workerGroup) // 设置两个线程组
                    .channel(NioServerSocketChannel.class) // 使用NioServerSocketChannel来作为服务器的通道实现
                    .option(ChannelOption.SO_BACKLOG, 128) // 设置线程队列等待连接的个数
                    .handler(new LoggingHandler(LogLevel.INFO)) // 设置日志级别
                    .childHandler(new ChannelInitializer<SocketChannel>() { // 给pipeline设置处理器
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            // 在这里添加你的业务处理器
                            // 例如：ch.pipeline().addLast(new ServerHandler());
                        }
                    });

            // 绑定端口，同步等待成功
            ChannelFuture f = b.bind(8080).sync();

            // 等待服务端监听端口关闭
            f.channel().closeFuture().sync();
        } finally {
            // 关闭所有事件循环以终止所有线程
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
