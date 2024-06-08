package com.source.netty;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;

public class NettyClient {
    public static void main(String[] args) throws Exception {
        // 配置客户端NIO线程组
        EventLoopGroup group = new NioEventLoopGroup();

        try {
            Bootstrap b = new Bootstrap(); // 启动客户端的Bootstrap
            b.group(group) // 设置线程组
                    .channel(NioSocketChannel.class) // 使用NioSocketChannel作为客户端的通道实现
                    .handler(new ChannelInitializer<SocketChannel>() { // 设置处理器
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            // 在这里添加你的业务处理器
                            // 例如：ch.pipeline().addLast(new ClientHandler());
                        }
                    });

            // 连接到服务器，同步等待连接成功
            ChannelFuture f = b.connect("localhost", 8080).sync();

            // 等待客户端连接关闭
            f.channel().closeFuture().sync();
        } finally {
            // 关闭所有事件循环以终止所有线程
            group.shutdownGracefully();
        }
    }
}
