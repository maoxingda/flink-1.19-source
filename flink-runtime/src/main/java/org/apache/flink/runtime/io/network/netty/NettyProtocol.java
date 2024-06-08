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

import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;

/** Defines the server and client channel handlers, i.e. the protocol, used by netty. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 定义netty使用的服务器和客户端通道处理程序，即协议
*/
public class NettyProtocol {

    private final NettyMessage.NettyMessageEncoder messageEncoder =
            new NettyMessage.NettyMessageEncoder();

    private final ResultPartitionProvider partitionProvider;
    private final TaskEventPublisher taskEventPublisher;

    NettyProtocol(
            ResultPartitionProvider partitionProvider, TaskEventPublisher taskEventPublisher) {
        this.partitionProvider = partitionProvider;
        this.taskEventPublisher = taskEventPublisher;
    }

    /**
     * Returns the server channel handlers.
     *
     * <pre>
     * +-------------------------------------------------------------------+
     * |                        SERVER CHANNEL PIPELINE                    |
     * |                                                                   |
     * |    +----------+----------+ (3) write  +----------------------+    |
     * |    | Queue of queues     +----------->| Message encoder      |    |
     * |    +----------+----------+            +-----------+----------+    |
     * |              /|\                                 \|/              |
     * |               | (2) enqueue                       |               |
     * |    +----------+----------+                        |               |
     * |    | Request handler     |                        |               |
     * |    +----------+----------+                        |               |
     * |              /|\                                  |               |
     * |               |                                   |               |
     * |   +-----------+-----------+                       |               |
     * |   | Message+Frame decoder |                       |               |
     * |   +-----------+-----------+                       |               |
     * |              /|\                                  |               |
     * +---------------+-----------------------------------+---------------+
     * |               | (1) client request               \|/
     * +---------------+-----------------------------------+---------------+
     * |               |                                   |               |
     * |       [ Socket.read() ]                    [ Socket.write() ]     |
     * |                                                                   |
     * |  Netty Internal I/O Threads (Transport Implementation)            |
     * +-------------------------------------------------------------------+
     * </pre>
     *
     * @return channel handlers
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 获取服务器通道处理器数组
     *
     * @return ChannelHandler 数组，包含消息编码器、Netty消息解码器、分区请求服务器处理器和分区请求队列
    */
    public ChannelHandler[] getServerChannelHandlers() {
        // 创建一个分区请求队列实例
        PartitionRequestQueue queueOfPartitionQueues = new PartitionRequestQueue();
        // 创建一个分区请求服务器处理器实例，传入分区提供者、任务事件发布者和分区请求队列
        PartitionRequestServerHandler serverHandler =
                new PartitionRequestServerHandler(
                        partitionProvider, taskEventPublisher, queueOfPartitionQueues);
        // 返回包含消息编码器、Netty消息解码器、分区请求服务器处理器和分区请求队列的ChannelHandler数组
        return new ChannelHandler[] {
            messageEncoder, // 消息编码器
            new NettyMessage.NettyMessageDecoder(),// Netty消息解码器
            serverHandler,// 分区请求服务器处理器
            queueOfPartitionQueues // 分区请求队列
        };
    }

    /**
     * Returns the client channel handlers.
     *
     * <pre>
     *     +-----------+----------+            +----------------------+
     *     | Remote input channel |            | request client       |
     *     +-----------+----------+            +-----------+----------+
     *                 |                                   | (1) write
     * +---------------+-----------------------------------+---------------+
     * |               |     CLIENT CHANNEL PIPELINE       |               |
     * |               |                                  \|/              |
     * |    +----------+----------+            +----------------------+    |
     * |    | Request handler     +            | Message encoder      |    |
     * |    +----------+----------+            +-----------+----------+    |
     * |              /|\                                 \|/              |
     * |               |                                   |               |
     * |    +----------+------------+                      |               |
     * |    | Message+Frame decoder |                      |               |
     * |    +----------+------------+                      |               |
     * |              /|\                                  |               |
     * +---------------+-----------------------------------+---------------+
     * |               | (3) server response              \|/ (2) client request
     * +---------------+-----------------------------------+---------------+
     * |               |                                   |               |
     * |       [ Socket.read() ]                    [ Socket.write() ]     |
     * |                                                                   |
     * |  Netty Internal I/O Threads (Transport Implementation)            |
     * +-------------------------------------------------------------------+
     * </pre>
     *
     * @return channel handlers
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 获取客户端通道处理器数组
     *
     * @return ChannelHandler 数组，包含消息编码器、基于Netty消息的客户端解码器代理和网络客户端处理器
    */
    public ChannelHandler[] getClientChannelHandlers() {
        // 创建一个网络客户端处理器实例，这里特指基于信用的分区请求客户端处理器
        NetworkClientHandler networkClientHandler = new CreditBasedPartitionRequestClientHandler();
        // 返回一个ChannelHandler数组，包含消息编码器、客户端解码器代理和网络客户端处理器
        return new ChannelHandler[] {
            // 消息编码器，用于将消息对象编码为网络可传输的格式
            messageEncoder,
                // 客户端解码器代理，将接收到的字节数据解码为NettyMessage对象，并传递给网络客户端处理器
                new NettyMessageClientDecoderDelegate(networkClientHandler),
                // 网络客户端处理器，处理解码后的NettyMessage对象，执行相应的业务逻辑
                networkClientHandler
        };
    }
}
