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
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class NettyConnectionManager implements ConnectionManager {

    private final NettyServer server;

    private final NettyClient client;

    private final NettyBufferPool bufferPool;

    private final PartitionRequestClientFactory partitionRequestClientFactory;

    private final NettyProtocol nettyProtocol;

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 构造NettyConnectionManager实例
     * @param partitionProvider 用于提供ResultPartition的提供者，通常用于分布式系统中的数据分区。
     * @param taskEventPublisher 用于发布任务相关事件的发布者，通常用于通知其他组件关于任务的状态变化。
     * @param nettyConfig Netty的配置对象，包含了Netty客户端和服务器的各种配置参数。
     * @param maxNumberOfConnections 允许的最大连接数，用于限制同时建立的连接数量。
     * @param connectionReuseEnabled 是否启用连接复用的标志。如果为true，则会尝试重用已经建立的连接。
    */
    public NettyConnectionManager(
            ResultPartitionProvider partitionProvider,
            TaskEventPublisher taskEventPublisher,
            NettyConfig nettyConfig,
            int maxNumberOfConnections,
            boolean connectionReuseEnabled) {

        // 调用另一个构造函数，传入一个新建的NettyBufferPool对象和其他参数。
        // NettyBufferPool是一个用于管理Netty ByteBuf的数据缓冲池，用于提高性能和减少内存分配。
        // 这里使用了nettyConfig中的getNumberOfArenas()方法来获取NettyBufferPool所需的参数。
        this(
                new NettyBufferPool(nettyConfig.getNumberOfArenas()),
                partitionProvider,
                taskEventPublisher,
                nettyConfig,
                maxNumberOfConnections,
                connectionReuseEnabled);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 构造NettyConnectionManager实例
     */
    @VisibleForTesting
    public NettyConnectionManager(
            NettyBufferPool bufferPool,
            ResultPartitionProvider partitionProvider,
            TaskEventPublisher taskEventPublisher,
            NettyConfig nettyConfig,
            int maxNumberOfConnections,
            boolean connectionReuseEnabled) {
        // 创建一个新的Netty服务器实例，使用传入的nettyConfig进行配置
        this.server = new NettyServer(nettyConfig);
        // 创建一个新的Netty客户端实例，同样使用传入的nettyConfig进行配置
        this.client = new NettyClient(nettyConfig);
        // 使用checkNotNull方法检查bufferPool是否非空，如果为空则抛出NullPointerException
        // 然后将非空的bufferPool赋值给成员变量
        this.bufferPool = checkNotNull(bufferPool);
        // 创建一个新的PartitionRequestClientFactory实例，用来构建PartitionRequestClient
        this.partitionRequestClientFactory =
                new PartitionRequestClientFactory(
                        client,
                        nettyConfig.getNetworkRetries(),
                        maxNumberOfConnections,
                        connectionReuseEnabled);
        // 创建一个新的NettyProtocol实例，它依赖于ResultPartitionProvider和TaskEventPublisher
        this.nettyProtocol =
                new NettyProtocol(
                        checkNotNull(partitionProvider), checkNotNull(taskEventPublisher));
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 启动Netty连接管理器。
     *
     * @throws IOException 如果在初始化客户端或服务器时发生I/O错误，则抛出IOException。
     * @return 返回服务器初始化的结果，通常是表示服务器是否成功启动的整数值。
    */
    @Override
    public int start() throws IOException {
        // 初始化客户端，将Netty协议和ByteBuf池传递给客户端
        client.init(nettyProtocol, bufferPool);

        // 初始化服务器，将Netty协议和ByteBuf池传递给服务器
        // 返回服务器的初始化结果，通常是表示服务器是否成功启动的整数值
        return server.init(nettyProtocol, bufferPool);
    }

    @Override
    public PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId)
            throws IOException, InterruptedException {
        return partitionRequestClientFactory.createPartitionRequestClient(connectionId);
    }

    @Override
    public void closeOpenChannelConnections(ConnectionID connectionId) {
        partitionRequestClientFactory.closeOpenChannelConnections(connectionId);
    }

    @Override
    public int getNumberOfActiveConnections() {
        return partitionRequestClientFactory.getNumberOfActiveClients();
    }

    @Override
    public void shutdown() {
        client.shutdown();
        server.shutdown();
    }

    NettyClient getClient() {
        return client;
    }

    NettyServer getServer() {
        return server;
    }

    NettyBufferPool getBufferPool() {
        return bufferPool;
    }
}
