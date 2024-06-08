/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.util.IOUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.runtime.state.SnapshotExecutionType.ASYNCHRONOUS;
import static org.apache.flink.runtime.state.SnapshotExecutionType.SYNCHRONOUS;

/**
 * Builder class for {@link DefaultOperatorStateBackend} which handles all necessary initializations
 * and clean ups.
 */
public class DefaultOperatorStateBackendBuilder
        implements StateBackendBuilder<DefaultOperatorStateBackend, BackendBuildingException> {
    /** The user code classloader. */
    @VisibleForTesting protected final ClassLoader userClassloader;
    /** The execution configuration. */
    @VisibleForTesting protected final ExecutionConfig executionConfig;
    /** Flag to de/activate asynchronous snapshots. */
    @VisibleForTesting protected final boolean asynchronousSnapshots;
    /** State handles for restore. */
    @VisibleForTesting protected final Collection<OperatorStateHandle> restoreStateHandles;

    @VisibleForTesting protected final CloseableRegistry cancelStreamRegistry;

    public DefaultOperatorStateBackendBuilder(
            ClassLoader userClassloader,
            ExecutionConfig executionConfig,
            boolean asynchronousSnapshots,
            Collection<OperatorStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry) {
        this.userClassloader = userClassloader;
        this.executionConfig = executionConfig;
        this.asynchronousSnapshots = asynchronousSnapshots;
        this.restoreStateHandles = stateHandles;
        this.cancelStreamRegistry = cancelStreamRegistry;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 构建并返回一个 DefaultOperatorStateBackend 实例。
     *
     * @return 构建的 DefaultOperatorStateBackend 实例
     * @throws BackendBuildingException 如果在构建或恢复状态后端时发生错误，则抛出此异常
    */
    @Override
    public DefaultOperatorStateBackend build() throws BackendBuildingException {
        // 存储已注册的操作符分区列表状态的映射
        Map<String, PartitionableListState<?>> registeredOperatorStates = new HashMap<>();
        // 存储已注册的广播状态的映射
        Map<String, BackendWritableBroadcastState<?, ?>> registeredBroadcastStates =
                new HashMap<>();
        // 用于管理需要关闭的流的注册中心
        CloseableRegistry cancelStreamRegistryForBackend = new CloseableRegistry();
       // 获取流压缩装饰器，用于在状态快照时进行压缩
        final StreamCompressionDecorator compressionDecorator =
                AbstractStateBackend.getCompressionDecorator(executionConfig);
        // 创建状态快照策略，该策略包含了如何快照已注册的状态和如何进行压缩的逻辑
        DefaultOperatorStateBackendSnapshotStrategy snapshotStrategy =
                new DefaultOperatorStateBackendSnapshotStrategy(
                        userClassloader,
                        registeredOperatorStates,
                        registeredBroadcastStates,
                        compressionDecorator);
        // 创建状态恢复操作，该操作负责从持久化存储中恢复状态
        OperatorStateRestoreOperation restoreOperation =
                new OperatorStateRestoreOperation(
                        cancelStreamRegistry,
                        userClassloader,
                        registeredOperatorStates,
                        registeredBroadcastStates,
                        restoreStateHandles);
        try {
            // 尝试恢复状态
            restoreOperation.restore();
        } catch (Exception e) {
            IOUtils.closeQuietly(cancelStreamRegistryForBackend);
            throw new BackendBuildingException(
                    "Failed when trying to restore operator state backend", e);
        }
        // 构建 DefaultOperatorStateBackend 实例
        return new DefaultOperatorStateBackend(
                executionConfig,
                cancelStreamRegistryForBackend,
                registeredOperatorStates,
                registeredBroadcastStates,
                new HashMap<>(),
                new HashMap<>(),
                new SnapshotStrategyRunner<>(
                        "DefaultOperatorStateBackend snapshot",
                        snapshotStrategy,
                        cancelStreamRegistryForBackend,
                        asynchronousSnapshots ? ASYNCHRONOUS : SYNCHRONOUS));
    }
}
