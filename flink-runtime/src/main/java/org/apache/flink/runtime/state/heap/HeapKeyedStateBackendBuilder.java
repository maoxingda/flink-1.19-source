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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.InternalKeyContextImpl;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.RestoreOperation;
import org.apache.flink.runtime.state.SavepointKeyedStateHandle;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.runtime.state.SnapshotExecutionType.ASYNCHRONOUS;
import static org.apache.flink.runtime.state.SnapshotExecutionType.SYNCHRONOUS;

/**
 * Builder class for {@link HeapKeyedStateBackend} which handles all necessary initializations and
 * clean ups.
 *
 * @param <K> The data type that the key serializer serializes.
 */
public class HeapKeyedStateBackendBuilder<K> extends AbstractKeyedStateBackendBuilder<K> {
    /** The configuration of local recovery. */
    private final LocalRecoveryConfig localRecoveryConfig;
    /** Factory for state that is organized as priority queue. */
    private final HeapPriorityQueueSetFactory priorityQueueSetFactory;
    /** Whether asynchronous snapshot is enabled. */
    private final boolean asynchronousSnapshots;

    public HeapKeyedStateBackendBuilder(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            LocalRecoveryConfig localRecoveryConfig,
            HeapPriorityQueueSetFactory priorityQueueSetFactory,
            boolean asynchronousSnapshots,
            CloseableRegistry cancelStreamRegistry) {
        super(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                numberOfKeyGroups,
                keyGroupRange,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                stateHandles,
                keyGroupCompressionDecorator,
                cancelStreamRegistry);
        this.localRecoveryConfig = localRecoveryConfig;
        this.priorityQueueSetFactory = priorityQueueSetFactory;
        this.asynchronousSnapshots = asynchronousSnapshots;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 构建HeapKeyedStateBackend
    */
    @Override
    public HeapKeyedStateBackend<K> build() throws BackendBuildingException {
        // Map of registered Key/Value states
        // 注册的键值状态的映射（键对应其状态表）
        // 映射关系，保存了已注册的Key/Value状态
        Map<String, StateTable<K, ?, ?>> registeredKVStates = new HashMap<>();
        // Map of registered priority queue set states
        // 注册的优先级队列集合状态的映射
        // 映射关系，保存了已注册的优先级队列集合状态
        Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates =
                new HashMap<>();
        // 后端使用的可关闭资源注册表
        CloseableRegistry cancelStreamRegistryForBackend = new CloseableRegistry();
        // 初始化快照策略，该策略将用于生成和恢复状态的快照
        HeapSnapshotStrategy<K> snapshotStrategy =
                initSnapshotStrategy(registeredKVStates, registeredPQStates);
        // 创建一个内部键上下文，用于管理键的分组和范围
        InternalKeyContext<K> keyContext =
                new InternalKeyContextImpl<>(keyGroupRange, numberOfKeyGroups);
        // 创建状态表的工厂类，用于创建状态表
        final StateTableFactory<K> stateTableFactory = CopyOnWriteStateTable::new;
        // 从检查点或其他存储中恢复已注册的状态
        restoreState(registeredKVStates, registeredPQStates, keyContext, stateTableFactory);
        // 创建一个HeapKeyedStateBackend实例，并传入所有必要的参数
        return new HeapKeyedStateBackend<>(
                kvStateRegistry, // 键值状态注册表
                keySerializerProvider.currentSchemaSerializer(),// 当前模式的键序列化器
                userCodeClassLoader,// 用户代码类加载器
                executionConfig,// 执行配置
                ttlTimeProvider, // TTL时间提供者
                latencyTrackingStateConfig,// 延迟跟踪状态配置
                cancelStreamRegistryForBackend,// 后端使用的可关闭资源注册表
                keyGroupCompressionDecorator,// 键组压缩装饰器
                registeredKVStates,// 已注册的键值状态
                registeredPQStates,// 已注册的优先级队列集合状态
                localRecoveryConfig,// 本地恢复配置
                priorityQueueSetFactory,// 优先级队列集合工厂
                snapshotStrategy,// 快照策略
                asynchronousSnapshots ? ASYNCHRONOUS : SYNCHRONOUS, // 是否使用异步快照的标志
                stateTableFactory,// 状态表工厂
                keyContext);// 键上下文
    }

    private void restoreState(
            Map<String, StateTable<K, ?, ?>> registeredKVStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            InternalKeyContext<K> keyContext,
            StateTableFactory<K> stateTableFactory)
            throws BackendBuildingException {
        final RestoreOperation<Void> restoreOperation;

        final KeyedStateHandle firstHandle;
        if (restoreStateHandles.isEmpty()) {
            firstHandle = null;
        } else {
            firstHandle = restoreStateHandles.iterator().next();
        }
        if (firstHandle instanceof SavepointKeyedStateHandle) {
            restoreOperation =
                    new HeapSavepointRestoreOperation<>(
                            restoreStateHandles,
                            keySerializerProvider,
                            userCodeClassLoader,
                            registeredKVStates,
                            registeredPQStates,
                            priorityQueueSetFactory,
                            keyGroupRange,
                            numberOfKeyGroups,
                            stateTableFactory,
                            keyContext);
        } else {
            restoreOperation =
                    new HeapRestoreOperation<>(
                            restoreStateHandles,
                            keySerializerProvider,
                            userCodeClassLoader,
                            registeredKVStates,
                            registeredPQStates,
                            cancelStreamRegistry,
                            priorityQueueSetFactory,
                            keyGroupRange,
                            numberOfKeyGroups,
                            stateTableFactory,
                            keyContext);
        }
        try {
            restoreOperation.restore();
            logger.info("Finished to build heap keyed state-backend.");
        } catch (Exception e) {
            throw new BackendBuildingException("Failed when trying to restore heap backend", e);
        }
    }

    private HeapSnapshotStrategy<K> initSnapshotStrategy(
            Map<String, StateTable<K, ?, ?>> registeredKVStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates) {
        return new HeapSnapshotStrategy<>(
                registeredKVStates,
                registeredPQStates,
                keyGroupCompressionDecorator,
                localRecoveryConfig,
                keyGroupRange,
                keySerializerProvider,
                numberOfKeyGroups);
    }
}
