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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.HeapPriorityQueuesManager;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotExecutionType;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.SnapshotStrategyRunner;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;
import org.apache.flink.runtime.state.StateSnapshotTransformers;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.StateMigrationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link AbstractKeyedStateBackend} that keeps state on the Java Heap and will serialize state to
 * streams provided by a {@link CheckpointStreamFactory} upon checkpointing.
 *
 * @param <K> The key by which state is keyed.
 */
public class HeapKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

    private static final Logger LOG = LoggerFactory.getLogger(HeapKeyedStateBackend.class);

    private static final Map<StateDescriptor.Type, StateCreateFactory> STATE_CREATE_FACTORIES =
            Stream.of(
                            Tuple2.of(
                                    StateDescriptor.Type.VALUE,
                                    (StateCreateFactory) HeapValueState::create),
                            Tuple2.of(
                                    StateDescriptor.Type.LIST,
                                    (StateCreateFactory) HeapListState::create),
                            Tuple2.of(
                                    StateDescriptor.Type.MAP,
                                    (StateCreateFactory) HeapMapState::create),
                            Tuple2.of(
                                    StateDescriptor.Type.AGGREGATING,
                                    (StateCreateFactory) HeapAggregatingState::create),
                            Tuple2.of(
                                    StateDescriptor.Type.REDUCING,
                                    (StateCreateFactory) HeapReducingState::create))
                    .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    private static final Map<StateDescriptor.Type, StateUpdateFactory> STATE_UPDATE_FACTORIES =
            Stream.of(
                            Tuple2.of(
                                    StateDescriptor.Type.VALUE,
                                    (StateUpdateFactory) HeapValueState::update),
                            Tuple2.of(
                                    StateDescriptor.Type.LIST,
                                    (StateUpdateFactory) HeapListState::update),
                            Tuple2.of(
                                    StateDescriptor.Type.MAP,
                                    (StateUpdateFactory) HeapMapState::update),
                            Tuple2.of(
                                    StateDescriptor.Type.AGGREGATING,
                                    (StateUpdateFactory) HeapAggregatingState::update),
                            Tuple2.of(
                                    StateDescriptor.Type.REDUCING,
                                    (StateUpdateFactory) HeapReducingState::update))
                    .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    /** Map of created Key/Value states. */
    private final Map<String, State> createdKVStates;

    /** Map of registered Key/Value states. */
    private final Map<String, StateTable<K, ?, ?>> registeredKVStates;

    /** The configuration for local recovery. */
    private final LocalRecoveryConfig localRecoveryConfig;

    /** The snapshot strategy for this backend. */
    private final SnapshotStrategy<KeyedStateHandle, ?> checkpointStrategy;

    private final SnapshotExecutionType snapshotExecutionType;

    private final StateTableFactory<K> stateTableFactory;

    /** Factory for state that is organized as priority queue. */
    private final HeapPriorityQueuesManager priorityQueuesManager;

    public HeapKeyedStateBackend(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            CloseableRegistry cancelStreamRegistry,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            Map<String, StateTable<K, ?, ?>> registeredKVStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            LocalRecoveryConfig localRecoveryConfig,
            HeapPriorityQueueSetFactory priorityQueueSetFactory,
            HeapSnapshotStrategy<K> checkpointStrategy,
            SnapshotExecutionType snapshotExecutionType,
            StateTableFactory<K> stateTableFactory,
            InternalKeyContext<K> keyContext) {
        super(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                cancelStreamRegistry,
                keyGroupCompressionDecorator,
                keyContext);
        this.registeredKVStates = registeredKVStates;
        this.createdKVStates = new HashMap<>();
        this.localRecoveryConfig = localRecoveryConfig;
        this.checkpointStrategy = checkpointStrategy;
        this.snapshotExecutionType = snapshotExecutionType;
        this.stateTableFactory = stateTableFactory;
        this.priorityQueuesManager =
                new HeapPriorityQueuesManager(
                        registeredPQStates,
                        priorityQueueSetFactory,
                        keyContext.getKeyGroupRange(),
                        keyContext.getNumberOfKeyGroups());
        LOG.info("Initializing heap keyed state backend with stream factory.");
    }

    // ------------------------------------------------------------------------
    //  state backend operations
    // ------------------------------------------------------------------------

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        return priorityQueuesManager.createOrUpdate(stateName, byteOrderedElementSerializer);
    }

    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer,
                    boolean allowFutureMetadataUpdates) {
        return priorityQueuesManager.createOrUpdate(
                stateName, byteOrderedElementSerializer, allowFutureMetadataUpdates);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 注册StateTable
     * @param namespaceSerializer 命名空间序列化器
     * @param stateDesc 状态描述符
     * @param snapshotTransformFactory 状态快照转换工厂
     * @param allowFutureMetadataUpdates 是否允许未来的元数据更新
    */
    private <N, V> StateTable<K, N, V> tryRegisterStateTable(
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<?, V> stateDesc,
            @Nonnull StateSnapshotTransformFactory<V> snapshotTransformFactory,
            boolean allowFutureMetadataUpdates)
            throws StateMigrationException {

        // 尝试从已注册的状态表中获取指定名称的状态表（此处进行了类型强制转换）
        @SuppressWarnings("unchecked")
        StateTable<K, N, V> stateTable =
                (StateTable<K, N, V>) registeredKVStates.get(stateDesc.getName());
        // 获取状态描述符中定义的状态值序列化器
        TypeSerializer<V> newStateSerializer = stateDesc.getSerializer();

        if (stateTable != null) {
            // 获取已恢复的状态表的元信息
            RegisteredKeyValueStateBackendMetaInfo<N, V> restoredKvMetaInfo =
                    stateTable.getMetaInfo();
            // 更新状态表的快照转换工厂
            restoredKvMetaInfo.updateSnapshotTransformFactory(snapshotTransformFactory);

            // fetch current serializer now because if it is incompatible, we can't access
            // it anymore to improve the error message
            // 现在获取当前命名空间序列化器，因为如果它与旧的序列化器不兼容，我们可能无法再次访问它
            // 以此来改进错误消息
            TypeSerializer<N> previousNamespaceSerializer =
                    restoredKvMetaInfo.getNamespaceSerializer();
            // 检查新的命名空间序列化器与旧的序列化器是否兼容
            TypeSerializerSchemaCompatibility<N> namespaceCompatibility =
                    restoredKvMetaInfo.updateNamespaceSerializer(namespaceSerializer);
            if (namespaceCompatibility.isCompatibleAfterMigration()
                    || namespaceCompatibility.isIncompatible()) {
                // 如果在迁移后兼容或完全不兼容，则抛出异常
                throw new StateMigrationException(
                        "For heap backends, the new namespace serializer ("
                                + namespaceSerializer
                                + ") must be compatible with the old namespace serializer ("
                                + previousNamespaceSerializer
                                + ").");
            }
            // 检查状态表的元信息是否与状态描述符匹配
            restoredKvMetaInfo.checkStateMetaInfo(stateDesc);

            // fetch current serializer now because if it is incompatible, we can't access
            // it anymore to improve the error message
            // 获取之前的状态值序列化器
            TypeSerializer<V> previousStateSerializer = restoredKvMetaInfo.getStateSerializer();
            // 检查新的状态序列化器与旧的序列化器是否兼容
            TypeSerializerSchemaCompatibility<V> stateCompatibility =
                    restoredKvMetaInfo.updateStateSerializer(newStateSerializer);

            if (stateCompatibility.isIncompatible()) {
                // 如果不兼容，则抛出异常
                throw new StateMigrationException(
                        "For heap backends, the new state serializer ("
                                + newStateSerializer
                                + ") must not be incompatible with the old state serializer ("
                                + previousStateSerializer
                                + ").");
            }
            // 根据allowFutureMetadataUpdates的值更新元信息的序列化器升级状态
            restoredKvMetaInfo =
                    allowFutureMetadataUpdates
                            ? restoredKvMetaInfo.withSerializerUpgradesAllowed()
                            : restoredKvMetaInfo;
            // 更新状态表的元信息
            stateTable.setMetaInfo(restoredKvMetaInfo);
        } else {
            // 如果StateTable不存在，则创建一个新的RegisteredKeyValueStateBackendMetaInfo
            RegisteredKeyValueStateBackendMetaInfo<N, V> newMetaInfo =
                    new RegisteredKeyValueStateBackendMetaInfo<>(
                            stateDesc.getType(),
                            stateDesc.getName(),
                            namespaceSerializer,
                            newStateSerializer,
                            snapshotTransformFactory);
            // 根据allowFutureMetadataUpdates的值更新新元信息的序列化器升级状态
            newMetaInfo =
                    allowFutureMetadataUpdates
                            ? newMetaInfo.withSerializerUpgradesAllowed()
                            : newMetaInfo;
            // 使用新的元信息创建一个新的StateTable
            stateTable = stateTableFactory.newStateTable(keyContext, newMetaInfo, keySerializer);
            // 将新的StateTable注册到registeredKVStates中
            registeredKVStates.put(stateDesc.getName(), stateTable);
        }
        // 返回已存在或新创建的StateTable
        return stateTable;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        if (!registeredKVStates.containsKey(state)) {
            return Stream.empty();
        }

        final StateSnapshotRestore stateSnapshotRestore = registeredKVStates.get(state);
        StateTable<K, N, ?> table = (StateTable<K, N, ?>) stateSnapshotRestore;
        return table.getKeys(namespace);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
        if (!registeredKVStates.containsKey(state)) {
            return Stream.empty();
        }

        final StateSnapshotRestore stateSnapshotRestore = registeredKVStates.get(state);
        StateTable<K, N, ?> table = (StateTable<K, N, ?>) stateSnapshotRestore;
        return table.getKeysAndNamespaces();
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 调用内部方法createOrUpdateInternalState创建State
    */
    @Override
    @Nonnull
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory)
            throws Exception {
        return createOrUpdateInternalState(
                namespaceSerializer, stateDesc, snapshotTransformFactory, false);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 根据给定的参数创建或更新内部状态。
     *
     * @param namespaceSerializer 命名空间的序列化器，不能为null
     * @param stateDesc 状态描述符，定义了状态的名称、类型等属性，不能为null
     * @param snapshotTransformFactory 状态快照转换工厂，用于生成状态快照的转换逻辑，不能为null
     * @param allowFutureMetadataUpdates 是否允许未来的元数据更新
     * @param <N> 命名空间的类型
     * @param <SV> 状态值的类型
     * @param <SEV> 状态快照值的类型
     * @param <S> 状态对象的基类
     * @param <IS> 具体的状态对象类型，继承自S
     * @return 更新或创建的状态对象
     * @throws Exception 如果在创建或更新状态过程中发生异常
    */
    @Override
    @Nonnull
    public <N, SV, SEV, S extends State, IS extends S> IS createOrUpdateInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformFactory<SEV> snapshotTransformFactory,
            boolean allowFutureMetadataUpdates)
            throws Exception {
        // 尝试注册状态表，并获取状态表实例
        StateTable<K, N, SV> stateTable =
                tryRegisterStateTable(
                        namespaceSerializer,
                        stateDesc,
                        getStateSnapshotTransformFactory(stateDesc, snapshotTransformFactory),
                        allowFutureMetadataUpdates);

        // 从已创建的状态映射中获取指定名称的状态对象（此处进行了类型强制转换）
        @SuppressWarnings("unchecked")
        IS createdState = (IS) createdKVStates.get(stateDesc.getName());
        // 如果状态对象为空（即尚未创建）
        if (createdState == null) {
            // 从状态类型映射中获取状态创建工厂
            StateCreateFactory stateCreateFactory = STATE_CREATE_FACTORIES.get(stateDesc.getType());
            // 如果找不到对应的创建工厂，则抛出异常
            if (stateCreateFactory == null) {
                throw new FlinkRuntimeException(stateNotSupportedMessage(stateDesc));
            }
            // 使用状态描述符、状态表和键序列化器创建状态对象
            createdState =
                    stateCreateFactory.createState(stateDesc, stateTable, getKeySerializer());
        } else {
            // 如果状态对象已存在（需要更新）
            // 从状态类型映射中获取状态更新工厂
            StateUpdateFactory stateUpdateFactory = STATE_UPDATE_FACTORIES.get(stateDesc.getType());
            // 如果找不到对应的更新工厂，则抛出异常
            if (stateUpdateFactory == null) {
                throw new FlinkRuntimeException(stateNotSupportedMessage(stateDesc));
            }
            // 使用状态描述符、状态表和当前状态对象更新状态
            createdState = stateUpdateFactory.updateState(stateDesc, stateTable, createdState);
        }
        // 将更新或创建的状态对象放入已创建的状态映射中
        createdKVStates.put(stateDesc.getName(), createdState);
        // 返回更新或创建的状态对象
        return createdState;
    }

    private <S extends State, SV> String stateNotSupportedMessage(
            StateDescriptor<S, SV> stateDesc) {
        return String.format(
                "State %s is not supported by %s", stateDesc.getClass(), this.getClass());
    }

    @SuppressWarnings("unchecked")
    private <SV, SEV> StateSnapshotTransformFactory<SV> getStateSnapshotTransformFactory(
            StateDescriptor<?, SV> stateDesc,
            StateSnapshotTransformFactory<SEV> snapshotTransformFactory) {
        if (stateDesc instanceof ListStateDescriptor) {
            return (StateSnapshotTransformFactory<SV>)
                    new StateSnapshotTransformers.ListStateSnapshotTransformFactory<>(
                            snapshotTransformFactory);
        } else if (stateDesc instanceof MapStateDescriptor) {
            return (StateSnapshotTransformFactory<SV>)
                    new StateSnapshotTransformers.MapStateSnapshotTransformFactory<>(
                            snapshotTransformFactory);
        } else {
            return (StateSnapshotTransformFactory<SV>) snapshotTransformFactory;
        }
    }

    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            final long checkpointId,
            final long timestamp,
            @Nonnull final CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {

        SnapshotStrategyRunner<KeyedStateHandle, ?> snapshotStrategyRunner =
                new SnapshotStrategyRunner<>(
                        "Heap backend snapshot",
                        checkpointStrategy,
                        cancelStreamRegistry,
                        snapshotExecutionType);
        return snapshotStrategyRunner.snapshot(
                checkpointId, timestamp, streamFactory, checkpointOptions);
    }

    @Nonnull
    @Override
    public SavepointResources<K> savepoint() {

        HeapSnapshotResources<K> snapshotResources =
                HeapSnapshotResources.create(
                        registeredKVStates,
                        priorityQueuesManager.getRegisteredPQStates(),
                        keyGroupCompressionDecorator,
                        keyGroupRange,
                        keySerializer,
                        numberOfKeyGroups);

        return new SavepointResources<>(snapshotResources, snapshotExecutionType);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // Nothing to do
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        // nothing to do
    }

    @Override
    public <N, S extends State, T> void applyToAllKeys(
            final N namespace,
            final TypeSerializer<N> namespaceSerializer,
            final StateDescriptor<S, T> stateDescriptor,
            final KeyedStateFunction<K, S> function,
            final PartitionStateFactory partitionStateFactory)
            throws Exception {

        try (Stream<K> keyStream = getKeys(stateDescriptor.getName(), namespace)) {

            // we copy the keys into list to avoid the concurrency problem
            // when state.clear() is invoked in function.process().
            final List<K> keys = keyStream.collect(Collectors.toList());

            final S state =
                    partitionStateFactory.get(namespace, namespaceSerializer, stateDescriptor);

            for (K key : keys) {
                setCurrentKey(key);
                function.process(key, state);
            }
        }
    }

    @Override
    public String toString() {
        return "HeapKeyedStateBackend";
    }

    /** Returns the total number of state entries across all keys/namespaces. */
    @VisibleForTesting
    @Override
    public int numKeyValueStateEntries() {
        int sum = 0;
        for (StateSnapshotRestore state : registeredKVStates.values()) {
            sum += ((StateTable<?, ?, ?>) state).size();
        }
        return sum;
    }

    /** Returns the total number of state entries across all keys for the given namespace. */
    @VisibleForTesting
    public int numKeyValueStateEntries(Object namespace) {
        int sum = 0;
        for (StateTable<?, ?, ?> state : registeredKVStates.values()) {
            sum += state.sizeOfNamespace(namespace);
        }
        return sum;
    }

    @VisibleForTesting
    public LocalRecoveryConfig getLocalRecoveryConfig() {
        return localRecoveryConfig;
    }

    private interface StateCreateFactory {
        <K, N, SV, S extends State, IS extends S> IS createState(
                StateDescriptor<S, SV> stateDesc,
                StateTable<K, N, SV> stateTable,
                TypeSerializer<K> keySerializer)
                throws Exception;
    }

    private interface StateUpdateFactory {
        <K, N, SV, S extends State, IS extends S> IS updateState(
                StateDescriptor<S, SV> stateDesc, StateTable<K, N, SV> stateTable, IS existingState)
                throws Exception;
    }
}
