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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.InternalCheckpointListener;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateFactory;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base implementation of KeyedStateBackend. The state can be checkpointed to streams using {@link
 * #snapshot(long, long, CheckpointStreamFactory, CheckpointOptions)}.
 *
 * @param <K> Type of the key by which state is keyed.
 */
public abstract class AbstractKeyedStateBackend<K>
        implements CheckpointableKeyedStateBackend<K>,
                InternalCheckpointListener,
                TestableKeyedStateBackend<K>,
                InternalKeyContext<K> {

    /** The key serializer. */
    protected final TypeSerializer<K> keySerializer;

    /** Listeners to changes of ({@link #keyContext}). */
    private final ArrayList<KeySelectionListener<K>> keySelectionListeners;

    /** So that we can give out state when the user uses the same key. */
    private final HashMap<String, InternalKvState<K, ?, ?>> keyValueStatesByName;

    /** For caching the last accessed partitioned state. */
    private String lastName;

    @SuppressWarnings("rawtypes")
    private InternalKvState lastState;

    /** The number of key-groups aka max parallelism. */
    protected final int numberOfKeyGroups;

    /** Range of key-groups for which this backend is responsible. */
    protected final KeyGroupRange keyGroupRange;

    /** KvStateRegistry helper for this task. */
    protected final TaskKvStateRegistry kvStateRegistry;

    /**
     * Registry for all opened streams, so they can be closed if the task using this backend is
     * closed.
     */
    protected CloseableRegistry cancelStreamRegistry;

    protected final ClassLoader userCodeClassLoader;

    private final ExecutionConfig executionConfig;

    protected final TtlTimeProvider ttlTimeProvider;

    protected final LatencyTrackingStateConfig latencyTrackingStateConfig;

    /** Decorates the input and output streams to write key-groups compressed. */
    protected final StreamCompressionDecorator keyGroupCompressionDecorator;

    /** The key context for this backend. */
    protected final InternalKeyContext<K> keyContext;

    public AbstractKeyedStateBackend(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            CloseableRegistry cancelStreamRegistry,
            InternalKeyContext<K> keyContext) {
        this(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                cancelStreamRegistry,
                determineStreamCompression(executionConfig),
                keyContext);
    }

    public AbstractKeyedStateBackend(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            CloseableRegistry cancelStreamRegistry,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            InternalKeyContext<K> keyContext) {
        this(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                cancelStreamRegistry,
                keyGroupCompressionDecorator,
                Preconditions.checkNotNull(keyContext),
                keyContext.getNumberOfKeyGroups(),
                keyContext.getKeyGroupRange(),
                new HashMap<>(),
                new ArrayList<>(1),
                null,
                null);
    }

    // Copy constructor
    protected AbstractKeyedStateBackend(AbstractKeyedStateBackend<K> abstractKeyedStateBackend) {
        this(
                abstractKeyedStateBackend.kvStateRegistry,
                abstractKeyedStateBackend.keySerializer,
                abstractKeyedStateBackend.userCodeClassLoader,
                abstractKeyedStateBackend.executionConfig,
                abstractKeyedStateBackend.ttlTimeProvider,
                abstractKeyedStateBackend.latencyTrackingStateConfig,
                abstractKeyedStateBackend.cancelStreamRegistry,
                abstractKeyedStateBackend.keyGroupCompressionDecorator,
                abstractKeyedStateBackend.keyContext,
                abstractKeyedStateBackend.numberOfKeyGroups,
                abstractKeyedStateBackend.keyGroupRange,
                abstractKeyedStateBackend.keyValueStatesByName,
                abstractKeyedStateBackend.keySelectionListeners,
                abstractKeyedStateBackend.lastState,
                abstractKeyedStateBackend.lastName);
    }

    @SuppressWarnings("rawtypes")
    private AbstractKeyedStateBackend(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            CloseableRegistry cancelStreamRegistry,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            InternalKeyContext<K> keyContext,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            HashMap<String, InternalKvState<K, ?, ?>> keyValueStatesByName,
            ArrayList<KeySelectionListener<K>> keySelectionListeners,
            InternalKvState lastState,
            String lastName) {
        this.keyContext = Preconditions.checkNotNull(keyContext);
        this.numberOfKeyGroups = numberOfKeyGroups;
        this.keyGroupRange = Preconditions.checkNotNull(keyGroupRange);
        Preconditions.checkArgument(
                numberOfKeyGroups >= 1, "NumberOfKeyGroups must be a positive number");
        Preconditions.checkArgument(
                numberOfKeyGroups >= keyGroupRange.getNumberOfKeyGroups(),
                "The total number of key groups must be at least the number in the key group range assigned to this backend. "
                        + "The total number of key groups: %s, the number in key groups in range: %s",
                numberOfKeyGroups,
                keyGroupRange.getNumberOfKeyGroups());

        this.kvStateRegistry = kvStateRegistry;
        this.keySerializer = keySerializer;
        this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
        this.cancelStreamRegistry = cancelStreamRegistry;
        this.keyValueStatesByName = keyValueStatesByName;
        this.executionConfig = executionConfig;
        this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
        this.ttlTimeProvider = Preconditions.checkNotNull(ttlTimeProvider);
        this.latencyTrackingStateConfig = Preconditions.checkNotNull(latencyTrackingStateConfig);
        this.keySelectionListeners = keySelectionListeners;
        this.lastState = lastState;
        this.lastName = lastName;
    }

    private static StreamCompressionDecorator determineStreamCompression(
            ExecutionConfig executionConfig) {
        if (executionConfig != null && executionConfig.isUseSnapshotCompression()) {
            return SnappyStreamCompressionDecorator.INSTANCE;
        } else {
            return UncompressedStreamCompressionDecorator.INSTANCE;
        }
    }

    @Override
    public void notifyCheckpointSubsumed(long checkpointId) throws Exception {}

    /**
     * Closes the state backend, releasing all internal resources, but does not delete any
     * persistent checkpoint data.
     */
    @Override
    public void dispose() {

        IOUtils.closeQuietly(cancelStreamRegistry);

        if (kvStateRegistry != null) {
            kvStateRegistry.unregisterAll();
        }

        lastName = null;
        lastState = null;
        keyValueStatesByName.clear();
    }

    /** @see KeyedStateBackend */
    @Override
    public void setCurrentKey(K newKey) {
        notifyKeySelected(newKey);
        this.keyContext.setCurrentKey(newKey);
        this.keyContext.setCurrentKeyGroupIndex(
                KeyGroupRangeAssignment.assignToKeyGroup(newKey, numberOfKeyGroups));
    }

    private void notifyKeySelected(K newKey) {
        // we prefer a for-loop over other iteration schemes for performance reasons here.
        for (int i = 0; i < keySelectionListeners.size(); ++i) {
            keySelectionListeners.get(i).keySelected(newKey);
        }
    }

    @Override
    public void registerKeySelectionListener(KeySelectionListener<K> listener) {
        keySelectionListeners.add(listener);
    }

    @Override
    public boolean deregisterKeySelectionListener(KeySelectionListener<K> listener) {
        return keySelectionListeners.remove(listener);
    }

    /** @see KeyedStateBackend */
    @Override
    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    /** @see KeyedStateBackend */
    @Override
    public K getCurrentKey() {
        return this.keyContext.getCurrentKey();
    }

    /** @see KeyedStateBackend */
    public int getCurrentKeyGroupIndex() {
        return this.keyContext.getCurrentKeyGroupIndex();
    }

    /** @see KeyedStateBackend */
    public int getNumberOfKeyGroups() {
        return numberOfKeyGroups;
    }

    /** @see KeyedStateBackend */
    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    /** @see KeyedStateBackend */
    @Override
    public <N, S extends State, T> void applyToAllKeys(
            final N namespace,
            final TypeSerializer<N> namespaceSerializer,
            final StateDescriptor<S, T> stateDescriptor,
            final KeyedStateFunction<K, S> function)
            throws Exception {

        applyToAllKeys(
                namespace,
                namespaceSerializer,
                stateDescriptor,
                function,
                this::getPartitionedState);
    }

    public <N, S extends State, T> void applyToAllKeys(
            final N namespace,
            final TypeSerializer<N> namespaceSerializer,
            final StateDescriptor<S, T> stateDescriptor,
            final KeyedStateFunction<K, S> function,
            final PartitionStateFactory partitionStateFactory)
            throws Exception {

        try (Stream<K> keyStream = getKeys(stateDescriptor.getName(), namespace)) {

            final S state =
                    partitionStateFactory.get(namespace, namespaceSerializer, stateDescriptor);

            keyStream.forEach(
                    (K key) -> {
                        setCurrentKey(key);
                        try {
                            function.process(key, state);
                        } catch (Throwable e) {
                            // we wrap the checked exception in an unchecked
                            // one and catch it (and re-throw it) later.
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    /** @see KeyedStateBackend */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建KeyedState
    */
    @Override
    @SuppressWarnings("unchecked")
    public <N, S extends State, V> S getOrCreateKeyedState(
            final TypeSerializer<N> namespaceSerializer, StateDescriptor<S, V> stateDescriptor)
            throws Exception {
        // 检查命名空间序列化器是否为null
        checkNotNull(namespaceSerializer, "Namespace serializer");
        // 检查键序列化器
        checkNotNull(
                keySerializer,
                "State key serializer has not been configured in the config. "
                        + "This operation cannot use partitioned state.");
        // 尝试从缓存中获取与状态描述符名称关联的内部键值状态
        InternalKvState<K, ?, ?> kvState = keyValueStatesByName.get(stateDescriptor.getName());
        // 如果缓存中没有找到状态
        if (kvState == null) {
            // 如果状态描述符的序列化器尚未初始化
            if (!stateDescriptor.isSerializerInitialized()) {
                // 使用执行配置来初始化状态描述符的序列化器（如果尚未设置）
                stateDescriptor.initializeSerializerUnlessSet(executionConfig);
            }
            // 创建一个具有TTL（如果启用）和延迟跟踪（如果启用）的内部键值状态
            kvState =
                    LatencyTrackingStateFactory.createStateAndWrapWithLatencyTrackingIfEnabled(
                            TtlStateFactory.createStateAndWrapWithTtlIfEnabled(
                                    namespaceSerializer, stateDescriptor, this, ttlTimeProvider),
                            stateDescriptor,
                            latencyTrackingStateConfig);
            // 将新创建的状态添加到缓存中
            keyValueStatesByName.put(stateDescriptor.getName(), kvState);
            // 如果状态可以被查询，则发布它
            publishQueryableStateIfEnabled(stateDescriptor, kvState);
        }
        // 将内部键值状态强制转换为指定的状态类型并返回
        return (S) kvState;
    }

    public void publishQueryableStateIfEnabled(
            StateDescriptor<?, ?> stateDescriptor, InternalKvState<?, ?, ?> kvState) {
        if (stateDescriptor.isQueryable()) {
            if (kvStateRegistry == null) {
                throw new IllegalStateException("State backend has not been initialized for job.");
            }
            String name = stateDescriptor.getQueryableStateName();
            kvStateRegistry.registerKvState(keyGroupRange, name, kvState, userCodeClassLoader);
        }
    }

    /**
     * TODO: NOTE: This method does a lot of work caching / retrieving states just to update the
     * namespace. This method should be removed for the sake of namespaces being lazily fetched from
     * the keyed state backend, or being set on the state directly.
     *
     * @see KeyedStateBackend
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 从给定的命名空间中获取或创建分区状态。
     * 注意：此方法目前为了更新命名空间而执行了大量的状态缓存/检索工作。
     * 为了优化，命名空间应该被延迟地从KeyedStateBackend中检索，或者直接在状态上设置。
     *
     * @param namespace 命名空间，用于区分不同的状态分区
     * @param namespaceSerializer 命名空间的序列化器
     * @param stateDescriptor 状态的描述符
     * @param <N> 命名空间的类型
     * @param <S> 状态的类型，必须是State的子类
     * @return 与给定命名空间和状态描述符匹配的状态对象
     * @throws Exception 如果在获取或创建状态过程中发生异常
     *
     * @see KeyedStateBackend 用于存储和检索与键关联的状态的组件
    */
    @SuppressWarnings("unchecked")
    @Override
    public <N, S extends State> S getPartitionedState(
            final N namespace,
            final TypeSerializer<N> namespaceSerializer,
            final StateDescriptor<S, ?> stateDescriptor)
            throws Exception {
        // 检查命名空间是否为null
        checkNotNull(namespace, "Namespace");
        // 如果上一个请求的状态描述符名称与当前请求的状态描述符名称相同
        // 则直接更新当前状态的命名空间并返回上一个状态
        if (lastName != null && lastName.equals(stateDescriptor.getName())) {
            lastState.setCurrentNamespace(namespace);
            return (S) lastState;
        }
        // 尝试从缓存中根据状态描述符名称获取之前的状态
        InternalKvState<K, ?, ?> previous = keyValueStatesByName.get(stateDescriptor.getName());
        if (previous != null) {
            // 如果找到，更新为当前状态并设置命名空间
            lastState = previous;
            lastState.setCurrentNamespace(namespace);
            lastName = stateDescriptor.getName();
            // 返回状态，因为无需再次创建
            return (S) previous;
        }
        // 如果缓存中没有找到状态，则创建新的状态
        final S state = getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
        // 假设getOrCreateKeyedState返回的是InternalKvState的实例
        final InternalKvState<K, N, ?> kvState = (InternalKvState<K, N, ?>) state;
        // 更新最近请求的状态名称和状态对象
        lastName = stateDescriptor.getName();
        lastState = kvState;
        // 设置命名空间并返回新创建的状态
        kvState.setCurrentNamespace(namespace);

        return state;
    }

    @Override
    public void close() throws IOException {
        cancelStreamRegistry.close();
    }

    public LatencyTrackingStateConfig getLatencyTrackingStateConfig() {
        return latencyTrackingStateConfig;
    }

    @VisibleForTesting
    public StreamCompressionDecorator getKeyGroupCompressionDecorator() {
        return keyGroupCompressionDecorator;
    }

    @VisibleForTesting
    public int numKeyValueStatesByName() {
        return keyValueStatesByName.size();
    }

    // TODO remove this once heap-based timers are working with RocksDB incremental snapshots!
    public boolean requiresLegacySynchronousTimerSnapshots(SnapshotType checkpointType) {
        return false;
    }

    public InternalKeyContext<K> getKeyContext() {
        return keyContext;
    }

    public interface PartitionStateFactory {
        <N, S extends State> S get(
                final N namespace,
                final TypeSerializer<N> namespaceSerializer,
                final StateDescriptor<S, ?> stateDescriptor)
                throws Exception;
    }

    @Override
    public void setCurrentKeyGroupIndex(int currentKeyGroupIndex) {
        keyContext.setCurrentKeyGroupIndex(currentKeyGroupIndex);
    }
}
