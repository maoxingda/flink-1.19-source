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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RunnableFuture;

/** Default implementation of OperatorStateStore that provides the ability to make snapshots. */
@Internal
public class DefaultOperatorStateBackend implements OperatorStateBackend {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultOperatorStateBackend.class);

    /** The default namespace for state in cases where no state name is provided */
    public static final String DEFAULT_OPERATOR_STATE_NAME = "_default_";

    /** Map for all registered operator states. Maps state name -> state */
    private final Map<String, PartitionableListState<?>> registeredOperatorStates;

    /** Map for all registered operator broadcast states. Maps state name -> state */
    private final Map<String, BackendWritableBroadcastState<?, ?>> registeredBroadcastStates;

    /** CloseableRegistry to participate in the tasks lifecycle. */
    private final CloseableRegistry closeStreamOnCancelRegistry;

    /** Default typeSerializer. Only used for the default operator state. */
    private final JavaSerializer<Serializable> deprecatedDefaultJavaSerializer =
            new JavaSerializer<>();

    /** The execution configuration. */
    private final ExecutionConfig executionConfig;

    /**
     * Cache of already accessed states.
     *
     * <p>In contrast to {@link #registeredOperatorStates} which may be repopulated with restored
     * state, this map is always empty at the beginning.
     *
     * <p>TODO this map should be moved to a base class once we have proper hierarchy for the
     * operator state backends.
     *
     * @see <a href="https://issues.apache.org/jira/browse/FLINK-6849">FLINK-6849</a>
     */
    private final Map<String, PartitionableListState<?>> accessedStatesByName;

    private final Map<String, BackendWritableBroadcastState<?, ?>> accessedBroadcastStatesByName;

    private final SnapshotStrategyRunner<OperatorStateHandle, ?> snapshotStrategyRunner;

    public DefaultOperatorStateBackend(
            ExecutionConfig executionConfig,
            CloseableRegistry closeStreamOnCancelRegistry,
            Map<String, PartitionableListState<?>> registeredOperatorStates,
            Map<String, BackendWritableBroadcastState<?, ?>> registeredBroadcastStates,
            Map<String, PartitionableListState<?>> accessedStatesByName,
            Map<String, BackendWritableBroadcastState<?, ?>> accessedBroadcastStatesByName,
            SnapshotStrategyRunner<OperatorStateHandle, ?> snapshotStrategyRunner) {
        this.closeStreamOnCancelRegistry = closeStreamOnCancelRegistry;
        this.executionConfig = executionConfig;
        this.registeredOperatorStates = registeredOperatorStates;
        this.registeredBroadcastStates = registeredBroadcastStates;
        this.accessedStatesByName = accessedStatesByName;
        this.accessedBroadcastStatesByName = accessedBroadcastStatesByName;
        this.snapshotStrategyRunner = snapshotStrategyRunner;
    }

    public ExecutionConfig getExecutionConfig() {
        return executionConfig;
    }

    @Override
    public Set<String> getRegisteredStateNames() {
        return registeredOperatorStates.keySet();
    }

    @Override
    public Set<String> getRegisteredBroadcastStateNames() {
        return registeredBroadcastStates.keySet();
    }

    @Override
    public void close() throws IOException {
        closeStreamOnCancelRegistry.close();
    }

    @Override
    public void dispose() {
        IOUtils.closeQuietly(closeStreamOnCancelRegistry);
        registeredOperatorStates.clear();
        registeredBroadcastStates.clear();
    }

    // -------------------------------------------------------------------------------------------
    //  State access methods
    // -------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> BroadcastState<K, V> getBroadcastState(
            final MapStateDescriptor<K, V> stateDescriptor) throws StateMigrationException {

        Preconditions.checkNotNull(stateDescriptor);
        String name = Preconditions.checkNotNull(stateDescriptor.getName());

        BackendWritableBroadcastState<K, V> previous =
                (BackendWritableBroadcastState<K, V>) accessedBroadcastStatesByName.get(name);

        if (previous != null) {
            checkStateNameAndMode(
                    previous.getStateMetaInfo().getName(),
                    name,
                    previous.getStateMetaInfo().getAssignmentMode(),
                    OperatorStateHandle.Mode.BROADCAST);
            return previous;
        }

        stateDescriptor.initializeSerializerUnlessSet(getExecutionConfig());
        TypeSerializer<K> broadcastStateKeySerializer =
                Preconditions.checkNotNull(stateDescriptor.getKeySerializer());
        TypeSerializer<V> broadcastStateValueSerializer =
                Preconditions.checkNotNull(stateDescriptor.getValueSerializer());

        BackendWritableBroadcastState<K, V> broadcastState =
                (BackendWritableBroadcastState<K, V>) registeredBroadcastStates.get(name);

        if (broadcastState == null) {
            broadcastState =
                    new HeapBroadcastState<>(
                            new RegisteredBroadcastStateBackendMetaInfo<>(
                                    name,
                                    OperatorStateHandle.Mode.BROADCAST,
                                    broadcastStateKeySerializer,
                                    broadcastStateValueSerializer));
            registeredBroadcastStates.put(name, broadcastState);
        } else {
            // has restored state; check compatibility of new state access

            checkStateNameAndMode(
                    broadcastState.getStateMetaInfo().getName(),
                    name,
                    broadcastState.getStateMetaInfo().getAssignmentMode(),
                    OperatorStateHandle.Mode.BROADCAST);

            RegisteredBroadcastStateBackendMetaInfo<K, V> restoredBroadcastStateMetaInfo =
                    broadcastState.getStateMetaInfo();

            // check whether new serializers are incompatible
            TypeSerializerSchemaCompatibility<K> keyCompatibility =
                    restoredBroadcastStateMetaInfo.updateKeySerializer(broadcastStateKeySerializer);
            if (keyCompatibility.isIncompatible()) {
                throw new StateMigrationException(
                        "The new key typeSerializer for broadcast state must not be incompatible.");
            }

            TypeSerializerSchemaCompatibility<V> valueCompatibility =
                    restoredBroadcastStateMetaInfo.updateValueSerializer(
                            broadcastStateValueSerializer);
            if (valueCompatibility.isIncompatible()) {
                throw new StateMigrationException(
                        "The new value typeSerializer for broadcast state must not be incompatible.");
            }

            broadcastState.setStateMetaInfo(restoredBroadcastStateMetaInfo);
        }

        accessedBroadcastStatesByName.put(name, broadcastState);
        return broadcastState;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 获取一个ListState对象，用于在Flink流处理作业中存储和检索一系列的元素。
     * 这个ListState对象可以用于在状态后端中存储和操作一组有序的元素。
     *
     * @param <S>           元素的类型
     * @param stateDescriptor 描述状态如何被序列化和反序列化的状态描述符
     * @return 返回一个ListState对象，用于操作指定类型的元素列表
     * @throws Exception 当无法获取ListState对象时，抛出异常
    */
    @Override
    public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
        // 调用重载版本的getListState方法，并指定OperatorStateHandle的模式为SPLIT_DISTRIBUTE
        return getListState(stateDescriptor, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE);
    }

    @Override
    public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor)
            throws Exception {
        return getListState(stateDescriptor, OperatorStateHandle.Mode.UNION);
    }

    // -------------------------------------------------------------------------------------------
    //  Snapshot
    // -------------------------------------------------------------------------------------------
    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<OperatorStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {
        return snapshotStrategyRunner.snapshot(
                checkpointId, timestamp, streamFactory, checkpointOptions);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 根据给定的状态描述符和状态句柄模式获取一个ListState对象。
     * 该方法首先检查已访问的状态中是否存在具有相同名称的状态，如果存在则直接返回。
     * 如果不存在，则会根据状态描述符创建一个新的PartitionableListState对象。
     *
     * @param <S>           元素的类型
     * @param stateDescriptor 描述状态如何被序列化和反序列化的状态描述符
     * @param mode          OperatorStateHandle的模式，定义了状态在恢复时如何分割和分发
     * @return 返回一个ListState对象，用于操作指定类型的元素列表
     * @throws StateMigrationException 如果状态迁移过程中发生错误
    */
    private <S> ListState<S> getListState(
            ListStateDescriptor<S> stateDescriptor, OperatorStateHandle.Mode mode)
            throws StateMigrationException {
        // 检查状态描述符是否为空
        Preconditions.checkNotNull(stateDescriptor);
        // 获取状态描述符中的状态名称，并检查是否为空
        String name = Preconditions.checkNotNull(stateDescriptor.getName());

        // 尝试从已访问的状态集合中按名称获取之前的PartitionableListState
        @SuppressWarnings("unchecked")
        PartitionableListState<S> previous =
                (PartitionableListState<S>) accessedStatesByName.get(name);
        if (previous != null) {
            // 如果之前的状态存在，则检查状态名称和模式是否匹配
            checkStateNameAndMode(
                    previous.getStateMetaInfo().getName(),
                    name,
                    previous.getStateMetaInfo().getAssignmentMode(),
                    mode);
            // 如果匹配，则返回之前的状态
            return previous;
        }

        // end up here if its the first time access after execution for the
        // provided state name; check compatibility of restored state, if any
        // TODO with eager registration in place, these checks should be moved to restore()
        // 初始化状态描述符的序列化器（如果尚未设置）
        stateDescriptor.initializeSerializerUnlessSet(getExecutionConfig());
        // 获取状态描述符中的元素序列化器，并检查是否为空
        TypeSerializer<S> partitionStateSerializer =
                Preconditions.checkNotNull(stateDescriptor.getElementSerializer());

        // 尝试从已注册的状态集合中按名称获取PartitionableListState
        @SuppressWarnings("unchecked")
        PartitionableListState<S> partitionableListState =
                (PartitionableListState<S>) registeredOperatorStates.get(name);
        // 如果没有为状态名称恢复状态，则简单地创建一个新的状态持有者
        if (null == partitionableListState) {
            // no restored state for the state name; simply create new state holder
            // 创建一个新的PartitionableListState对象，并为其分配一个RegisteredOperatorStateBackendMetaInfo对象
            partitionableListState =
                    new PartitionableListState<>(
                            new RegisteredOperatorStateBackendMetaInfo<>(
                                    name, partitionStateSerializer, mode));
            // 将新创建的PartitionableListState对象添加到已注册的状态集合中
            registeredOperatorStates.put(name, partitionableListState);
        } else {
            // has restored state; check compatibility of new state access
            // 如果已存在已恢复的状态，则检查新状态访问的兼容性

            // 检查状态名称和模式是否匹配
            checkStateNameAndMode(
                    partitionableListState.getStateMetaInfo().getName(),
                    name,
                    partitionableListState.getStateMetaInfo().getAssignmentMode(),
                    mode);

            // 获取已恢复状态的元信息
            RegisteredOperatorStateBackendMetaInfo<S> restoredPartitionableListStateMetaInfo =
                    partitionableListState.getStateMetaInfo();

            // check compatibility to determine if new serializers are incompatible
            // 创建一个新的序列化器实例以进行比较，因为序列化器实例可能包含状态
            TypeSerializer<S> newPartitionStateSerializer = partitionStateSerializer.duplicate();

            // 检查新序列化器与已恢复状态的序列化器是否兼容
            TypeSerializerSchemaCompatibility<S> stateCompatibility =
                    restoredPartitionableListStateMetaInfo.updatePartitionStateSerializer(
                            newPartitionStateSerializer);
            // 如果不兼容，则抛出异常
            if (stateCompatibility.isIncompatible()) {
                throw new StateMigrationException(
                        "The new state typeSerializer for operator state must not be incompatible.");
            }
            // 更新PartitionableListState的状态元信息（如果进行了序列化器的更新）
            partitionableListState.setStateMetaInfo(restoredPartitionableListStateMetaInfo);
        }
        // 将访问的状态添加到已访问的状态集合中
        accessedStatesByName.put(name, partitionableListState);
        // 返回PartitionableListState对象
        return partitionableListState;
    }

    private static void checkStateNameAndMode(
            String actualName,
            String expectedName,
            OperatorStateHandle.Mode actualMode,
            OperatorStateHandle.Mode expectedMode) {

        Preconditions.checkState(
                actualName.equals(expectedName),
                "Incompatible state names. "
                        + "Was ["
                        + actualName
                        + "], "
                        + "registered with ["
                        + expectedName
                        + "].");

        Preconditions.checkState(
                actualMode.equals(expectedMode),
                "Incompatible state assignment modes. "
                        + "Was ["
                        + actualMode
                        + "], "
                        + "registered with ["
                        + expectedMode
                        + "].");
    }
}
