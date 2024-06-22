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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.runtime.state.CheckpointStreamWithResultProvider.createDuplicatingStream;
import static org.apache.flink.runtime.state.CheckpointStreamWithResultProvider.createSimpleStream;
import static org.apache.flink.runtime.state.CheckpointStreamWithResultProvider.toKeyedStateHandleSnapshotResult;

/** A strategy how to perform a snapshot of a {@link HeapKeyedStateBackend}. */
class HeapSnapshotStrategy<K>
        implements SnapshotStrategy<KeyedStateHandle, HeapSnapshotResources<K>> {

    private final Map<String, StateTable<K, ?, ?>> registeredKVStates;
    private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates;
    private final StreamCompressionDecorator keyGroupCompressionDecorator;
    private final LocalRecoveryConfig localRecoveryConfig;
    private final KeyGroupRange keyGroupRange;
    private final StateSerializerProvider<K> keySerializerProvider;
    private final int totalKeyGroups;

    HeapSnapshotStrategy(
            Map<String, StateTable<K, ?, ?>> registeredKVStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            LocalRecoveryConfig localRecoveryConfig,
            KeyGroupRange keyGroupRange,
            StateSerializerProvider<K> keySerializerProvider,
            int totalKeyGroups) {
        this.registeredKVStates = registeredKVStates;
        this.registeredPQStates = registeredPQStates;
        this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
        this.localRecoveryConfig = localRecoveryConfig;
        this.keyGroupRange = keyGroupRange;
        this.keySerializerProvider = keySerializerProvider;
        this.totalKeyGroups = totalKeyGroups;
    }

    @Override
    public HeapSnapshotResources<K> syncPrepareResources(long checkpointId) {
        return HeapSnapshotResources.create(
                registeredKVStates,
                registeredPQStates,
                keyGroupCompressionDecorator,
                keyGroupRange,
                getKeySerializer(),
                totalKeyGroups);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 一个操作，将快照写入由给定 {@link CheckpointStreamFactory} 提供的流中，
     * 并返回一个 {@link SupplierWithException}（带有异常的提供者），该提供者提供对快照的状态句柄。
     *
     * @param syncPartResource 同步部分资源，可能用于协调快照过程或包含与快照相关的状态信息。
     * @param checkpointId 快照的ID。
     * @param timestamp 快照的时间戳。
     * @param streamFactory 我们可以使用的工厂，用于将状态写入流。
     * @param checkpointOptions 执行此检查点的选项。
     * @return 一个提供者，将生成一个 {@link StateObject}（状态对象）。
     * @throws Exception 如果在快照过程中发生异常，则抛出异常。
    */
    @Override
    public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(
            HeapSnapshotResources<K> syncPartResource,
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions) {

        // 获取状态元数据的快照列表
        List<StateMetaInfoSnapshot> metaInfoSnapshots = syncPartResource.getMetaInfoSnapshots();
        // 如果元数据快照列表为空，则返回一个直接返回空快照
        if (metaInfoSnapshots.isEmpty()) {
            return snapshotCloseableRegistry -> SnapshotResult.empty();
        }
        // 创建一个序列化代理，该代理将用于序列化键（Key）和其他元数据
        final KeyedBackendSerializationProxy<K> serializationProxy =
                // 创建一个KeyedBackendSerializationProxy对象，它使用同步资源中的键序列化器和其他参数
                new KeyedBackendSerializationProxy<>(
                        // TODO: this code assumes that writing a serializer is threadsafe, we
                        // should support to
                        // get a serialized form already at state registration time in the future
                        syncPartResource.getKeySerializer(),// 获取键的序列化器
                        metaInfoSnapshots,// 元数据快照列表
                        !Objects.equals(// 判断是否不使用UncompressedStreamCompressionDecorator进行键组压缩
                                UncompressedStreamCompressionDecorator.INSTANCE,
                                keyGroupCompressionDecorator));
          // 定义一个带有异常处理的流创建供（Supplier），用于在创建检查点（Checkpoint）时提供流和结果提供者
        final SupplierWithException<CheckpointStreamWithResultProvider, Exception>
                checkpointStreamSupplier =
                // 如果本地恢复（Local Recovery）功能被启用，并且当前检查点类型不是保存点（Savepoint）
                localRecoveryConfig.isLocalRecoveryEnabled()
                                        && !checkpointOptions.getCheckpointType().isSavepoint()
                                ? () ->
                                        // 则使用下面的lambda表达式创建一个支持本地恢复功能的流
                                        createDuplicatingStream(
                                                checkpointId,
                                                CheckpointedStateScope.EXCLUSIVE,
                                                streamFactory,
                                                localRecoveryConfig
                                                        .getLocalStateDirectoryProvider()
                                                        .orElseThrow(
                                                                LocalRecoveryConfig
                                                                        .localRecoveryNotEnabled()))
                                : () ->
                        // 则创建一个简单的流，不支持本地恢复功能
                                        createSimpleStream(
                                                CheckpointedStateScope.EXCLUSIVE, streamFactory);

        return (snapshotCloseableRegistry) -> {
            // 获取从同步分区资源中保存的状态名称到ID的映射
            final Map<StateUID, Integer> stateNamesToId = syncPartResource.getStateNamesToId();
            // 获取从同步分区资源中获取的写时复制（COW）稳定状态的快照
            final Map<StateUID, StateSnapshot> cowStateStableSnapshots =
                    syncPartResource.getCowStateStableSnapshots();
            // 获取检查点流提供器
            final CheckpointStreamWithResultProvider streamWithResultProvider =
                    checkpointStreamSupplier.get();
            // 将检查点流提供器注册为可关闭资源，以便在检查点完成后进行清理
            snapshotCloseableRegistry.registerCloseable(streamWithResultProvider);
            // 从检查点流提供器中获取实际的检查点状态输出流
            final CheckpointStateOutputStream localStream =
                    streamWithResultProvider.getCheckpointOutputStream();
            // 创建一个封装了检查点状态输出流的数据输出视图包装器
            // 这允许我们以更方便的方式写入序列化数据
            final DataOutputViewStreamWrapper outView =
                    new DataOutputViewStreamWrapper(localStream);
            // 使用序列化代理将序列化对象写入到输出视图中
            // 这里的serializationProxy应该是一个实现了序列化逻辑的对象
            serializationProxy.write(outView);

            // 创建一个数组来保存每个键组范围的偏移量
            final long[] keyGroupRangeOffsets = new long[keyGroupRange.getNumberOfKeyGroups()];

            // 遍历所有的键组范围
            for (int keyGroupPos = 0;
                    keyGroupPos < keyGroupRange.getNumberOfKeyGroups();
                    ++keyGroupPos) {
                // 获取当前键组的ID
                int keyGroupId = keyGroupRange.getKeyGroupId(keyGroupPos);
                // 记录当前键组在输出流中的偏移量
                keyGroupRangeOffsets[keyGroupPos] = localStream.getPos();
                // 将键组ID写入到输出流中
                outView.writeInt(keyGroupId);
                // 遍历所有稳定的写时复制状态快照
                for (Map.Entry<StateUID, StateSnapshot> stateSnapshot :
                        cowStateStableSnapshots.entrySet()) {
                    // 获取当前状态的键组写入器
                    StateSnapshot.StateKeyGroupWriter partitionedSnapshot =
                            stateSnapshot.getValue().getKeyGroupWriter();
                    // 使用键组压缩装饰器对检查点状态输出流进行压缩包装
                    // 该装饰器可能负责压缩数据以减少输出流的大小
                    try (OutputStream kgCompressionOut =
                            keyGroupCompressionDecorator.decorateWithCompression(localStream)) {
                        // 创建一个封装了压缩输出流的数据输出视图包装器
                        DataOutputViewStreamWrapper kgCompressionView =
                                new DataOutputViewStreamWrapper(kgCompressionOut);
                        // 写入状态名称对应的ID到压缩输出流中
                        kgCompressionView.writeShort(stateNamesToId.get(stateSnapshot.getKey()));
                        // 使用键组写入器将状态数据写入到压缩的数据输出视图中
                        partitionedSnapshot.writeStateInKeyGroup(kgCompressionView, keyGroupId);
                    } // this will just close the outer compression stream
                }
            }
             // 尝试从可关闭资源注册表中注销检查点流提供器
            if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
                // 创建一个包含键组范围及其对应偏移量的对象
                // 用于在恢复时定位每个键组的数据
                KeyGroupRangeOffsets kgOffs =
                        new KeyGroupRangeOffsets(keyGroupRange, keyGroupRangeOffsets);
                // 关闭并最终化检查点流提供器，以获取检查点的结果
                SnapshotResult<StreamStateHandle> result =
                        streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
                // 将结果转换为特定的KeyedStateHandleSnapshotResult类型
                return toKeyedStateHandleSnapshotResult(result, kgOffs, KeyGroupsStateHandle::new);
            } else {
                // 抛出异常表示检查点数据可能未能正确写入或处理
                throw new IOException("Stream already unregistered.");
            }
        };
    }

    public TypeSerializer<K> getKeySerializer() {
        return keySerializerProvider.currentSchemaSerializer();
    }
}
