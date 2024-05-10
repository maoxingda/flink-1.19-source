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

package org.apache.flink.runtime.deployment;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.PermanentBlobService;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.MaybeOffloaded;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.NonOffloaded;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.Offloaded;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorGroup;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.util.GroupCache;
import org.apache.flink.util.CompressedSerializedValue;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Deployment descriptor for a single input gate instance.
 *
 * <p>Each input gate consumes partitions of a single intermediate result. The consumed subpartition
 * index range is the same for each consumed partition.
 *
 * @see SingleInputGate
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 单个输入门实例的部署描述符。
*/
public class InputGateDeploymentDescriptor implements Serializable {

    private static final long serialVersionUID = -7143441863165366704L;
    /**
     * The ID of the consumed intermediate result. Each input gate consumes partitions of the
     * intermediate result specified by this ID. This ID also identifies the input gate at the
     * consuming task.
     */
    //已消耗的中间结果的ID。每个输入消耗由该ID指定的中间结果的分区。该ID还标识消耗任务中的输入。
    private final IntermediateDataSetID consumedResultId;

    /** The type of the partition the input gate is going to consume. */
    //输入要使用的分区的类型。
    private final ResultPartitionType consumedPartitionType;

    /**
     * Range of the index of the consumed subpartition of each consumed partition. This index
     * depends on the {@link DistributionPattern} and the subtask indices of the producing and
     * consuming task. The range is inclusive.
     */
    //每个已消耗分区的已消耗子分区的索引范围。此索引取决于 DistributionPattern 以及生产任务和消耗任务的子任务索引。范围包括在内。
    private final IndexRange consumedSubpartitionIndexRange;

    /** An input channel for each consumed subpartition. */
    //每个消耗的子分区的输入通道。
    private transient ShuffleDescriptor[] inputChannels;

    /** Serialized value of shuffle descriptors. */
    //Shuffle描述符的序列化值。
    private final List<MaybeOffloaded<ShuffleDescriptorGroup>> serializedInputChannels;

    /** Number of input channels. */
    //输入通道数
    private final int numberOfInputChannels;

    @VisibleForTesting
    public InputGateDeploymentDescriptor(
            IntermediateDataSetID consumedResultId,
            ResultPartitionType consumedPartitionType,
            @Nonnegative int consumedSubpartitionIndex,
            ShuffleDescriptorAndIndex[] inputChannels)
            throws IOException {
        this(
                consumedResultId,
                consumedPartitionType,
                new IndexRange(consumedSubpartitionIndex, consumedSubpartitionIndex),
                inputChannels.length,
                Collections.singletonList(
                        new NonOffloaded<>(
                                CompressedSerializedValue.fromObject(
                                        new ShuffleDescriptorGroup(inputChannels)))));
    }

    public InputGateDeploymentDescriptor(
            IntermediateDataSetID consumedResultId,
            ResultPartitionType consumedPartitionType,
            IndexRange consumedSubpartitionIndexRange,
            int numberOfInputChannels,
            List<MaybeOffloaded<ShuffleDescriptorGroup>> serializedInputChannels) {
        this.consumedResultId = checkNotNull(consumedResultId);
        this.consumedPartitionType = checkNotNull(consumedPartitionType);
        this.consumedSubpartitionIndexRange = checkNotNull(consumedSubpartitionIndexRange);
        this.serializedInputChannels = checkNotNull(serializedInputChannels);
        this.numberOfInputChannels = numberOfInputChannels;
    }

    public IntermediateDataSetID getConsumedResultId() {
        return consumedResultId;
    }

    /**
     * Returns the type of this input channel's consumed result partition.
     *
     * @return consumed result partition type
     */
    public ResultPartitionType getConsumedPartitionType() {
        return consumedPartitionType;
    }

    @Nonnegative
    public int getConsumedSubpartitionIndex() {
        checkState(
                consumedSubpartitionIndexRange.getStartIndex()
                        == consumedSubpartitionIndexRange.getEndIndex());
        return consumedSubpartitionIndexRange.getStartIndex();
    }

    /** Return the index range of the consumed subpartitions. */
    public IndexRange getConsumedSubpartitionIndexRange() {
        return consumedSubpartitionIndexRange;
    }

    public ShuffleDescriptor[] getShuffleDescriptors() {
        if (inputChannels == null) {
            // This is only for testing scenarios, in a production environment we always call
            // tryLoadAndDeserializeShuffleDescriptors to deserialize ShuffleDescriptors first.
            inputChannels = new ShuffleDescriptor[numberOfInputChannels];
            try {
                for (MaybeOffloaded<ShuffleDescriptorGroup> serializedShuffleDescriptors :
                        serializedInputChannels) {
                    checkState(
                            serializedShuffleDescriptors instanceof NonOffloaded,
                            "Trying to work with offloaded serialized shuffle descriptors.");
                    NonOffloaded<ShuffleDescriptorGroup> nonOffloadedSerializedValue =
                            (NonOffloaded<ShuffleDescriptorGroup>) serializedShuffleDescriptors;
                    tryDeserializeShuffleDescriptorGroup(nonOffloadedSerializedValue);
                }
            } catch (ClassNotFoundException | IOException e) {
                throw new RuntimeException("Could not deserialize shuffle descriptors.", e);
            }
        }
        return inputChannels;
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 加载并序列化ShuffleDescriptors
    */
    public void tryLoadAndDeserializeShuffleDescriptors(
            @Nullable PermanentBlobService blobService,
            JobID jobId,
            GroupCache<JobID, PermanentBlobKey, ShuffleDescriptorGroup> shuffleDescriptorsCache)
            throws IOException {
        /** 如果inputChannels已经不为空，则直接返回，不再进行加载和反序列化操作 */
        if (inputChannels != null) {
            return;
        }

        try {
            /** 初始化inputChannels数组，大小为numberOfInputChannels  */
            inputChannels = new ShuffleDescriptor[numberOfInputChannels];
            /** 遍历serializedInputChannels，它可能包含已序列化的ShuffleDescriptorGroup  */
            for (MaybeOffloaded<ShuffleDescriptorGroup> serializedShuffleDescriptors :
                    serializedInputChannels) {
                /**
                 *  尝试加载并反序列化ShuffleDescriptorGroup
                 *  注意：此函数内部可能会处理序列化数据的加载和反序列化，并更新shuffleDescriptorsCache
                 */
                tryLoadAndDeserializeShuffleDescriptorGroup(
                        blobService, jobId, serializedShuffleDescriptors, shuffleDescriptorsCache);
            }
        } catch (ClassNotFoundException e) {
            /** 抛出运行时异常，并附带原始异常信 */
            throw new RuntimeException("Could not deserialize shuffle descriptors.", e);
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *
    */
    private void tryLoadAndDeserializeShuffleDescriptorGroup(
            @Nullable PermanentBlobService blobService,
            JobID jobId,
            MaybeOffloaded<ShuffleDescriptorGroup> serializedShuffleDescriptors,
            GroupCache<JobID, PermanentBlobKey, ShuffleDescriptorGroup> shuffleDescriptorsCache)
            throws IOException, ClassNotFoundException {
        /** 检查serializedShuffleDescriptors是否为Offloaded类型   */
        if (serializedShuffleDescriptors instanceof Offloaded) {
            /** 获取序列化后的ShuffleDescriptorGroup的Blob键 */
            PermanentBlobKey blobKey =
                    ((Offloaded<ShuffleDescriptorGroup>) serializedShuffleDescriptors)
                            .serializedValueKey;
            /** 尝试从缓存中获取ShuffleDescriptorGroup  */
            ShuffleDescriptorGroup shuffleDescriptorGroup =
                    shuffleDescriptorsCache.get(jobId, blobKey);
            /** 如果缓存中不存在   */
            if (shuffleDescriptorGroup == null) {
                /** 确保blobService不为空，因为需要从Blob服务中读取数据 */
                Preconditions.checkNotNull(blobService);
                // NOTE: Do not delete the ShuffleDescriptor BLOBs since it may be needed again
                // during
                // recovery. (it is deleted automatically on the BLOB server and cache when its
                // partition is no longer available or the job enters a terminal state)
                /** 从Blob服务中读取压缩的序列化值   */
                CompressedSerializedValue<ShuffleDescriptorGroup> serializedValue =
                        CompressedSerializedValue.fromBytes(blobService.readFile(jobId, blobKey));
                /** 反序列化值并获取ShuffleDescriptorGroup   */
                shuffleDescriptorGroup =
                        serializedValue.deserializeValue(getClass().getClassLoader());
                // update cache
                /** 更新缓存 */
                shuffleDescriptorsCache.put(jobId, blobKey, shuffleDescriptorGroup);
            }
            /** 将反序列化后的ShuffleDescriptorGroup放入或替换到相应的位置   */
            putOrReplaceShuffleDescriptors(shuffleDescriptorGroup);
        } else {
            /** 如果serializedShuffleDescriptors不是Offloaded类型，则假设它是NonOffloaded类型   */
            NonOffloaded<ShuffleDescriptorGroup> nonOffloadedSerializedValue =
                    (NonOffloaded<ShuffleDescriptorGroup>) serializedShuffleDescriptors;
            tryDeserializeShuffleDescriptorGroup(nonOffloadedSerializedValue);
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 尝试反序列化并处理一个ShuffleDescriptorGroup的实例。
    */
    private void tryDeserializeShuffleDescriptorGroup(
            NonOffloaded<ShuffleDescriptorGroup> nonOffloadedShuffleDescriptorGroup)
            throws IOException, ClassNotFoundException {
        /** 获取ShuffleDescriptorGroup的序列化值，并使用当前类的类加载器进行反序列化 */
        ShuffleDescriptorGroup shuffleDescriptorGroup =
                nonOffloadedShuffleDescriptorGroup.serializedValue.deserializeValue(
                        getClass().getClassLoader());
        /** 将反序列化后的ShuffleDescriptorGroup对象放入或替换到相应的数据结构中 */
        putOrReplaceShuffleDescriptors(shuffleDescriptorGroup);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将给定的ShuffleDescriptorGroup中的ShuffleDescriptor放入或替换到inputChannels数组中。
    */
    private void putOrReplaceShuffleDescriptors(ShuffleDescriptorGroup shuffleDescriptorGroup) {
        /** 遍历ShuffleDescriptorGroup中的所有ShuffleDescriptor和对应的索引 */
        for (ShuffleDescriptorAndIndex shuffleDescriptorAndIndex :
                shuffleDescriptorGroup.getShuffleDescriptors()) {
            /** 根据索引从inputChannels数组中获取对应的ShuffleDescriptor   */
            ShuffleDescriptor inputChannelDescriptor =
                    inputChannels[shuffleDescriptorAndIndex.getIndex()];
            /** 如果该索引对应的ShuffleDescriptor不为空   */
            if (inputChannelDescriptor != null) {
                /** 如果不是未知的，则抛出IllegalStateException异常   */
                checkState(
                        inputChannelDescriptor.isUnknown(),
                        "Only unknown shuffle descriptor can be replaced.");
            }
            /** 将新的ShuffleDescriptor放入inputChannels数组的对应索引位置   */
            inputChannels[shuffleDescriptorAndIndex.getIndex()] =
                    shuffleDescriptorAndIndex.getShuffleDescriptor();
        }
    }

    @Override
    public String toString() {
        return String.format(
                "InputGateDeploymentDescriptor [result id: %s, "
                        + "consumed subpartition index range: %s]",
                consumedResultId.toString(), consumedSubpartitionIndexRange);
    }
}
