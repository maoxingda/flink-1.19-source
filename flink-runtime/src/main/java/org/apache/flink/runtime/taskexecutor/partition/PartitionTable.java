/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor.partition;

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Thread-safe Utility for tracking partitions. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 用于跟踪分区的线程安全实用程序
*/
@ThreadSafe
public class PartitionTable<K> {

    private final Map<K, Set<ResultPartitionID>> trackedPartitionsPerKey =
            new ConcurrentHashMap<>(8);

    /** Returns whether any partitions are being tracked for the given key. */
    public boolean hasTrackedPartitions(K key) {
        return trackedPartitionsPerKey.containsKey(key);
    }

    /** Starts the tracking of the given partition for the given key. */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 开始跟踪给定键的给定分区
    */
    public void startTrackingPartitions(K key, Collection<ResultPartitionID> newPartitionIds) {
        // 检查传入的key是否为空，如果为空则抛出NullPointerException异常
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(newPartitionIds);
        // 如果newPartitionIds集合为空，则直接返回，不进行后续操作
        if (newPartitionIds.isEmpty()) {
            return;
        }
        // 使用computeIfAbsent方法（这里用compute是为了处理partitionIds可能为null的情况）
        // 以key为键，将新的partitionIds集合与已跟踪的partitionIds集合合并
        // trackedPartitionsPerKey应该是一个Map，用于存储key与对应的partitionIds集合的映射关系
        trackedPartitionsPerKey.compute(
                key,
                (ignored, partitionIds) -> {
                    // 如果对应的partitionIds集合为空（即第一次添加），则创建一个新的HashSet，并设置预期的初始容量为8
                    if (partitionIds == null) {
                        partitionIds = CollectionUtil.newHashSetWithExpectedSize(8);
                    }
                    // 将新的partitionIds集合添加到已存在的partitionIds集合中
                    partitionIds.addAll(newPartitionIds);
                    // 返回更新后的partitionIds集合
                    return partitionIds;
                });
    }

    /** Stops the tracking of all partition for the given key. */
    public Collection<ResultPartitionID> stopTrackingPartitions(K key) {
        Preconditions.checkNotNull(key);

        Set<ResultPartitionID> storedPartitions = trackedPartitionsPerKey.remove(key);
        return storedPartitions == null ? Collections.emptyList() : storedPartitions;
    }

    /** Stops the tracking of the given set of partitions for the given key. */
    public void stopTrackingPartitions(K key, Collection<ResultPartitionID> partitionIds) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(partitionIds);

        // If the key is unknown we do not fail here, in line with
        // ShuffleEnvironment#releaseFinishedPartitions
        trackedPartitionsPerKey.computeIfPresent(
                key,
                (ignored, resultPartitionIDS) -> {
                    resultPartitionIDS.removeAll(partitionIds);
                    return resultPartitionIDS.isEmpty() ? null : resultPartitionIDS;
                });
    }
}
