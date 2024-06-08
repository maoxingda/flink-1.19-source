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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * The result partition manager keeps track of all currently produced/consumed partitions of a task
 * manager.
 */
public class ResultPartitionManager implements ResultPartitionProvider {

    private static final Logger LOG = LoggerFactory.getLogger(ResultPartitionManager.class);

    private final Map<ResultPartitionID, ResultPartition> registeredPartitions =
            CollectionUtil.newHashMapWithExpectedSize(16);

    @GuardedBy("registeredPartitions")
    private final Map<ResultPartitionID, PartitionRequestListenerManager> listenerManagers =
            new HashMap<>();

    @Nullable private ScheduledFuture<?> partitionListenerTimeoutChecker;

    private final int partitionListenerTimeout;

    private boolean isShutdown;

    @VisibleForTesting
    public ResultPartitionManager() {
        this(0, null);
    }

    public ResultPartitionManager(
            int partitionListenerTimeout, ScheduledExecutor scheduledExecutor) {
        this.partitionListenerTimeout = partitionListenerTimeout;
        if (partitionListenerTimeout > 0 && scheduledExecutor != null) {
            this.partitionListenerTimeoutChecker =
                    scheduledExecutor.scheduleWithFixedDelay(
                            this::checkRequestPartitionListeners,
                            partitionListenerTimeout,
                            partitionListenerTimeout,
                            TimeUnit.MILLISECONDS);
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * Map<ResultPartitionID, ResultPartition> registeredPartitions
     * 注册ResultPartition
    */
    public void registerResultPartition(ResultPartition partition) throws IOException {
        // 获取PartitionRequestListenerManager对象，这里一个用于管理监听器的类
        PartitionRequestListenerManager listenerManager;
        // 使用synchronized关键字对registeredPartitions对象进行同步，确保在多线程环境下对注册的分区进行安全操作
        synchronized (registeredPartitions) {
            // 检查ResultPartitionManager是否已经被关闭
            // 如果已经关闭，则抛出异常
            checkState(!isShutdown, "Result partition manager already shut down.");

            // 尝试在registeredPartitions映射中放入新的ResultPartition，key为partitionId
            // 如果已经存在相同的partitionId，则返回旧的ResultPartition
            ResultPartition previous =
                    registeredPartitions.put(partition.getPartitionId(), partition);

            // 如果返回的previous不为null，则说明存在重复的partitionId，抛出异常
            if (previous != null) {
                throw new IllegalStateException("Result partition already registered.");
            }
            // 尝试从listenerManagers映射中移除与partitionId对应的listenerManager
            // 如果该partitionId之前注册过监听器，这里将会移除它
            listenerManager = listenerManagers.remove(partition.getPartitionId());
        }
        // 如果成功从listenerManagers中移除了对应的listenerManager
        if (listenerManager != null) {
            // 遍历listenerManager中注册的监听器
            for (PartitionRequestListener listener :
                    listenerManager.getPartitionRequestListeners()) {
                // 通知每个监听器，该partition已经被创建
                listener.notifyPartitionCreated(partition);
            }
        }
        // 记录日志，表示已经成功注册了partition
        LOG.debug("Registered {}.", partition);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 根据给定的分区ID和子分区索引集合创建一个ResultSubpartitionView对象。
     *
     * @param partitionId 结果分区的ID
     * @param subpartitionIndexSet 子分区索引集合
     * @param availabilityListener 缓冲区可用性监听器
     * @return ResultSubpartitionView对象，代表结果分区中的一个或多个子分区
     * @throws IOException 如果在创建子分区视图时发生I/O错误
    */
    @Override
    public ResultSubpartitionView createSubpartitionView(
            ResultPartitionID partitionId,
            ResultSubpartitionIndexSet subpartitionIndexSet,
            BufferAvailabilityListener availabilityListener)
            throws IOException {
        // 声明一个ResultSubpartitionView对象，用于存储创建的子分区视图
        final ResultSubpartitionView subpartitionView;
        synchronized (registeredPartitions) {
            // 根据分区ID从registeredPartitions映射中获取结果分区对象
            final ResultPartition partition = registeredPartitions.get(partitionId);
            // 如果找不到对应的分区，则抛出PartitionNotFoundException异常
            if (partition == null) {
                throw new PartitionNotFoundException(partitionId);
            }
            // 记录日志，显示正在请求哪个分区的哪些子分区
            LOG.debug("Requesting subpartitions {} of {}.", subpartitionIndexSet, partition);
             // 调用结果分区的createSubpartitionView方法来创建子分区视图
            // 并传入子分区索引集合和缓冲区可用性监听器
            subpartitionView =
                    partition.createSubpartitionView(subpartitionIndexSet, availabilityListener);
        }
        // 返回创建的子分区视图对象
        return subpartitionView;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建一个子分区视图或注册监听器。
     *
     * <p>根据给定的分区ID和子分区索引集，尝试创建一个子分区视图。
     * 如果相应的分区尚未注册，则注册分区请求监听器，但不创建子分区视图。
     *
     * @param partitionId 分区ID
     * @param subpartitionIndexSet 子分区索引集
     * @param availabilityListener 缓冲区可用性监听器
     * @param partitionRequestListener 分区请求监听器
     * @return 包含子分区视图的Optional对象，如果分区未注册则返回空的Optional
     * @throws IOException 如果在创建子分区视图或注册监听器时发生I/O错误
    */
    @Override
    public Optional<ResultSubpartitionView> createSubpartitionViewOrRegisterListener(
            ResultPartitionID partitionId,
            ResultSubpartitionIndexSet subpartitionIndexSet,
            BufferAvailabilityListener availabilityListener,
            PartitionRequestListener partitionRequestListener)
            throws IOException {
        // 声明一个ResultSubpartitionView类型的局部变量来存储可能的子分区视图
        final ResultSubpartitionView subpartitionView;
        // 使用synchronized块来确保对registeredPartitions的线程安全访问
        synchronized (registeredPartitions) {
            // 从registeredPartitions中获取对应partitionId的分区
            final ResultPartition partition = registeredPartitions.get(partitionId);
            // 如果分区未注册
            if (partition == null) {
                // 在listenerManagers中根据partitionId获取或创建一个PartitionRequestListenerManager
                // 并注册给定的分区请求监听器
                listenerManagers
                        .computeIfAbsent(partitionId, key -> new PartitionRequestListenerManager())
                        .registerListener(partitionRequestListener);
                // 因为分区未注册，所以子分区视图无法创建，设为null
                subpartitionView = null;
            } else {
                // 如果分区已注册，则记录日志，表明正在请求该分区的子分区
                LOG.debug("Requesting subpartitions {} of {}.", subpartitionIndexSet, partition);
                // 调用分区的createSubpartitionView方法来创建子分区视图
                // 并传入子分区索引集和缓冲区可用性监听器
                subpartitionView =
                        partition.createSubpartitionView(
                                subpartitionIndexSet, availabilityListener);
            }
        }
        // 如果子分区视图为null，则返回空的Optional；否则，返回包含子分区视图的Optional
        return subpartitionView == null ? Optional.empty() : Optional.of(subpartitionView);
    }

    @Override
    public void releasePartitionRequestListener(PartitionRequestListener listener) {
        synchronized (registeredPartitions) {
            PartitionRequestListenerManager listenerManager =
                    listenerManagers.get(listener.getResultPartitionId());
            if (listenerManager != null) {
                listenerManager.remove(listener.getReceiverId());
                if (listenerManager.isEmpty()) {
                    listenerManagers.remove(listener.getResultPartitionId());
                }
            }
        }
    }

    public void releasePartition(ResultPartitionID partitionId, Throwable cause) {
        PartitionRequestListenerManager listenerManager;
        synchronized (registeredPartitions) {
            ResultPartition resultPartition = registeredPartitions.remove(partitionId);
            if (resultPartition != null) {
                resultPartition.release(cause);
                LOG.debug(
                        "Released partition {} produced by {}.",
                        partitionId.getPartitionId(),
                        partitionId.getProducerId());
            }
            listenerManager = listenerManagers.remove(partitionId);
        }
        if (listenerManager != null && !listenerManager.isEmpty()) {
            for (PartitionRequestListener listener :
                    listenerManager.getPartitionRequestListeners()) {
                listener.notifyPartitionCreatedTimeout();
            }
        }
    }

    public void shutdown() {
        synchronized (registeredPartitions) {
            LOG.debug(
                    "Releasing {} partitions because of shutdown.",
                    registeredPartitions.values().size());

            for (ResultPartition partition : registeredPartitions.values()) {
                partition.release();
            }

            registeredPartitions.clear();

            releaseListenerManagers();

            // stop the timeout checks for the TaskManagers
            if (partitionListenerTimeoutChecker != null) {
                partitionListenerTimeoutChecker.cancel(false);
                partitionListenerTimeoutChecker = null;
            }

            isShutdown = true;

            LOG.debug("Successful shutdown.");
        }
    }

    private void releaseListenerManagers() {
        for (PartitionRequestListenerManager listenerManager : listenerManagers.values()) {
            for (PartitionRequestListener listener :
                    listenerManager.getPartitionRequestListeners()) {
                listener.notifyPartitionCreatedTimeout();
            }
        }
        listenerManagers.clear();
    }

    /** Check whether the partition request listener is timeout. */
    private void checkRequestPartitionListeners() {
        List<PartitionRequestListener> timeoutPartitionRequestListeners = new LinkedList<>();
        synchronized (registeredPartitions) {
            if (isShutdown) {
                return;
            }
            long now = System.currentTimeMillis();
            Iterator<Map.Entry<ResultPartitionID, PartitionRequestListenerManager>> iterator =
                    listenerManagers.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<ResultPartitionID, PartitionRequestListenerManager> entry =
                        iterator.next();
                PartitionRequestListenerManager partitionRequestListeners = entry.getValue();
                partitionRequestListeners.removeExpiration(
                        now, partitionListenerTimeout, timeoutPartitionRequestListeners);
                if (partitionRequestListeners.isEmpty()) {
                    iterator.remove();
                }
            }
        }
        for (PartitionRequestListener partitionRequestListener : timeoutPartitionRequestListeners) {
            partitionRequestListener.notifyPartitionCreatedTimeout();
        }
    }

    @VisibleForTesting
    public Map<ResultPartitionID, PartitionRequestListenerManager> getListenerManagers() {
        return listenerManagers;
    }

    // ------------------------------------------------------------------------
    // Notifications
    // ------------------------------------------------------------------------

    void onConsumedPartition(ResultPartition partition) {
        LOG.debug("Received consume notification from {}.", partition);

        synchronized (registeredPartitions) {
            final ResultPartition previous =
                    registeredPartitions.remove(partition.getPartitionId());
            // Release the partition if it was successfully removed
            if (partition == previous) {
                partition.release();
                ResultPartitionID partitionId = partition.getPartitionId();
                LOG.debug(
                        "Released partition {} produced by {}.",
                        partitionId.getPartitionId(),
                        partitionId.getProducerId());
            }
            PartitionRequestListenerManager listenerManager =
                    listenerManagers.remove(partition.getPartitionId());
            checkState(
                    listenerManager == null || listenerManager.isEmpty(),
                    "The partition request listeners is not empty for "
                            + partition.getPartitionId());
        }
    }

    public Collection<ResultPartitionID> getUnreleasedPartitions() {
        synchronized (registeredPartitions) {
            return registeredPartitions.keySet();
        }
    }
}
