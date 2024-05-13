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

package org.apache.flink.runtime.io.network;

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.api.TaskEventHandler;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.util.event.EventListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The task event dispatcher dispatches events flowing backwards from a consuming task to the task
 * producing the consumed result.
 *
 * <p>Backwards events only work for tasks, which produce pipelined results, where both the
 * producing and consuming task are running at the same time.
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 任务事件调度器将事件从消耗任务向后调度到产生消耗结果的任务。
*/
public class TaskEventDispatcher implements TaskEventPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(TaskEventDispatcher.class);

    private final Map<ResultPartitionID, TaskEventHandler> registeredHandlers = new HashMap<>();

    /**
     * Registers the given partition for incoming task events allowing calls to {@link
     * #subscribeToEvent(ResultPartitionID, EventListener, Class)}.
     *
     * @param partitionId the partition ID
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 注册给定的分区以接收传入的任务事件，允许调用 {@link #subscribeToEvent(ResultPartitionID, EventListener, Class)} 方法。
    */
    public void registerPartition(ResultPartitionID partitionId) {
        // 检查传入的partitionId是否为空，如果为空则抛出NullPointerException
        checkNotNull(partitionId);
        // 使用synchronized关键字确保在多线程环境下，registeredHandlers的注册操作是线程安全的
        synchronized (registeredHandlers) {
            // 在日志中记录正在注册的partitionId
            LOG.debug("registering {}", partitionId);
            // 尝试在registeredHandlers Map中将partitionId映射到一个新的TaskEventHandler对象
            // 如果put方法返回null，说明该partitionId之前没有注册过，此时注册成功
            // 如果返回的不是null，说明该partitionId之前已经被注册过，此时抛出IllegalStateException
            if (registeredHandlers.put(partitionId, new TaskEventHandler()) != null) {
                throw new IllegalStateException(
                        "Partition "
                                + partitionId
                                + " already registered at task event dispatcher.");
            }
        }
    }

    /**
     * Removes the given partition from listening to incoming task events, thus forbidding calls to
     * {@link #subscribeToEvent(ResultPartitionID, EventListener, Class)}.
     *
     * @param partitionId the partition ID
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 卸载registerPartition
    */
    public void unregisterPartition(ResultPartitionID partitionId) {
        // 检查传入的partitionId是否为空，如果为空则抛出NullPointerException异常
        checkNotNull(partitionId);
        // 使用synchronized关键字对registeredHandlers对象进行同步，以确保在多线程环境下对registeredHandlers的访问是线程安全的
        synchronized (registeredHandlers) {
            LOG.debug("unregistering {}", partitionId);
            // NOTE: tolerate un-registration of non-registered task (unregister is always called
            //       in the cleanup phase of a task even if it never came to the registration - see
            //       Task.java)
            // 从registeredHandlers中移除与partitionId相关联的handler（如果存在的话）
            registeredHandlers.remove(partitionId);
        }
    }

    /**
     * Subscribes a listener to this dispatcher for events on a partition.
     *
     * @param partitionId ID of the partition to subscribe for (must be registered via {@link
     *     #registerPartition(ResultPartitionID)} first!)
     * @param eventListener the event listener to subscribe
     * @param eventType event type to subscribe to
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 订阅特定分区的事件。
     * @param partitionId   要订阅事件的分区ID
     * @param eventListener 接收事件的监听器
     * @param eventType     要订阅的事件类型（TaskEvent的子类）
     * @throws IllegalStateException 如果指定的分区没有在任务事件调度器中注册
     *
    */
    public void subscribeToEvent(
            ResultPartitionID partitionId,
            EventListener<TaskEvent> eventListener,
            Class<? extends TaskEvent> eventType) {
        // 检查分区ID是否为空
        checkNotNull(partitionId);
        // 检查事件监听器是否为空
        checkNotNull(eventListener);
        // 检查事件类型是否为空
        checkNotNull(eventType);

        TaskEventHandler taskEventHandler;
        // 同步访问registeredHandlers集合，防止并发修改
        synchronized (registeredHandlers) {
            // 从registeredHandlers集合中获取指定分区ID对应的事件处理器
            taskEventHandler = registeredHandlers.get(partitionId);
        }
        // 如果事件处理器为空（即指定分区没有在任务事件调度器中注册）
        if (taskEventHandler == null) {
            // 抛出IllegalStateException异常
            throw new IllegalStateException(
                    "Partition " + partitionId + " not registered at task event dispatcher.");
        }
        // 使用获取到的事件处理器订阅事件
        taskEventHandler.subscribe(eventListener, eventType);
    }

    /**
     * Publishes the event to the registered {@link EventListener} instances.
     *
     * <p>This method is either called directly from a {@link LocalInputChannel} or the network I/O
     * thread on behalf of a {@link RemoteInputChannel}.
     *
     * @return whether the event was published to a registered event handler (initiated via {@link
     *     #registerPartition(ResultPartitionID)}) or not
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将任务事件发布到指定的结果分区ID对应的任务事件处理器中。
     * @param partitionId 结果分区的ID，用于确定将事件发布到哪个任务事件处理器
     * @param event       要发布的任务事件
     * @return 如果成功将事件发布到任务事件处理器，则返回true；否则返回false
    */
    @Override
    public boolean publish(ResultPartitionID partitionId, TaskEvent event) {
        // 检查partitionId和event是否为null，如果是则抛出NullPointerException异常
        checkNotNull(partitionId);
        checkNotNull(event);
        // 声明一个TaskEventHandler类型的变量，用于存储从registeredHandlers中获取的任务事件处理器
        TaskEventHandler taskEventHandler;
        // 同步块，确保在访问registeredHandlers集合时线程安全
        synchronized (registeredHandlers) {
            // 从registeredHandlers集合中根据partitionId获取对应的任务事件处理器
            taskEventHandler = registeredHandlers.get(partitionId);
        }
        // 如果获取到的任务事件处理器不为null
        if (taskEventHandler != null) {
            // 调用任务事件处理器的publish方法，将事件发布到该处理器
            taskEventHandler.publish(event);
            // 如果发布成功，则返回true
            return true;
        }
        // 如果没有找到对应的任务事件处理器，则返回false
        return false;
    }

    /** Removes all registered event handlers. */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 移除所有已注册的事件处理器
    */
    public void clearAll() {
        synchronized (registeredHandlers) {
            registeredHandlers.clear();
        }
    }
}
