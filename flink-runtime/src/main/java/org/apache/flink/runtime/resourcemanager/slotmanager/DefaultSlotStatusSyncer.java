/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotOccupiedException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** Default implementation of {@link SlotStatusSyncer} for fine-grained slot management. */
public class DefaultSlotStatusSyncer implements SlotStatusSyncer {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSlotStatusSyncer.class);

    private final Set<AllocationID> pendingSlotAllocations = new HashSet<>();
    /** Timeout for slot requests to the task manager. */
    private final Time taskManagerRequestTimeout;

    @Nullable private TaskManagerTracker taskManagerTracker;
    @Nullable private ResourceTracker resourceTracker;
    @Nullable private Executor mainThreadExecutor;
    @Nullable private ResourceManagerId resourceManagerId;

    private boolean started = false;

    public DefaultSlotStatusSyncer(Time taskManagerRequestTimeout) {
        this.taskManagerRequestTimeout = Preconditions.checkNotNull(taskManagerRequestTimeout);
    }

    @Override
    public void initialize(
            TaskManagerTracker taskManagerTracker,
            ResourceTracker resourceTracker,
            ResourceManagerId resourceManagerId,
            Executor mainThreadExecutor) {
        this.taskManagerTracker = Preconditions.checkNotNull(taskManagerTracker);
        this.resourceTracker = Preconditions.checkNotNull(resourceTracker);
        this.mainThreadExecutor = Preconditions.checkNotNull(mainThreadExecutor);
        this.resourceManagerId = Preconditions.checkNotNull(resourceManagerId);
        this.pendingSlotAllocations.clear();
        started = true;
    }

    @Override
    public void close() {
        this.taskManagerTracker = null;
        this.resourceTracker = null;
        this.mainThreadExecutor = null;
        this.resourceManagerId = null;
        this.pendingSlotAllocations.clear();
        started = false;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 从TaskManager分配插槽。
    */
    @Override
    public CompletableFuture<Void> allocateSlot(
            InstanceID instanceId,
            JobID jobId,
            String targetAddress,
            ResourceProfile resourceProfile) {
        // 检查输入参数是否非空
        Preconditions.checkNotNull(instanceId);
        Preconditions.checkNotNull(jobId);
        Preconditions.checkNotNull(targetAddress);
        Preconditions.checkNotNull(resourceProfile);
        // 检查服务是否已经开始
        checkStarted();
        // 创建一个新的AllocationID用于此次的slot分配
        final AllocationID allocationId = new AllocationID();
        /**
         * 获取TaskManagerInfo
         * TaskManagerTracker 跟踪TaskManager的资源和插槽状态
         */
        // 从taskManagerTracker中获取对应instanceId的TaskManagerInfo
        final Optional<TaskManagerInfo> taskManager =
                taskManagerTracker.getRegisteredTaskManager(instanceId);
        // 检查是否找到了已注册的task manager
        Preconditions.checkState(
                taskManager.isPresent(),
                "Could not find a registered task manager for instance id " + instanceId + '.');
        // 获取TaskExecutorGateway进行后续的RPC调用
        final TaskExecutorGateway gateway =
                taskManager.get().getTaskExecutorConnection().getTaskExecutorGateway();
        // 获取task manager的ResourceID
        final ResourceID resourceId = taskManager.get().getTaskExecutorConnection().getResourceID();
        // 记录日志，表明开始为特定job从指定task manager分配slot
        LOG.info(
                "Starting allocation of slot {} from {} for job {} with resource profile {}.",
                allocationId,
                resourceId,
                jobId,
                resourceProfile);
        /**
         * 将Slot状态更新为PENDING
         * FREE,
         * PENDING,
         * ALLOCATED
         * FineGrainedTaskManagerRegistration.notifyAllocation
         */
        // 通知slot的状态为PENDING
        taskManagerTracker.notifySlotStatus(
                allocationId, jobId, instanceId, resourceProfile, SlotState.PENDING);
        /**
         *通知跟踪器获取给定作业的具有给定资源配置文件的资源
         * 就是更新Map<JobID, JobScopedResourceTracker> trackers
         */
        // 通知resource tracker已获取资源
        resourceTracker.notifyAcquiredResource(jobId, resourceProfile);
        // 将此次的allocationId添加到pending的slot分配列表中
        pendingSlotAllocations.add(allocationId);
        /** RPC开始调用请求资源 */
        // RPC call to the task manager
        // 发起RPC调用到task manager请求slot
        CompletableFuture<Acknowledge> requestFuture =
                gateway.requestSlot(
                        SlotID.getDynamicSlotID(resourceId),
                        jobId,
                        allocationId,
                        resourceProfile,
                        targetAddress,
                        resourceManagerId,
                        taskManagerRequestTimeout);
        // 创建一个新的CompletableFuture来包装结果
        CompletableFuture<Void> returnedFuture = new CompletableFuture<>();
        // 异步处理RPC调用的结果
        FutureUtils.assertNoException(
                requestFuture.handleAsync(
                        (Acknowledge acknowledge, Throwable throwable) -> {
                            // 如果从pending列表中移除allocationId失败，说明分配已经完成或取消
                            if (!pendingSlotAllocations.remove(allocationId)) {
                                LOG.debug(
                                        "Ignoring slot allocation update from task manager {} for allocation {} and job {}, because the allocation was already completed or cancelled.",
                                        instanceId,
                                        allocationId,
                                        jobId);
                                // 完成返回的Future，无论成功还是失败，都返回null
                                returnedFuture.complete(null);
                                return null;
                            }
                            //获取具有给定allocationId的已分配插槽的 TaskManagerSlotInformation。
                            if (!taskManagerTracker
                                    .getAllocatedOrPendingSlot(allocationId)
                                    .isPresent()) {
                                LOG.debug(
                                        "The slot {} has been removed before. Ignore the future.",
                                        allocationId);
                                // 完成返回的 Future，无论成功还是失败，都返回 null
                                // 这可能是因为我们不关心这个特定的分配更新，因为分配已经完成了
                                returnedFuture.complete(null);
                                return null;
                            }
                            if (acknowledge != null) {
                                LOG.trace(
                                        "Completed allocation of allocation {} for job {}.",
                                        allocationId,
                                        jobId);
                                // 通知任务管理器跟踪器关于槽位的状态更新 已经申请
                                taskManagerTracker.notifySlotStatus(
                                        allocationId,
                                        jobId,
                                        instanceId,
                                        resourceProfile,
                                        SlotState.ALLOCATED);
                                returnedFuture.complete(null);
                            } else {
                                if (throwable instanceof SlotOccupiedException) {
                                    LOG.error("Should not get this exception.", throwable);
                                } else {
                                    // TODO If the taskManager does not have enough resource, we
                                    // may endlessly allocate slot on it until the next heartbeat.
                                    LOG.warn(
                                            "Slot allocation for allocation {} for job {} failed.",
                                            allocationId,
                                            jobId,
                                            throwable);
                                    // 通知资源跟踪器该作业已失去资源
                                    resourceTracker.notifyLostResource(jobId, resourceProfile);
                                    // 通知任务管理器跟踪器关于槽位的状态更新（例如，变为空闲）
                                    taskManagerTracker.notifySlotStatus(
                                            allocationId,
                                            jobId,
                                            instanceId,
                                            resourceProfile,
                                            SlotState.FREE);
                                }
                                returnedFuture.completeExceptionally(throwable);
                            }
                            return null;
                        },
                        mainThreadExecutor));
        // 返回处理后的Future
        return returnedFuture;
    }

    @Override
    public void freeSlot(AllocationID allocationId) {
        Preconditions.checkNotNull(allocationId);
        checkStarted();
        LOG.info("Freeing slot {}.", allocationId);

        final Optional<TaskManagerSlotInformation> slotOptional =
                taskManagerTracker.getAllocatedOrPendingSlot(allocationId);
        if (!slotOptional.isPresent()) {
            LOG.warn("Try to free unknown slot {}.", allocationId);
            return;
        }

        final TaskManagerSlotInformation slot = slotOptional.get();
        if (slot.getState() == SlotState.PENDING) {
            pendingSlotAllocations.remove(allocationId);
        }
        resourceTracker.notifyLostResource(slot.getJobId(), slot.getResourceProfile());
        taskManagerTracker.notifySlotStatus(
                allocationId,
                slot.getJobId(),
                slot.getInstanceId(),
                slot.getResourceProfile(),
                SlotState.FREE);
    }

    @Override
    public boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport) {
        Preconditions.checkNotNull(slotReport);
        Preconditions.checkNotNull(instanceId);
        checkStarted();
        final Optional<TaskManagerInfo> taskManager =
                taskManagerTracker.getRegisteredTaskManager(instanceId);

        if (!taskManager.isPresent()) {
            LOG.debug(
                    "Received slot report for unknown task manager with instance id {}. Ignoring this report.",
                    instanceId);
            return false;
        }

        LOG.debug("Received slot report from instance {}: {}.", instanceId, slotReport);

        boolean canApplyPreviousAllocations = true;
        final Set<AllocationID> reportedAllocationIds = new HashSet<>();
        slotReport
                .iterator()
                .forEachRemaining(
                        slotStatus -> reportedAllocationIds.add(slotStatus.getAllocationID()));

        for (TaskManagerSlotInformation slot :
                new HashSet<>(taskManager.get().getAllocatedSlots().values())) {
            // Only free the slot which is previously allocated. For pending slot, we might wait for
            // the next slot report or the acknowledgement of the allocation request.
            if (!reportedAllocationIds.contains(slot.getAllocationId())
                    && slot.getState() == SlotState.ALLOCATED) {
                LOG.info("Freeing slot {} by slot report.", slot.getAllocationId());
                taskManagerTracker.notifySlotStatus(
                        slot.getAllocationId(),
                        slot.getJobId(),
                        slot.getInstanceId(),
                        slot.getResourceProfile(),
                        SlotState.FREE);
                resourceTracker.notifyLostResource(slot.getJobId(), slot.getResourceProfile());
                canApplyPreviousAllocations = false;
            }
        }

        for (SlotStatus slotStatus : slotReport) {
            if (slotStatus.getAllocationID() == null) {
                continue;
            }
            if (!syncAllocatedSlotStatus(slotStatus, taskManager.get())) {
                canApplyPreviousAllocations = false;
            }
        }
        return canApplyPreviousAllocations;
    }

    private boolean syncAllocatedSlotStatus(SlotStatus slotStatus, TaskManagerInfo taskManager) {
        final AllocationID allocationId = Preconditions.checkNotNull(slotStatus.getAllocationID());
        final JobID jobId = Preconditions.checkNotNull(slotStatus.getJobID());
        final ResourceProfile resourceProfile =
                Preconditions.checkNotNull(slotStatus.getResourceProfile());

        if (taskManager.getAllocatedSlots().containsKey(allocationId)) {
            if (taskManager.getAllocatedSlots().get(allocationId).getState() == SlotState.PENDING) {
                // Allocation Complete
                final TaskManagerSlotInformation slot =
                        taskManager.getAllocatedSlots().get(allocationId);
                pendingSlotAllocations.remove(slot.getAllocationId());
                taskManagerTracker.notifySlotStatus(
                        slot.getAllocationId(),
                        slot.getJobId(),
                        slot.getInstanceId(),
                        slot.getResourceProfile(),
                        SlotState.ALLOCATED);
            }
            return true;
        } else {
            Preconditions.checkState(
                    !taskManagerTracker.getAllocatedOrPendingSlot(allocationId).isPresent(),
                    "Duplicated allocation for " + allocationId);
            taskManagerTracker.notifySlotStatus(
                    allocationId,
                    jobId,
                    taskManager.getInstanceId(),
                    resourceProfile,
                    SlotState.ALLOCATED);
            resourceTracker.notifyAcquiredResource(jobId, resourceProfile);
            return false;
        }
    }

    @Override
    public void freeInactiveSlots(JobID jobId) {
        checkStarted();
        for (TaskManagerInfo taskManagerInfo :
                taskManagerTracker.getTaskManagersWithAllocatedSlotsForJob(jobId)) {
            taskManagerInfo
                    .getTaskExecutorConnection()
                    .getTaskExecutorGateway()
                    .freeInactiveSlots(jobId, taskManagerRequestTimeout);
        }
    }

    private void checkStarted() {
        Preconditions.checkState(started);
        Preconditions.checkNotNull(taskManagerTracker);
        Preconditions.checkNotNull(resourceTracker);
        Preconditions.checkNotNull(mainThreadExecutor);
        Preconditions.checkNotNull(resourceManagerId);
    }
}
