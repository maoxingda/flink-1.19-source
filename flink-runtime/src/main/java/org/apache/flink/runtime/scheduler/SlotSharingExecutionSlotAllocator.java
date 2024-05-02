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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequest;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequestBulkChecker;
import org.apache.flink.runtime.scheduler.SharedSlotProfileRetriever.SharedSlotProfileRetrieverFactory;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Allocates {@link LogicalSlot}s from physical shared slots.
 *
 * <p>The allocator maintains a shared slot for each {@link ExecutionSlotSharingGroup}. It allocates
 * a physical slot for the shared slot and then allocates logical slots from it for scheduled tasks.
 * The physical slot is lazily allocated for a shared slot, upon any hosted subtask asking for the
 * shared slot. Each subsequent sharing subtask allocates a logical slot from the existing shared
 * slot. The shared/physical slot can be released only if all the requested logical slots are
 * released or canceled.
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 从物理共享插槽中分配  LogicalSlot
*/
class SlotSharingExecutionSlotAllocator implements ExecutionSlotAllocator {
    private static final Logger LOG =
            LoggerFactory.getLogger(SlotSharingExecutionSlotAllocator.class);

    private final PhysicalSlotProvider slotProvider;

    private final boolean slotWillBeOccupiedIndefinitely;

    private final SlotSharingStrategy slotSharingStrategy;

    private final Map<ExecutionSlotSharingGroup, SharedSlot> sharedSlots;

    private final SharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory;

    private final PhysicalSlotRequestBulkChecker bulkChecker;

    private final Time allocationTimeout;

    private final Function<ExecutionVertexID, ResourceProfile> resourceProfileRetriever;

    SlotSharingExecutionSlotAllocator(
            PhysicalSlotProvider slotProvider,
            boolean slotWillBeOccupiedIndefinitely,
            SlotSharingStrategy slotSharingStrategy,
            SharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory,
            PhysicalSlotRequestBulkChecker bulkChecker,
            Time allocationTimeout,
            Function<ExecutionVertexID, ResourceProfile> resourceProfileRetriever) {
        this.slotProvider = checkNotNull(slotProvider);
        this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
        this.slotSharingStrategy = checkNotNull(slotSharingStrategy);
        this.sharedSlotProfileRetrieverFactory = checkNotNull(sharedSlotProfileRetrieverFactory);
        this.bulkChecker = checkNotNull(bulkChecker);
        this.allocationTimeout = checkNotNull(allocationTimeout);
        this.resourceProfileRetriever = checkNotNull(resourceProfileRetriever);
        this.sharedSlots = new IdentityHashMap<>();

        this.slotProvider.disableBatchSlotRequestTimeoutCheck();
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 
    */
    @Override
    public Map<ExecutionAttemptID, ExecutionSlotAssignment> allocateSlotsFor(
            List<ExecutionAttemptID> executionAttemptIds) {
        /** 创建一个HashMap，名为vertexIdToExecutionId，用于存储ExecutionVertexID到ExecutionAttemptID的映射。 */
        final Map<ExecutionVertexID, ExecutionAttemptID> vertexIdToExecutionId = new HashMap<>();
        /**
         * List<ExecutionAttemptID> executionAttemptIds
         * 将ExecutionVertexID和ExecutionAttemptID存入映射中
         */
        executionAttemptIds.forEach(
                executionId ->
                        vertexIdToExecutionId.put(executionId.getExecutionVertexId(), executionId));
        /** 状态检查 vertexIdToExecutionId的大小与executionAttemptIds的大小相同 */
        checkState(
                vertexIdToExecutionId.size() == executionAttemptIds.size(),
                "SlotSharingExecutionSlotAllocator does not support one execution vertex to have multiple concurrent executions");
        /**
         * 使用Java Stream API从executionAttemptIds列表中提取出所有的ExecutionVertexID，
         * 并将它们收集到List<ExecutionVertexID>列表中
         */
        final List<ExecutionVertexID> vertexIds =
                executionAttemptIds.stream()
                        .map(ExecutionAttemptID::getExecutionVertexId)
                        .collect(Collectors.toList());
        /**
         * allocateSlotsForVertices 基于顶点进行slot申请 返回值List<SlotExecutionVertexAssignment>
         */
        return allocateSlotsForVertices(vertexIds).stream()
                .collect(
                        /**
                         * 构建Map<ExecutionAttemptID, ExecutionSlotAssignment> 结构
                         */
                        Collectors.toMap(
                                vertexAssignment ->
                                        vertexIdToExecutionId.get(
                                                vertexAssignment.getExecutionVertexId()),
                                vertexAssignment ->
                                        new ExecutionSlotAssignment(
                                                vertexIdToExecutionId.get(
                                                        vertexAssignment.getExecutionVertexId()),
                                                vertexAssignment.getLogicalSlotFuture())));
    }

    /**
     * Creates logical {@link SlotExecutionVertexAssignment}s from physical shared slots.
     *
     * <p>The allocation has the following steps:
     *
     * <ol>
     *   <li>Map the executions to {@link ExecutionSlotSharingGroup}s using {@link
     *       SlotSharingStrategy}
     *   <li>Check which {@link ExecutionSlotSharingGroup}s already have shared slot
     *   <li>For all involved {@link ExecutionSlotSharingGroup}s which do not have a shared slot
     *       yet:
     *   <li>Create a {@link SlotProfile} future using {@link SharedSlotProfileRetriever} and then
     *   <li>Allocate a physical slot from the {@link PhysicalSlotProvider}
     *   <li>Create a shared slot based on the returned physical slot futures
     *   <li>Allocate logical slot futures for the executions from all corresponding shared slots.
     *   <li>If a physical slot request fails, associated logical slot requests are canceled within
     *       the shared slot
     *   <li>Generate {@link SlotExecutionVertexAssignment}s based on the logical slot futures and
     *       returns the results.
     * </ol>
     *
     * @param executionVertexIds Execution vertices to allocate slots for
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 为ExecutionVertex进行Slot分配
    */
    private List<SlotExecutionVertexAssignment> allocateSlotsForVertices(
            List<ExecutionVertexID> executionVertexIds) {
        /**
         * 创建SharedSlotProfileRetriever
         * 计算SlotProfile以分配用于执行的Slot，共享该Slot。
         */
        SharedSlotProfileRetriever sharedSlotProfileRetriever =
                sharedSlotProfileRetrieverFactory.createFromBulk(new HashSet<>(executionVertexIds));
        /**
         * 使用Java Stream API将executionVertexIds列表中的每个ExecutionVertexID根据其共享槽位策略
         * （slotSharingStrategy）的getExecutionSlotSharingGroup方法返回的ExecutionSlotSharingGroup进行分组。
         * 总结：基于ExecutionSlotSharingGroup进行分组
         */
        Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> executionsByGroup =
                executionVertexIds.stream()
                        .collect(
                                Collectors.groupingBy(
                                        slotSharingStrategy::getExecutionSlotSharingGroup));
        /**
         * 创建一个空的HashMap（slots），用于存储每个ExecutionSlotSharingGroup到其对应的SharedSlot的映射。
         */
        Map<ExecutionSlotSharingGroup, SharedSlot> slots = new HashMap<>(executionsByGroup.size());
        /**
         * 创建一个HashSet（groupsToAssign），用于存储待分配槽位的ExecutionSlotSharingGroup集合。
         */
        Set<ExecutionSlotSharingGroup> groupsToAssign = new HashSet<>(executionsByGroup.keySet());
        /** 尝试分配已存在的共享Slot */
        Map<ExecutionSlotSharingGroup, SharedSlot> assignedSlots =
                tryAssignExistingSharedSlots(groupsToAssign);
        /** 分配的结果（assignedSlots）被合并到slots映射中， */
        slots.putAll(assignedSlots);
        /** 并从groupsToAssign集合中移除已分配的组。 */
        groupsToAssign.removeAll(assignedSlots.keySet());
        /** 如果groupsToAssign集合中还有未分配的组 */
        if (!groupsToAssign.isEmpty()) {
            /** 调用allocateSharedSlots方法为它们分配新的共享Slot。 */
            Map<ExecutionSlotSharingGroup, SharedSlot> allocatedSlots =
                    allocateSharedSlots(groupsToAssign, sharedSlotProfileRetriever);
            /** 分配的结果被合并到slots映射中 */
            slots.putAll(allocatedSlots);
            /** 并从groupsToAssign集合中移除已分配的组。 */
            groupsToAssign.removeAll(allocatedSlots.keySet());
            /** 确保所有组都已分配槽位。 */
            Preconditions.checkState(groupsToAssign.isEmpty());
        }
        /**
         * 获取所有组的共享Slot后，将Slot分配给这些共享槽位。
         * 结果是Map<ExecutionVertexID, SlotExecutionVertexAssignment>映射，
         * 其中键是ExecutionVertexID，值是对应的SlotExecutionVertexAssignment。
         */
        Map<ExecutionVertexID, SlotExecutionVertexAssignment> assignments =
                allocateLogicalSlotsFromSharedSlots(slots, executionsByGroup);

        // we need to pass the slots map to the createBulk method instead of using the allocator's
        // 'sharedSlots'
        // because if any physical slots have already failed, their shared slots have been removed
        // from the allocator's 'sharedSlots' by failed logical slots.
        /**
         * 创建一个SharingPhysicalSlotRequestBulk对象，它表示一个请求，用于请求物理资源以支持这些共享槽位。
         */
        SharingPhysicalSlotRequestBulk bulk = createBulk(slots, executionsByGroup);
        /**
         * 使用bulkChecker安排一个超时检查，以确保这些请求在指定的allocationTimeout内得到响应。
         */
        bulkChecker.schedulePendingRequestBulkTimeoutCheck(bulk, allocationTimeout);
        /** 返回分配结果 */
        return executionVertexIds.stream().map(assignments::get).collect(Collectors.toList());
    }

    @Override
    public void cancel(ExecutionAttemptID executionAttemptId) {
        cancelLogicalSlotRequest(executionAttemptId.getExecutionVertexId(), null);
    }

    private void cancelLogicalSlotRequest(ExecutionVertexID executionVertexId, Throwable cause) {
        ExecutionSlotSharingGroup executionSlotSharingGroup =
                slotSharingStrategy.getExecutionSlotSharingGroup(executionVertexId);
        checkNotNull(
                executionSlotSharingGroup,
                "There is no ExecutionSlotSharingGroup for ExecutionVertexID " + executionVertexId);
        SharedSlot slot = sharedSlots.get(executionSlotSharingGroup);
        if (slot != null) {
            slot.cancelLogicalSlotRequest(executionVertexId, cause);
        } else {
            LOG.debug(
                    "There is no SharedSlot for ExecutionSlotSharingGroup of ExecutionVertexID {}",
                    executionVertexId);
        }
    }

    private static Map<ExecutionVertexID, SlotExecutionVertexAssignment>
            allocateLogicalSlotsFromSharedSlots(
                    Map<ExecutionSlotSharingGroup, SharedSlot> slots,
                    Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> executionsByGroup) {

        Map<ExecutionVertexID, SlotExecutionVertexAssignment> assignments = new HashMap<>();

        for (Map.Entry<ExecutionSlotSharingGroup, List<ExecutionVertexID>> entry :
                executionsByGroup.entrySet()) {
            ExecutionSlotSharingGroup group = entry.getKey();
            List<ExecutionVertexID> executionIds = entry.getValue();

            for (ExecutionVertexID executionId : executionIds) {
                CompletableFuture<LogicalSlot> logicalSlotFuture =
                        slots.get(group).allocateLogicalSlot(executionId);
                SlotExecutionVertexAssignment assignment =
                        new SlotExecutionVertexAssignment(executionId, logicalSlotFuture);
                assignments.put(executionId, assignment);
            }
        }

        return assignments;
    }

    private Map<ExecutionSlotSharingGroup, SharedSlot> tryAssignExistingSharedSlots(
            Set<ExecutionSlotSharingGroup> executionSlotSharingGroups) {
        Map<ExecutionSlotSharingGroup, SharedSlot> assignedSlots =
                new HashMap<>(executionSlotSharingGroups.size());
        for (ExecutionSlotSharingGroup group : executionSlotSharingGroups) {
            SharedSlot sharedSlot = sharedSlots.get(group);
            if (sharedSlot != null) {
                assignedSlots.put(group, sharedSlot);
            }
        }
        return assignedSlots;
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 给定的 ExecutionSlotSharingGroup 集合分配共享槽位（SharedSlot）
     * 方法接受两个参数：一个 ExecutionSlotSharingGroup 的集合 executionSlotSharingGroups 和一个 SharedSlotProfileRetriever 类型的 sharedSlotProfileRetriever。
     * 方法返回一个 Map<ExecutionSlotSharingGroup, SharedSlot>，但在提供的代码片段中，这个返回值的填充并没有显示。
    */
    private Map<ExecutionSlotSharingGroup, SharedSlot> allocateSharedSlots(
            Set<ExecutionSlotSharingGroup> executionSlotSharingGroups,
            SharedSlotProfileRetriever sharedSlotProfileRetriever) {
        /**
         * 创建List<PhysicalSlotRequest> PhysicalSlotRequest 表示对物理Slot的请求
         */
        List<PhysicalSlotRequest> slotRequests = new ArrayList<>();
        /**
         * Map<ExecutionSlotSharingGroup, SharedSlot>
         * ExecutionSlotSharingGroup:运行同一共享Slot的执行顶点
         * SharedSlot:共享Slot实现
         */
        Map<ExecutionSlotSharingGroup, SharedSlot> allocatedSlots = new HashMap<>();
        /**
         * Map<SlotRequestId, ExecutionSlotSharingGroup>
         * 将物理Slot请求的 ID（SlotRequestId）映射到对应的 ExecutionSlotSharingGroup。
         */
        Map<SlotRequestId, ExecutionSlotSharingGroup> requestToGroup = new HashMap<>();
        /**
         * Map<SlotRequestId, ResourceProfile>
         * 将物理Slot请求的 ID（SlotRequestId）映射到对应的 ResourceProfile。
         * ResourceProfile:Slot的不可变资源配置文件
         */
        Map<SlotRequestId, ResourceProfile> requestToPhysicalResources = new HashMap<>();
        /**
         * 遍历执行槽位共享组
         */
        for (ExecutionSlotSharingGroup group : executionSlotSharingGroups) {
            /** 创建一个新的 SlotRequestId 实例，作为物理槽位请求的 ID。 */
            SlotRequestId physicalSlotRequestId = new SlotRequestId();
            /**
             * 调用 getPhysicalSlotResourceProfile(group) 方法来获取该组的物理Slot资源配置。
             */
            ResourceProfile physicalSlotResourceProfile = getPhysicalSlotResourceProfile(group);
            /**
             * 获取与该组和资源配置匹配的Slot配置（SlotProfile）。
             */
            SlotProfile slotProfile =
                    sharedSlotProfileRetriever.getSlotProfile(group, physicalSlotResourceProfile);
            /** 创建一个新的 PhysicalSlotRequest 实例， */
            PhysicalSlotRequest request =
                    new PhysicalSlotRequest(
                            physicalSlotRequestId, slotProfile, slotWillBeOccupiedIndefinitely);
            /** 将request添加到 slotRequests 列表中。 */
            slotRequests.add(request);
            /** 将物理Slot请求的 ID 与对应的 ExecutionSlotSharingGroup 添加到 requestToGroup 映射中。 */
            requestToGroup.put(physicalSlotRequestId, group);
            /** 将物理槽位Slot ID 与对应的资源配置添加到 requestToPhysicalResources 映射中 */
            requestToPhysicalResources.put(physicalSlotRequestId, physicalSlotResourceProfile);
        }
        /**
         *
         * slotProvider.allocatePhysicalSlots(slotRequests) 发起槽位分配请求，
         * 并得到了一个 Map，其键是 SlotRequestId，值是一个 CompletableFuture<PhysicalSlotRequest.Result>。
         * 这个 CompletableFuture 将在未来某个时刻完成，并包含Slot分配的结果。
         */
        Map<SlotRequestId, CompletableFuture<PhysicalSlotRequest.Result>> allocateResult =
                slotProvider.allocatePhysicalSlots(slotRequests);
        /**
         * 遍历 allocateResult 映射，对每个 slotRequestId 和其对应的 resultCompletableFuture 进行处理。
         */
        allocateResult.forEach(
                (slotRequestId, resultCompletableFuture) -> {
                    /** 请求ID关联的 ExecutionSlotSharingGroup */
                    ExecutionSlotSharingGroup group = requestToGroup.get(slotRequestId);
                    /** thenApply 方法来创建一个新的 CompletableFuture<PhysicalSlot> */
                    CompletableFuture<PhysicalSlot> physicalSlotFuture =
                            resultCompletableFuture.thenApply(
                                    /**
                                     * CompletableFuture 将在 resultCompletableFuture 完成时执行，
                                     * 并应用 PhysicalSlotRequest.Result::getPhysicalSlot 方法来获取实际的
                                     * PhysicalSlot。
                                     */
                                    PhysicalSlotRequest.Result::getPhysicalSlot);
                    /**
                     * 创建一个新的 SharedSlot 实例，它封装了Slot请求 ID、资源配置、执行Slot共享组、物理Slot的 CompletableFuture
                     */
                    SharedSlot slot =
                            new SharedSlot(
                                    slotRequestId,
                                    requestToPhysicalResources.get(slotRequestId),
                                    group,
                                    physicalSlotFuture,
                                    slotWillBeOccupiedIndefinitely,
                                    this::releaseSharedSlot);
                    /** 将新创建的 SharedSlot 添加到 allocatedSlots 映射中 */
                    allocatedSlots.put(group, slot);
                    /** 检查 sharedSlots 映射中是否已包含当前 group 的键。 */
                    Preconditions.checkState(!sharedSlots.containsKey(group));
                    /** 将 SharedSlot 添加到 sharedSlots 映射中 */
                    sharedSlots.put(group, slot);
                });
        return allocatedSlots;
    }

    private void releaseSharedSlot(ExecutionSlotSharingGroup executionSlotSharingGroup) {
        SharedSlot slot = sharedSlots.remove(executionSlotSharingGroup);
        Preconditions.checkNotNull(slot);
        Preconditions.checkState(
                slot.isEmpty(),
                "Trying to remove a shared slot with physical request id %s which has assigned logical slots",
                slot.getPhysicalSlotRequestId());
        slotProvider.cancelSlotRequest(
                slot.getPhysicalSlotRequestId(),
                new FlinkException(
                        "Slot is being returned from SlotSharingExecutionSlotAllocator."));
    }

    private ResourceProfile getPhysicalSlotResourceProfile(
            ExecutionSlotSharingGroup executionSlotSharingGroup) {
        if (!executionSlotSharingGroup.getResourceProfile().equals(ResourceProfile.UNKNOWN)) {
            return executionSlotSharingGroup.getResourceProfile();
        } else {
            return executionSlotSharingGroup.getExecutionVertexIds().stream()
                    .reduce(
                            ResourceProfile.ZERO,
                            (r, e) -> r.merge(resourceProfileRetriever.apply(e)),
                            ResourceProfile::merge);
        }
    }

    private SharingPhysicalSlotRequestBulk createBulk(
            Map<ExecutionSlotSharingGroup, SharedSlot> slots,
            Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> executions) {
        Map<ExecutionSlotSharingGroup, ResourceProfile> pendingRequests =
                executions.keySet().stream()
                        .collect(
                                Collectors.toMap(
                                        group -> group,
                                        group ->
                                                slots.get(group).getPhysicalSlotResourceProfile()));
        SharingPhysicalSlotRequestBulk bulk =
                new SharingPhysicalSlotRequestBulk(
                        executions, pendingRequests, this::cancelLogicalSlotRequest);
        registerPhysicalSlotRequestBulkCallbacks(slots, executions.keySet(), bulk);
        return bulk;
    }

    private static void registerPhysicalSlotRequestBulkCallbacks(
            Map<ExecutionSlotSharingGroup, SharedSlot> slots,
            Iterable<ExecutionSlotSharingGroup> executions,
            SharingPhysicalSlotRequestBulk bulk) {
        for (ExecutionSlotSharingGroup group : executions) {
            CompletableFuture<PhysicalSlot> slotContextFuture =
                    slots.get(group).getSlotContextFuture();
            slotContextFuture.thenAccept(
                    physicalSlot -> bulk.markFulfilled(group, physicalSlot.getAllocationId()));
            slotContextFuture.exceptionally(
                    t -> {
                        // clear the bulk to stop the fulfillability check
                        bulk.clearPendingRequests();
                        return null;
                    });
        }
    }

    /** The slot assignment for an {@link ExecutionVertex}. */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 为 ExecutionVertex 对应的Slot分配
    */
    private static class SlotExecutionVertexAssignment {

        private final ExecutionVertexID executionVertexId;

        private final CompletableFuture<LogicalSlot> logicalSlotFuture;

        SlotExecutionVertexAssignment(
                ExecutionVertexID executionVertexId,
                CompletableFuture<LogicalSlot> logicalSlotFuture) {
            this.executionVertexId = checkNotNull(executionVertexId);
            this.logicalSlotFuture = checkNotNull(logicalSlotFuture);
        }

        ExecutionVertexID getExecutionVertexId() {
            return executionVertexId;
        }

        CompletableFuture<LogicalSlot> getLogicalSlotFuture() {
            return logicalSlotFuture;
        }
    }
}
