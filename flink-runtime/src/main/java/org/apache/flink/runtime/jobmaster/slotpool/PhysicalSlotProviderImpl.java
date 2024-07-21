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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The provider serves physical slot requests. */
public class PhysicalSlotProviderImpl implements PhysicalSlotProvider {
    private static final Logger LOG = LoggerFactory.getLogger(PhysicalSlotProviderImpl.class);

    private final SlotSelectionStrategy slotSelectionStrategy;

    private final SlotPool slotPool;

    public PhysicalSlotProviderImpl(
            SlotSelectionStrategy slotSelectionStrategy, SlotPool slotPool) {
        this.slotSelectionStrategy = checkNotNull(slotSelectionStrategy);
        this.slotPool = checkNotNull(slotPool);
    }

    @Override
    public void disableBatchSlotRequestTimeoutCheck() {
        slotPool.disableBatchSlotRequestTimeoutCheck();
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 申请物理Slots
    */
    @Override
    public Map<SlotRequestId, CompletableFuture<PhysicalSlotRequest.Result>> allocatePhysicalSlots(
            Collection<PhysicalSlotRequest> physicalSlotRequests) {
        /** 使用for循环遍历传入的physicalSlotRequests集合 记录日志 */
        for (PhysicalSlotRequest physicalSlotRequest : physicalSlotRequests) {
            LOG.debug(
                    "Received slot request [{}] with resource requirements: {}",
                    physicalSlotRequest.getSlotRequestId(),
                    physicalSlotRequest.getSlotProfile().getPhysicalSlotResourceProfile());
        }
        // 使用流操作将物理槽位请求集合转换为一个Map，其中键为SlotRequestId，值为PhysicalSlotRequest
        Map<SlotRequestId, PhysicalSlotRequest> physicalSlotRequestsById =
                physicalSlotRequests.stream()
                        .collect(
                                Collectors.toMap(
                                        PhysicalSlotRequest::getSlotRequestId,
                                        Function.identity()));
        // 尝试从可用槽位中分配物理槽位，并返回一个Map，键为SlotRequestId，值为Optional<PhysicalSlot>
        Map<SlotRequestId, Optional<PhysicalSlot>> availablePhysicalSlots =
                tryAllocateFromAvailable(physicalSlotRequestsById.values());

        // 使用流操作将上一步的结果转换为CompletableFuture的Map，其中键为SlotRequestId，值为PhysicalSlotRequest.Result的CompletableFuture
        return availablePhysicalSlots.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> {
                                    /** 获取可用的PhysicalSlot*/
                                    Optional<PhysicalSlot> availablePhysicalSlot = entry.getValue();
                                    /**  获取SlotRequestId */
                                    SlotRequestId slotRequestId = entry.getKey();
                                    /** 获取物理请求PhysicalSlotRequest */
                                    PhysicalSlotRequest physicalSlotRequest =
                                            physicalSlotRequestsById.get(slotRequestId);
                                    /** 通过PhysicalSlotRequest 获取 SlotProfile*/
                                    SlotProfile slotProfile = physicalSlotRequest.getSlotProfile();
                                    /** 通过slotProfile获取ResourceProfile*/
                                    ResourceProfile resourceProfile =
                                            slotProfile.getPhysicalSlotResourceProfile();
                                    // 创建一个CompletableFuture，根据是否有可用的物理槽位来决定是立即完成还是异步执行
                                    CompletableFuture<PhysicalSlot> slotFuture =
                                            availablePhysicalSlot
                                                    .map(CompletableFuture::completedFuture)
                                                    .orElseGet(
                                                            () ->
                                                                    /** todo 如果槽位不可用，则请求一个新的槽位   */
                                                                    requestNewSlot(
                                                                            slotRequestId,
                                                                            resourceProfile,
                                                                            slotProfile
                                                                                    .getPreferredAllocations(),
                                                                            physicalSlotRequest
                                                                                    .willSlotBeOccupiedIndefinitely()));
                                    // 返回一个新的CompletableFuture，当slotFuture完成时，将结果包装为PhysicalSlotRequest.Result
                                    return slotFuture.thenApply(
                                            physicalSlot ->
                                                    /**
                                                     * 在这里处理PhysicalSlot到PhysicalSlotRequest.Result的转换
                                                     * 例如: return new PhysicalSlotRequest.Result(physicalSlot);
                                                     * 或者，如果您需要等待额外的异步操作来填充Result，可以链式调用更多的.thenApplyAsync或.thenComposeAsync
                                                     */
                                                    new PhysicalSlotRequest.Result(
                                                            slotRequestId, physicalSlot));
                                }));
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 从可用的物理槽位中尝试分配请求的物理槽位
    */
    private Map<SlotRequestId, Optional<PhysicalSlot>> tryAllocateFromAvailable(
            Collection<PhysicalSlotRequest> slotRequests) {
        // 获取空闲槽位信息跟踪器
        FreeSlotInfoTracker freeSlotInfoTracker = slotPool.getFreeSlotInfoTracker();
        // 用于存储分配结果的映射，键为槽位请求ID，值为分配结果的Optional对象
        Map<SlotRequestId, Optional<PhysicalSlot>> allocateResult = new HashMap<>();
        // 遍历每个槽位请求
        for (PhysicalSlotRequest request : slotRequests) {
            // 使用槽位选择策略选择最适合当前槽位请求的空闲槽位
            Optional<SlotSelectionStrategy.SlotInfoAndLocality> slot =
                    slotSelectionStrategy.selectBestSlotForProfile(
                            freeSlotInfoTracker, request.getSlotProfile());

            // 将分配结果放入映射中，如果找到合适的槽位，则预留该槽位并尝试分配
            allocateResult.put(
                    request.getSlotRequestId(),
                    slot.flatMap(
                            slotInfoAndLocality -> {
                                // 预留槽位
                                freeSlotInfoTracker.reserveSlot(
                                        slotInfoAndLocality.getSlotInfo().getAllocationId());
                                // 尝试从槽位池中分配该槽位
                                return slotPool.allocateAvailableSlot(
                                        request.getSlotRequestId(),
                                        slotInfoAndLocality.getSlotInfo().getAllocationId(),
                                        request.getSlotProfile().getPhysicalSlotResourceProfile());
                            }));
        }
        return allocateResult;
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 请求新的Slot 返回物理PhysicalSlot
     * SlotRequestId slotRequestId: 用于唯一标识请求的ID。
     * ResourceProfile resourceProfile: 包含所需资源（如CPU、内存等）配置的对象。
     * Collection<AllocationID> preferredAllocations: 包含首选分配ID的集合，用于在资源池中选择特定的资源分配。
     * boolean willSlotBeOccupiedIndefinitely: 表示这个slot是否会被无限期地占用。
    */
    private CompletableFuture<PhysicalSlot> requestNewSlot(
            SlotRequestId slotRequestId,
            ResourceProfile resourceProfile,
            Collection<AllocationID> preferredAllocations,
            boolean willSlotBeOccupiedIndefinitely) {
        /** SlotPool 请求申请新了Slot*/
        if (willSlotBeOccupiedIndefinitely) {
            return slotPool.requestNewAllocatedSlot(
                    slotRequestId, resourceProfile, preferredAllocations, null);
        } else {
            /**
             * 请求一个仅在一段时间内被占用的新slot，例如，用于批处理任务。
             */
            return slotPool.requestNewAllocatedBatchSlot(
                    slotRequestId, resourceProfile, preferredAllocations);
        }
    }

    @Override
    public void cancelSlotRequest(SlotRequestId slotRequestId, Throwable cause) {
        slotPool.releaseSlot(slotRequestId, cause);
    }
}
