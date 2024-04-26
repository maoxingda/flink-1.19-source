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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** The Interface of a slot pool that manages slots. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 管理Slots的接口
 * 在JobMaster中会通过SlotPool组件管理JobManager中的Slot计算资源。
 * 每个JobMaster都会创建一个SlotPool实例。
 * SlotPool 管理Slot的接口
*/
public interface SlotPool extends AllocatedSlotActions, AutoCloseable {

    // ------------------------------------------------------------------------
    //  lifecycle
    // ------------------------------------------------------------------------
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 启动SlotPool,启动后JobManater会通过SlotPool向ResourceManager申请执行作业需要的资源
    */
    void start(
            JobMasterId jobMasterId,
            String newJobManagerAddress,
            ComponentMainThreadExecutor jmMainThreadScheduledExecutor)
            throws Exception;
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 关闭SlotPool
    */
    void close();

    // ------------------------------------------------------------------------
    //  resource manager connection
    // ------------------------------------------------------------------------

    /**
     * Connects the SlotPool to the given ResourceManager. After this method is called, the SlotPool
     * will be able to request resources from the given ResourceManager.
     *
     * @param resourceManagerGateway The RPC gateway for the resource manager.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * SlotPool与ResourceManager建立链接。
     * 创建链接后，SlotPool将能够从给定的ResourceManager请求资源。
    */
    void connectToResourceManager(ResourceManagerGateway resourceManagerGateway);

    /**
     * Disconnects the slot pool from its current Resource Manager. After this call, the pool will
     * not be able to request further slots from the Resource Manager, and all currently pending
     * requests to the resource manager will be canceled.
     *
     * <p>The slot pool will still be able to serve slots from its internal pool.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * SlotPool断开与ResourceManager链接，
     * 1.SlotPool将无法从资源管理器获取更多的资源
     * 2.所有当前挂起的对资源管理器的请求都将被取消。
    */
    void disconnectResourceManager();

    // ------------------------------------------------------------------------
    //  registering / un-registering TaskManagers and slots
    // ------------------------------------------------------------------------

    /**
     * Registers a TaskExecutor with the given {@link ResourceID} at {@link SlotPool}.
     *
     * @param resourceID identifying the TaskExecutor to register
     * @return true iff a new resource id was registered
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 注册/取消注册TaskManager和插槽
    */
    boolean registerTaskManager(ResourceID resourceID);

    /**
     * Releases a TaskExecutor with the given {@link ResourceID} from the {@link SlotPool}.
     *
     * @param resourceId identifying the TaskExecutor which shall be released from the SlotPool
     * @param cause for the releasing of the TaskManager
     * @return true iff a given registered resource id was removed
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 从SlotPool中根据给定的ResourceId 释放TaskManager
    */
    boolean releaseTaskManager(final ResourceID resourceId, final Exception cause);

    /**
     * Offers multiple slots to the {@link SlotPool}. The slot offerings can be individually
     * accepted or rejected by returning the collection of accepted slot offers.
     *
     * @param taskManagerLocation from which the slot offers originate
     * @param taskManagerGateway to talk to the slot offerer
     * @param offers slot offers which are offered to the {@link SlotPool}
     * @return A collection of accepted slot offers. The remaining slot offers are implicitly
     *     rejected.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 向ResourceManager成功申请资源后，ResourceManager会调用TaskExecutor的RPC 网关通信TaskExecutor提供申请到的Slot资源
     * TaskExecutor接收到来自ResourceManager的slot分配请求后，调用offerSlots，向JobMaster提供已分配好的Slot资源信息
     * offerSlot内部会将消息转换为AllocatedSlot对向，存储在allocatedSlots数据集中，在Job启动Task时，会从Slot数据集中获取slot信息
    */
    Collection<SlotOffer> offerSlots(
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            Collection<SlotOffer> offers);

    // ------------------------------------------------------------------------
    //  allocating and disposing slots
    // ------------------------------------------------------------------------

    /**
     * Returns all free slot tracker.
     *
     * @return all free slot tracker
     */
    FreeSlotInfoTracker getFreeSlotInfoTracker();

    /**
     * Returns a list of {@link SlotInfo} objects about all slots that are currently allocated in
     * the slot pool.
     *
     * @return a list of {@link SlotInfo} objects about all slots that are currently allocated in
     *     the slot pool.
     */
    Collection<SlotInfo> getAllocatedSlotsInformation();

    /**
     * Allocates the available slot with the given allocation id under the given request id for the
     * given requirement profile. The slot must be able to fulfill the requirement profile,
     * otherwise an {@link IllegalStateException} will be thrown.
     *
     * @param slotRequestId identifying the requested slot
     * @param allocationID the allocation id of the requested available slot
     * @param requirementProfile resource profile of the requirement for which to allocate the slot
     * @return the previously available slot with the given allocation id, if a slot with this
     *     allocation id exists
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 基于需求配置请求申请Slot
    */
    Optional<PhysicalSlot> allocateAvailableSlot(
            SlotRequestId slotRequestId,
            AllocationID allocationID,
            ResourceProfile requirementProfile);

    /**
     * Request the allocation of a new slot from the resource manager. This method will not return a
     * slot from the already available slots from the pool, but instead will add a new slot to that
     * pool that is immediately allocated and returned.
     *
     * @param slotRequestId identifying the requested slot
     * @param resourceProfile resource profile that specifies the resource requirements for the
     *     requested slot
     * @param timeout timeout for the allocation procedure
     * @return a newly allocated slot that was previously not available.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *向资源管理器请求分配新的Slot。
     * 此方法不会从池中已可用的Slot中返回Slot，而是将立即分配并返回的新插槽添加到池中。
     */
    default CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
            SlotRequestId slotRequestId, ResourceProfile resourceProfile, @Nullable Time timeout) {
        return requestNewAllocatedSlot(
                slotRequestId, resourceProfile, Collections.emptyList(), timeout);
    }

    /**
     * Request the allocation of a new slot from the resource manager. This method will not return a
     * slot from the already available slots from the pool, but instead will add a new slot to that
     * pool that is immediately allocated and returned.
     *
     * @param slotRequestId identifying the requested slot
     * @param resourceProfile resource profile that specifies the resource requirements for the
     *     requested slot
     * @param preferredAllocations preferred allocations for the new allocated slot
     * @param timeout timeout for the allocation procedure
     * @return a newly allocated slot that was previously not available.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 向资源管理器请求分配新的插槽。此方法不会从池中已可用的插槽中返回插槽，
     * 而是将请求并返回的新插槽添加到池中。
    */
    CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
            SlotRequestId slotRequestId,
            ResourceProfile resourceProfile,
            Collection<AllocationID> preferredAllocations,
            @Nullable Time timeout);

    /**
     * Requests the allocation of a new batch slot from the resource manager. Unlike the normal
     * slot, a batch slot will only time out if the slot pool does not contain a suitable slot.
     * Moreover, it won't react to failure signals from the resource manager.
     *
     * @param slotRequestId identifying the requested slot
     * @param resourceProfile resource profile that specifies the resource requirements for the
     *     requested batch slot
     * @return a future which is completed with newly allocated batch slot
     */
    default CompletableFuture<PhysicalSlot> requestNewAllocatedBatchSlot(
            SlotRequestId slotRequestId, ResourceProfile resourceProfile) {
        return requestNewAllocatedBatchSlot(
                slotRequestId, resourceProfile, Collections.emptyList());
    }

    CompletableFuture<PhysicalSlot> requestNewAllocatedBatchSlot(
            SlotRequestId slotRequestId,
            ResourceProfile resourceProfile,
            Collection<AllocationID> preferredAllocations);

    /**
     * Disables batch slot request timeout check. Invoked when someone else wants to take over the
     * timeout check responsibility.
     */
    void disableBatchSlotRequestTimeoutCheck();

    /**
     * Create report about the allocated slots belonging to the specified task manager.
     *
     * @param taskManagerId identifies the task manager
     * @return the allocated slots on the task manager
     */
    AllocatedSlotReport createAllocatedSlotReport(ResourceID taskManagerId);

    /**
     * Sets whether the underlying job is currently restarting or not.
     *
     * @param isJobRestarting whether the job is restarting or not
     */
    void setIsJobRestarting(boolean isJobRestarting);
}
