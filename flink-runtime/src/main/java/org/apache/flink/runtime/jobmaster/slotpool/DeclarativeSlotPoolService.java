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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.AllocatedSlotInfo;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** {@link SlotPoolService} implementation for the {@link DeclarativeSlotPool}. */
public class DeclarativeSlotPoolService implements SlotPoolService {

    private final JobID jobId;

    private final Time rpcTimeout;

    private final DeclarativeSlotPool declarativeSlotPool;

    private final Clock clock;

    private final Set<ResourceID> registeredTaskManagers;

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private DeclareResourceRequirementServiceConnectionManager
            resourceRequirementServiceConnectionManager =
                    NoOpDeclareResourceRequirementServiceConnectionManager.INSTANCE;

    @Nullable private JobMasterId jobMasterId;

    @Nullable private String jobManagerAddress;

    private State state = State.CREATED;

    public DeclarativeSlotPoolService(
            JobID jobId,
            DeclarativeSlotPoolFactory declarativeSlotPoolFactory,
            Clock clock,
            Time idleSlotTimeout,
            Time rpcTimeout) {
        this.jobId = jobId;
        this.clock = clock;
        this.rpcTimeout = rpcTimeout;
        this.registeredTaskManagers = new HashSet<>();

        this.declarativeSlotPool =
                declarativeSlotPoolFactory.create(
                        jobId, this::declareResourceRequirements, idleSlotTimeout, rpcTimeout);
    }

    protected DeclarativeSlotPool getDeclarativeSlotPool() {
        return declarativeSlotPool;
    }

    protected long getRelativeTimeMillis() {
        return clock.relativeTimeMillis();
    }

    @Override
    public <T> Optional<T> castInto(Class<T> clazz) {
        if (clazz.isAssignableFrom(declarativeSlotPool.getClass())) {
            return Optional.of(clazz.cast(declarativeSlotPool));
        }

        return Optional.empty();
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *
    */
    @Override
    public final void start(
            JobMasterId jobMasterId, String address, ComponentMainThreadExecutor mainThreadExecutor)
            throws Exception {
        /**
         * 服务的状态为State.CREATED（即已创建但尚未启动）时，才能调用start方法。
         * 如果服务已经启动过，则抛出异常，因为DeclarativeSlotPoolService只能启动一次。
         */
        Preconditions.checkState(
                state == State.CREATED, "The DeclarativeSlotPoolService can only be started once.");
        /** 检查jobMasterId是否为null，如果为null，则抛出NullPointerException。 */
        this.jobMasterId = Preconditions.checkNotNull(jobMasterId);
        /** 检查address是否为null，如果为null，则抛出NullPointerException */
        this.jobManagerAddress = Preconditions.checkNotNull(address);
        /**
         * 创建DeclareResourceRequirementServiceConnectionManager对象
         * 用于声明资源需求的ServiceConnectionManager。
         */
        this.resourceRequirementServiceConnectionManager =
                DefaultDeclareResourceRequirementServiceConnectionManager.create(
                        mainThreadExecutor);

        onStart(mainThreadExecutor);
        /** 将服务的状态更新为State.STARTED，表示服务已经成功启动。 */
        state = State.STARTED;
    }

    /**
     * This method is called when the slot pool service is started. It can be overridden by
     * subclasses.
     *
     * @param componentMainThreadExecutor componentMainThreadExecutor used by this slot pool service
     */
    protected void onStart(ComponentMainThreadExecutor componentMainThreadExecutor) {}

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 检查某个条件是否满足。在这里，它检查 state 变量是否等于 State.STARTED。如果条件不满足（即服务没有启动），
     * 它将抛出一个 IllegalStateException，并带有指定的错误消息。
    */
    protected void assertHasBeenStarted() {
        Preconditions.checkState(
                state == State.STARTED, "The DeclarativeSlotPoolService has to be started.");
    }

    @Override
    public final void close() {
        if (state != State.CLOSED) {

            onClose();

            resourceRequirementServiceConnectionManager.close();
            resourceRequirementServiceConnectionManager =
                    NoOpDeclareResourceRequirementServiceConnectionManager.INSTANCE;

            releaseAllTaskManagers(
                    new FlinkException("The DeclarativeSlotPoolService is being closed."));

            state = State.CLOSED;
        }
    }

    /**
     * This method is called when the slot pool service is closed. It can be overridden by
     * subclasses.
     */
    protected void onClose() {}

    @Override
    public Collection<SlotOffer> offerSlots(
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            Collection<SlotOffer> offers) {
        assertHasBeenStarted();

        if (!isTaskManagerRegistered(taskManagerLocation.getResourceID())) {
            log.debug(
                    "Ignoring offered slots from unknown task manager {}.",
                    taskManagerLocation.getResourceID());
            return Collections.emptyList();
        }

        return declarativeSlotPool.offerSlots(
                offers, taskManagerLocation, taskManagerGateway, clock.relativeTimeMillis());
    }

    boolean isTaskManagerRegistered(ResourceID taskManagerId) {
        return registeredTaskManagers.contains(taskManagerId);
    }

    @Override
    public Optional<ResourceID> failAllocation(
            @Nullable ResourceID taskManagerId, AllocationID allocationId, Exception cause) {
        assertHasBeenStarted();
        Preconditions.checkNotNull(allocationId);
        Preconditions.checkNotNull(
                taskManagerId,
                "This slot pool only supports failAllocation calls coming from the TaskExecutor.");

        final ResourceCounter previouslyFulfilledRequirements =
                declarativeSlotPool.releaseSlot(allocationId, cause);

        onFailAllocation(previouslyFulfilledRequirements);

        if (declarativeSlotPool.containsSlots(taskManagerId)) {
            return Optional.empty();
        } else {
            return Optional.of(taskManagerId);
        }
    }

    /**
     * This method is called when an allocation fails. It can be overridden by subclasses.
     *
     * @param previouslyFulfilledRequirements previouslyFulfilledRequirements by the failed
     *     allocation
     */
    protected void onFailAllocation(ResourceCounter previouslyFulfilledRequirements) {}

    @Override
    public boolean registerTaskManager(ResourceID taskManagerId) {
        assertHasBeenStarted();

        log.debug("Register new TaskExecutor {}.", taskManagerId);
        return registeredTaskManagers.add(taskManagerId);
    }

    @Override
    public boolean releaseTaskManager(ResourceID taskManagerId, Exception cause) {
        assertHasBeenStarted();

        if (registeredTaskManagers.remove(taskManagerId)) {
            internalReleaseTaskManager(taskManagerId, cause);
            return true;
        }

        return false;
    }

    @Override
    public void releaseFreeSlotsOnTaskManager(ResourceID taskManagerId, Exception cause) {
        assertHasBeenStarted();
        if (isTaskManagerRegistered(taskManagerId)) {

            Collection<AllocationID> freeSlots =
                    declarativeSlotPool.getFreeSlotInfoTracker().getFreeSlotsInformation().stream()
                            .filter(
                                    slotInfo ->
                                            slotInfo.getTaskManagerLocation()
                                                    .getResourceID()
                                                    .equals(taskManagerId))
                            .map(SlotInfo::getAllocationId)
                            .collect(Collectors.toSet());

            for (AllocationID allocationId : freeSlots) {
                final ResourceCounter previouslyFulfilledRequirement =
                        declarativeSlotPool.releaseSlot(allocationId, cause);
                // release free slots, previously fulfilled requirement should be empty.
                Preconditions.checkState(
                        previouslyFulfilledRequirement.equals(ResourceCounter.empty()));
            }
        }
    }

    private void releaseAllTaskManagers(Exception cause) {
        for (ResourceID registeredTaskManager : registeredTaskManagers) {
            internalReleaseTaskManager(registeredTaskManager, cause);
        }

        registeredTaskManagers.clear();
    }

    private void internalReleaseTaskManager(ResourceID taskManagerId, Exception cause) {
        assertHasBeenStarted();

        final ResourceCounter previouslyFulfilledRequirement =
                declarativeSlotPool.releaseSlots(taskManagerId, cause);

        onReleaseTaskManager(previouslyFulfilledRequirement);
    }

    /**
     * This method is called when a TaskManager is released. It can be overridden by subclasses.
     *
     * @param previouslyFulfilledRequirement previouslyFulfilledRequirement by the released
     *     TaskManager
     */
    protected void onReleaseTaskManager(ResourceCounter previouslyFulfilledRequirement) {}

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *
    */
    @Override
    public void connectToResourceManager(ResourceManagerGateway resourceManagerGateway) {
        /**
         * 它检查 state 变量是否等于 State.STARTED。
         */
        assertHasBeenStarted();
        /** 连接到给定的服务。 */
        resourceRequirementServiceConnectionManager.connect(
                resourceRequirements ->
                        /**
                         * 动态代理向resourceManager发送declareRequiredResources
                         * 发送资源申请
                         */
                        resourceManagerGateway.declareRequiredResources(
                                jobMasterId, resourceRequirements, rpcTimeout));

        declareResourceRequirements(declarativeSlotPool.getResourceRequirements());
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *
    */
    private void declareResourceRequirements(Collection<ResourceRequirement> resourceRequirements) {
        /** 校验 DeclarativeSlotPoolService是否启动 */
        assertHasBeenStarted();
        /**
         * DeclareResourceRequirementServiceConnectionManager 用于声明资源需求的ServiceConnectionManager。
         * declareResourceRequirements申请资源
         */
        resourceRequirementServiceConnectionManager.declareResourceRequirements(
                ResourceRequirements.create(jobId, jobManagerAddress, resourceRequirements));
    }

    @Override
    public void disconnectResourceManager() {
        assertHasBeenStarted();

        resourceRequirementServiceConnectionManager.disconnect();
    }

    @Override
    public AllocatedSlotReport createAllocatedSlotReport(ResourceID taskManagerId) {
        assertHasBeenStarted();

        final Collection<AllocatedSlotInfo> allocatedSlotInfos = new ArrayList<>();

        for (SlotInfo slotInfo : declarativeSlotPool.getAllSlotsInformation()) {
            if (slotInfo.getTaskManagerLocation().getResourceID().equals(taskManagerId)) {
                allocatedSlotInfos.add(
                        new AllocatedSlotInfo(
                                slotInfo.getPhysicalSlotNumber(), slotInfo.getAllocationId()));
            }
        }
        return new AllocatedSlotReport(jobId, allocatedSlotInfos);
    }

    private enum State {
        CREATED,
        STARTED,
        CLOSED,
    }

    protected String getSlotServiceStatus() {
        return String.format(
                "Registered TMs: %d, registered slots: %d free slots: %d",
                registeredTaskManagers.size(),
                declarativeSlotPool.getAllSlotsInformation().size(),
                declarativeSlotPool.getFreeSlotInfoTracker().getAvailableSlots().size());
    }
}
