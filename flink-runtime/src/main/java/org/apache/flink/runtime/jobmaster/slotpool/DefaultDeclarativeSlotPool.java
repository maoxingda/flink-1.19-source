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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.slots.DefaultRequirementMatcher;
import org.apache.flink.runtime.slots.RequirementMatcher;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Default {@link DeclarativeSlotPool} implementation.
 *
 * <p>The implementation collects the current resource requirements and declares them at the
 * ResourceManager. Whenever new slots are offered, the slot pool compares the offered slots to the
 * set of available and required resources and only accepts those slots which are required.
 *
 * <p>Slots which are released won't be returned directly to their owners. Instead, the slot pool
 * implementation will only return them after the idleSlotTimeout has been exceeded by a free slot.
 *
 * <p>The slot pool will call {@link #newSlotsListener} whenever newly offered slots are accepted or
 * if an allocated slot should become free after it is being {@link #freeReservedSlot freed}.
 *
 * <p>This class expects 1 of 2 access patterns for changing requirements, which should not be
 * mixed:
 *
 * <p>1) the legacy approach (used by the DefaultScheduler) tightly couples requirements to reserved
 * slots. When a slot is requested it increases the requirements, when the slot is freed they are
 * decreased again. In the general case what happens is that requirements are increased, a free slot
 * is reserved, (the slot is used for a bit,) the slot is freed, the requirements are reduced. To
 * this end {@link #freeReservedSlot}, {@link #releaseSlot} and {@link #releaseSlots} return a
 * {@link ResourceCounter} describing which requirement the slot(s) were fulfilling, with the
 * expectation that the scheduler will subsequently decrease the requirements by that amount.
 *
 * <p>2) The declarative approach (used by the AdaptiveScheduler) in contrast derives requirements
 * exclusively based on what a given job currently requires. It may repeatedly reserve/free slots
 * without any modifications to the requirements.
 */
public class DefaultDeclarativeSlotPool implements DeclarativeSlotPool {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final Consumer<? super Collection<ResourceRequirement>> notifyNewResourceRequirements;

    private final Time idleSlotTimeout;
    private final Time rpcTimeout;

    private final JobID jobId;
    protected final AllocatedSlotPool slotPool;

    private final Map<AllocationID, ResourceProfile> slotToRequirementProfileMappings;

    private ResourceCounter totalResourceRequirements;//总资源需求

    private ResourceCounter fulfilledResourceRequirements;//满足资源的需求

    private NewSlotsListener newSlotsListener = NoOpNewSlotsListener.INSTANCE;

    private final RequirementMatcher requirementMatcher = new DefaultRequirementMatcher();

    public DefaultDeclarativeSlotPool(
            JobID jobId,
            AllocatedSlotPool slotPool,
            Consumer<? super Collection<ResourceRequirement>> notifyNewResourceRequirements,
            Time idleSlotTimeout,
            Time rpcTimeout) {

        this.jobId = jobId;
        this.slotPool = slotPool;
        this.notifyNewResourceRequirements = notifyNewResourceRequirements;
        this.idleSlotTimeout = idleSlotTimeout;
        this.rpcTimeout = rpcTimeout;
        this.totalResourceRequirements = ResourceCounter.empty();
        this.fulfilledResourceRequirements = ResourceCounter.empty();
        this.slotToRequirementProfileMappings = new HashMap<>();
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 该方法用于增加某个任务或作业的资源需求
    */
    @Override
    public void increaseResourceRequirementsBy(ResourceCounter increment) {
        if (increment.isEmpty()) {
            return;
        }
        totalResourceRequirements = totalResourceRequirements.add(increment);
        /** 流转到内部方法请求资源 */
        declareResourceRequirements();
    }

    @Override
    public void decreaseResourceRequirementsBy(ResourceCounter decrement) {
        if (decrement.isEmpty()) {
            return;
        }
        totalResourceRequirements = totalResourceRequirements.subtract(decrement);

        declareResourceRequirements();
    }

    @Override
    public void setResourceRequirements(ResourceCounter resourceRequirements) {
        totalResourceRequirements = resourceRequirements;

        declareResourceRequirements();
    }

    private void declareResourceRequirements() {
        final Collection<ResourceRequirement> resourceRequirements = getResourceRequirements();

        log.debug(
                "Declare new resource requirements for job {}.{}\trequired resources: {}{}\tacquired resources: {}",
                jobId,
                System.lineSeparator(),
                resourceRequirements,
                System.lineSeparator(),
                fulfilledResourceRequirements);
        /** notifyNewResourceRequirements,DeclarativeSlotPoolService */
        notifyNewResourceRequirements.accept(resourceRequirements);
    }

    @Override
    public Collection<ResourceRequirement> getResourceRequirements() {
        final Collection<ResourceRequirement> currentResourceRequirements = new ArrayList<>();

        for (Map.Entry<ResourceProfile, Integer> resourceRequirement :
                totalResourceRequirements.getResourcesWithCount()) {
            currentResourceRequirements.add(
                    ResourceRequirement.create(
                            resourceRequirement.getKey(), resourceRequirement.getValue()));
        }

        return currentResourceRequirements;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 处理来自TaskExecutor的SlotOffer集合
    */
    @Override
    public Collection<SlotOffer> offerSlots(
            Collection<? extends SlotOffer> offers,
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            long currentTime) {
        // 使用日志记录器记录接收到SlotOffer的数量和来源的TaskExecutor位置
        log.debug("Received {} slot offers from TaskExecutor {}.", offers, taskManagerLocation);
        // 调用internalOfferSlots方法进行内部处理
        return internalOfferSlots(
                offers,
                taskManagerLocation,
                taskManagerGateway,
                currentTime,
                this::matchWithOutstandingRequirement);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 内部处理SlotOffer的集合
    */
    private Collection<SlotOffer> internalOfferSlots(
            Collection<? extends SlotOffer> offers,
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            long currentTime,
            Function<ResourceProfile, Optional<ResourceProfile>> matchingCondition) {
        // 存储已接受的SlotOffer的集合
        final Collection<SlotOffer> acceptedSlotOffers = new ArrayList<>();
        // 存储已接受并分配的Slot的集合
        final Collection<AllocatedSlot> acceptedSlots = new ArrayList<>();
        // 遍历待处理的SlotOffer集合
        for (SlotOffer offer : offers) {
            // 检查该SlotOffer是否已经在slotPool中存在（即是否已经被接受）
            if (slotPool.containsSlot(offer.getAllocationId())) {
                // we have already accepted this offer
                // 如果已经存在，则直接添加到已接受的SlotOffer集合中
                acceptedSlotOffers.add(offer);
            } else {
                // 尝试将SlotOffer与待处理的需求进行匹配
                Optional<AllocatedSlot> acceptedSlot =
                        matchOfferWithOutstandingRequirements(
                                offer, taskManagerLocation, taskManagerGateway, matchingCondition);            // 如果匹配成功

                // 如果匹配成功
                if (acceptedSlot.isPresent()) {
                    // 将SlotOffer添加到已接受的SlotOffer集合中
                    acceptedSlotOffers.add(offer);
                    // 将匹配的AllocatedSlot添加到已接受的Slot集合中
                    acceptedSlots.add(acceptedSlot.get());
                } else {
                    // 如果匹配失败，则记录日志
                    log.debug(
                            "Could not match offer {} to any outstanding requirement.",
                            offer.getAllocationId());
                }
            }
        }
        // 将已接受的AllocatedSlot添加到slotPool中
        slotPool.addSlots(acceptedSlots, currentTime);
        // 如果有已接受的Slot
        if (!acceptedSlots.isEmpty()) {
            // 记录日志，显示新获取的资源
            log.debug(
                    "Acquired new resources; new total acquired resources: {}",
                    fulfilledResourceRequirements);
            // 通知监听器有新Slot可用
            newSlotsListener.notifyNewSlotsAreAvailable(acceptedSlots);
        }
         // 返回已接受的SlotOffer集合
        return acceptedSlotOffers;
    }

    @Override
    public Collection<SlotOffer> registerSlots(
            Collection<? extends SlotOffer> slots,
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            long currentTime) {
        // This method exists to allow slots to be re-offered by recovered TMs while the job is in a
        // restarting state (where it usually hasn't set any requirements).
        // For this to work with the book-keeping of this class these slots are "matched" against
        // the ResourceProfile.ANY, which in practice isn't used.
        // If such a slot is then later reserved the mapping is updated accordingly.
        // While this approach does have the downside of somewhat hiding this special case, it
        // does allow the slot timeouts or releases to work as if the case didn't exist at all.

        log.debug("Register slots {} from TaskManager {}.", slots, taskManagerLocation);
        internalOfferSlots(
                slots,
                taskManagerLocation,
                taskManagerGateway,
                currentTime,
                this::matchWithOutstandingRequirementOrWildcard);
        return new ArrayList<>(slots);
    }

    private Optional<ResourceProfile> matchWithOutstandingRequirementOrWildcard(
            ResourceProfile resourceProfile) {
        final Optional<ResourceProfile> match = matchWithOutstandingRequirement(resourceProfile);

        if (match.isPresent()) {
            return match;
        } else {
            // use ANY as wildcard as there is no practical purpose for a slot with 0 resources
            return Optional.of(ResourceProfile.ANY);
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 根据提供的SlotOffer和条件来匹配未满足的资源需求。
     * @param slotOffer 提供的SlotOffer
     * @param taskManagerLocation TaskManager的位置信息
     * @param taskManagerGateway TaskManager的网关，用于与TaskManager进行通信
     * @param matchingCondition 匹配条件，一个函数，接受ResourceProfile并返回与之匹配的Optional<ResourceProfile>
     * @return 如果匹配成功，则返回包含AllocatedSlot的Optional；否则返回空的Optional
    */
    private Optional<AllocatedSlot> matchOfferWithOutstandingRequirements(
            SlotOffer slotOffer,
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            Function<ResourceProfile, Optional<ResourceProfile>> matchingCondition) {
        // 应用匹配条件到slotOffer的资源配置
        final Optional<ResourceProfile> match =
                matchingCondition.apply(slotOffer.getResourceProfile());
        // 如果匹配成功
        if (match.isPresent()) {
            // 获取匹配到的资源需求配置
            final ResourceProfile matchedRequirement = match.get();
            // 记录日志，表示slotOffer与需求匹配成功
            log.debug(
                    "Matched slot offer {} to requirement {}.",
                    slotOffer.getAllocationId(),
                    matchedRequirement);

            increaseAvailableResources(ResourceCounter.withResource(matchedRequirement, 1));
            // 根据slotOffer创建AllocatedSlot
            final AllocatedSlot allocatedSlot =
                    createAllocatedSlot(slotOffer, taskManagerLocation, taskManagerGateway);

            // store the ResourceProfile against which the given slot has matched for future
            // book-keeping
            // 存储与给定slot匹配的ResourceProfile，存储给定插槽为将来匹配的ResourceProfile
            slotToRequirementProfileMappings.put(
                    allocatedSlot.getAllocationId(), matchedRequirement);
            // 返回包含AllocatedSlot的Optional
            return Optional.of(allocatedSlot);
        }
        // 如果没有匹配成功，则返回空的Optional
        return Optional.empty();
    }

    private Optional<ResourceProfile> matchWithOutstandingRequirement(
            ResourceProfile resourceProfile) {
        return requirementMatcher.match(
                resourceProfile,
                totalResourceRequirements,
                fulfilledResourceRequirements::getResourceCount);
    }

    @VisibleForTesting
    ResourceCounter calculateUnfulfilledResources() {
        return totalResourceRequirements.subtract(fulfilledResourceRequirements);
    }

    private AllocatedSlot createAllocatedSlot(
            SlotOffer slotOffer,
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway) {
        return new AllocatedSlot(
                slotOffer.getAllocationId(),
                taskManagerLocation,
                slotOffer.getSlotIndex(),
                slotOffer.getResourceProfile(),
                taskManagerGateway);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将ResourceCounter添加到 Map<ResourceProfile, Integer> resources
    */
    private void increaseAvailableResources(ResourceCounter acceptedResources) {
        fulfilledResourceRequirements = fulfilledResourceRequirements.add(acceptedResources);
    }

    @Nonnull
    private ResourceProfile getMatchingResourceProfile(AllocationID allocationId) {
        return Preconditions.checkNotNull(
                slotToRequirementProfileMappings.get(allocationId),
                "No matching resource profile found for %s",
                allocationId);
    }

    @Override
    public PhysicalSlot reserveFreeSlot(
            AllocationID allocationId, ResourceProfile requiredSlotProfile) {
        final AllocatedSlot allocatedSlot = slotPool.reserveFreeSlot(allocationId);

        Preconditions.checkState(
                allocatedSlot.getResourceProfile().isMatching(requiredSlotProfile),
                "Slot {} cannot fulfill the given requirement. SlotProfile={} Requirement={}",
                allocationId,
                allocatedSlot.getResourceProfile(),
                requiredSlotProfile);

        ResourceProfile previouslyMatchedResourceProfile =
                Preconditions.checkNotNull(slotToRequirementProfileMappings.get(allocationId));

        if (!previouslyMatchedResourceProfile.equals(requiredSlotProfile)) {
            // slots can be reserved for a requirement that is not in line with the mapping we
            // computed when the slot was offered, so we have to update the mapping
            updateSlotToRequirementProfileMapping(allocationId, requiredSlotProfile);
            if (previouslyMatchedResourceProfile == ResourceProfile.ANY) {
                log.debug(
                        "Re-matched slot offer {} to requirement {}.",
                        allocationId,
                        requiredSlotProfile);
            } else {
                // adjust the requirements accordingly to ensure we still request enough slots to
                // be able to fulfill the total requirements
                // If the previous profile was ANY, then the slot was accepted without
                // being matched against a resource requirement; thus no update is needed.

                log.debug(
                        "Adjusting requirements because a slot was reserved for a different requirement than initially assumed. Slot={} assumedRequirement={} actualRequirement={}",
                        allocationId,
                        previouslyMatchedResourceProfile,
                        requiredSlotProfile);
                adjustRequirements(previouslyMatchedResourceProfile, requiredSlotProfile);
            }
        }

        return allocatedSlot;
    }

    @Override
    public ResourceCounter freeReservedSlot(
            AllocationID allocationId, @Nullable Throwable cause, long currentTime) {
        log.debug("Free reserved slot {}.", allocationId);

        final Optional<AllocatedSlot> freedSlot =
                slotPool.freeReservedSlot(allocationId, currentTime);

        Optional<ResourceCounter> previouslyFulfilledRequirement =
                freedSlot.map(Collections::singleton).map(this::getFulfilledRequirements);

        freedSlot.ifPresent(
                allocatedSlot -> {
                    releasePayload(Collections.singleton(allocatedSlot), cause);
                    newSlotsListener.notifyNewSlotsAreAvailable(
                            Collections.singletonList(allocatedSlot));
                });

        return previouslyFulfilledRequirement.orElseGet(ResourceCounter::empty);
    }

    private void updateSlotToRequirementProfileMapping(
            AllocationID allocationId, ResourceProfile matchedResourceProfile) {
        final ResourceProfile oldResourceProfile =
                Preconditions.checkNotNull(
                        slotToRequirementProfileMappings.put(allocationId, matchedResourceProfile),
                        "Expected slot profile matching to be non-empty.");

        fulfilledResourceRequirements =
                fulfilledResourceRequirements.add(matchedResourceProfile, 1);
        fulfilledResourceRequirements =
                fulfilledResourceRequirements.subtract(oldResourceProfile, 1);
    }

    private void adjustRequirements(
            ResourceProfile oldResourceProfile, ResourceProfile newResourceProfile) {
        // slots can be reserved for a requirement that is not in line with the mapping we computed
        // when the slot was
        // offered, so we have to adjust the requirements accordingly to ensure we still request
        // enough slots to
        // be able to fulfill the total requirements
        decreaseResourceRequirementsBy(ResourceCounter.withResource(newResourceProfile, 1));
        increaseResourceRequirementsBy(ResourceCounter.withResource(oldResourceProfile, 1));
    }

    @Override
    public void registerNewSlotsListener(NewSlotsListener newSlotsListener) {
        Preconditions.checkState(
                this.newSlotsListener == NoOpNewSlotsListener.INSTANCE,
                "DefaultDeclarativeSlotPool only supports a single slot listener.");
        this.newSlotsListener = newSlotsListener;
    }

    @Override
    public ResourceCounter releaseSlots(ResourceID owner, Exception cause) {
        final AllocatedSlotPool.AllocatedSlotsAndReservationStatus removedSlots =
                slotPool.removeSlots(owner);

        final Collection<AllocatedSlot> slotsToFree = new ArrayList<>();
        for (AllocatedSlot removedSlot : removedSlots.getAllocatedSlots()) {
            if (!removedSlots.wasFree(removedSlot.getAllocationId())) {
                slotsToFree.add(removedSlot);
            }
        }

        return freeAndReleaseSlots(slotsToFree, removedSlots.getAllocatedSlots(), cause);
    }

    @Override
    public ResourceCounter releaseSlot(AllocationID allocationId, Exception cause) {
        final boolean wasSlotFree = slotPool.containsFreeSlot(allocationId);
        final Optional<AllocatedSlot> removedSlot = slotPool.removeSlot(allocationId);

        if (removedSlot.isPresent()) {
            final AllocatedSlot slot = removedSlot.get();

            final Collection<AllocatedSlot> slotAsCollection = Collections.singleton(slot);
            return freeAndReleaseSlots(
                    wasSlotFree ? Collections.emptySet() : slotAsCollection,
                    slotAsCollection,
                    cause);
        } else {
            return ResourceCounter.empty();
        }
    }

    private ResourceCounter freeAndReleaseSlots(
            Collection<AllocatedSlot> currentlyReservedSlots,
            Collection<AllocatedSlot> slots,
            Exception cause) {

        ResourceCounter previouslyFulfilledRequirements =
                getFulfilledRequirements(currentlyReservedSlots);

        releasePayload(currentlyReservedSlots, cause);
        releaseSlots(slots, cause);

        return previouslyFulfilledRequirements;
    }

    private void releasePayload(Iterable<? extends AllocatedSlot> allocatedSlots, Throwable cause) {
        for (AllocatedSlot allocatedSlot : allocatedSlots) {
            allocatedSlot.releasePayload(cause);
        }
    }

    @Override
    public void releaseIdleSlots(long currentTimeMillis) {
        final Collection<AllocatedSlotPool.FreeSlotInfo> freeSlotsInformation =
                slotPool.getFreeSlotInfoTracker().getFreeSlotsWithIdleSinceInformation();

        ResourceCounter excessResources =
                fulfilledResourceRequirements.subtract(totalResourceRequirements);

        final Iterator<AllocatedSlotPool.FreeSlotInfo> freeSlotIterator =
                freeSlotsInformation.iterator();

        final Collection<AllocatedSlot> slotsToReturnToOwner = new ArrayList<>();

        while (!excessResources.isEmpty() && freeSlotIterator.hasNext()) {
            final AllocatedSlotPool.FreeSlotInfo idleSlot = freeSlotIterator.next();

            if (currentTimeMillis >= idleSlot.getFreeSince() + idleSlotTimeout.toMilliseconds()) {
                final ResourceProfile matchingProfile =
                        getMatchingResourceProfile(idleSlot.getAllocationId());

                if (excessResources.containsResource(matchingProfile)) {
                    excessResources = excessResources.subtract(matchingProfile, 1);
                    final Optional<AllocatedSlot> removedSlot =
                            slotPool.removeSlot(idleSlot.getAllocationId());

                    final AllocatedSlot allocatedSlot =
                            removedSlot.orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    String.format(
                                                            "Could not find slot for allocation id %s.",
                                                            idleSlot.getAllocationId())));
                    slotsToReturnToOwner.add(allocatedSlot);
                }
            }
        }

        releaseSlots(
                slotsToReturnToOwner, new FlinkException("Returning idle slots to their owners."));
        log.debug(
                "Idle slots have been returned; new total acquired resources: {}",
                fulfilledResourceRequirements);
    }

    private void releaseSlots(Iterable<AllocatedSlot> slotsToReturnToOwner, Throwable cause) {
        for (AllocatedSlot slotToReturn : slotsToReturnToOwner) {
            Preconditions.checkState(!slotToReturn.isUsed(), "Free slot must not be used.");

            if (log.isDebugEnabled()) {
                log.info("Releasing slot [{}].", slotToReturn.getAllocationId(), cause);
            } else {
                log.info("Releasing slot [{}].", slotToReturn.getAllocationId());
            }

            final ResourceProfile matchingResourceProfile =
                    getMatchingResourceProfile(slotToReturn.getAllocationId());
            fulfilledResourceRequirements =
                    fulfilledResourceRequirements.subtract(matchingResourceProfile, 1);
            slotToRequirementProfileMappings.remove(slotToReturn.getAllocationId());

            final CompletableFuture<Acknowledge> freeSlotFuture =
                    slotToReturn
                            .getTaskManagerGateway()
                            .freeSlot(slotToReturn.getAllocationId(), cause, rpcTimeout);

            freeSlotFuture.whenComplete(
                    (Acknowledge ignored, Throwable throwable) -> {
                        if (throwable != null) {
                            // The slot status will be synced to task manager in next heartbeat.
                            log.debug(
                                    "Releasing slot [{}] of registered TaskExecutor {} failed. Discarding slot.",
                                    slotToReturn.getAllocationId(),
                                    slotToReturn.getTaskManagerId(),
                                    throwable);
                        }
                    });
        }
    }

    @Override
    public FreeSlotInfoTracker getFreeSlotInfoTracker() {
        return slotPool.getFreeSlotInfoTracker();
    }

    @Override
    public Collection<? extends SlotInfo> getAllSlotsInformation() {
        return slotPool.getAllSlotsInformation();
    }

    @Override
    public boolean containsFreeSlot(AllocationID allocationId) {
        return slotPool.containsFreeSlot(allocationId);
    }

    @Override
    public boolean containsSlots(ResourceID owner) {
        return slotPool.containsSlots(owner);
    }

    private ResourceCounter getFulfilledRequirements(
            Iterable<? extends AllocatedSlot> allocatedSlots) {
        ResourceCounter resourceDecrement = ResourceCounter.empty();

        for (AllocatedSlot allocatedSlot : allocatedSlots) {
            final ResourceProfile matchingResourceProfile =
                    getMatchingResourceProfile(allocatedSlot.getAllocationId());
            resourceDecrement = resourceDecrement.add(matchingResourceProfile, 1);
        }

        return resourceDecrement;
    }

    @VisibleForTesting
    ResourceCounter getFulfilledResourceRequirements() {
        return fulfilledResourceRequirements;
    }
}
