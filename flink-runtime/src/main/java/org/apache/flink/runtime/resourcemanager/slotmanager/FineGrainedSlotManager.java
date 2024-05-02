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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.blocklist.BlockedTaskManagerChecker;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.rest.messages.taskmanager.SlotInfo;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Implementation of {@link SlotManager} supporting fine-grained resource management. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 细粒度资源管理
*/
public class FineGrainedSlotManager implements SlotManager {
    private static final Logger LOG = LoggerFactory.getLogger(FineGrainedSlotManager.class);

    private final TaskManagerTracker taskManagerTracker;
    private final ResourceTracker resourceTracker;
    private final ResourceAllocationStrategy resourceAllocationStrategy;

    private final SlotStatusSyncer slotStatusSyncer;

    /** Scheduled executor for timeouts. */
    private final ScheduledExecutor scheduledExecutor;

    /** Timeout after which an unused TaskManager is released. */
    private final Time taskManagerTimeout;

    /** Delay of the requirement change check in the slot manager. */
    private final Duration requirementsCheckDelay;

    /** Delay of the resource declare in the slot manager. */
    private final Duration declareNeededResourceDelay;

    private final SlotManagerMetricGroup slotManagerMetricGroup;

    private final Map<JobID, String> jobMasterTargetAddresses = new HashMap<>();

    /**
     * Release task executor only when each produced result partition is either consumed or failed.
     */
    private final boolean waitResultConsumedBeforeRelease;

    private final CPUResource maxTotalCpu;
    private final MemorySize maxTotalMem;

    private boolean sendNotEnoughResourceNotifications = true;

    private final Set<JobID> unfulfillableJobs = new HashSet<>();

    /** ResourceManager's id. */
    @Nullable private ResourceManagerId resourceManagerId;

    /** Executor for future callbacks which have to be "synchronized". */
    @Nullable private Executor mainThreadExecutor;

    /** Callbacks for resource (de-)allocations. */
    @Nullable private ResourceAllocator resourceAllocator;

    /** Callbacks for resource not enough. */
    @Nullable private ResourceEventListener resourceEventListener;

    @Nullable private ScheduledFuture<?> clusterReconciliationCheck;

    @Nullable private CompletableFuture<Void> requirementsCheckFuture;

    @Nullable private CompletableFuture<Void> declareNeededResourceFuture;

    /** Blocked task manager checker. */
    @Nullable private BlockedTaskManagerChecker blockedTaskManagerChecker;

    /** True iff the component has been started. */
    private boolean started;

    public FineGrainedSlotManager(
            ScheduledExecutor scheduledExecutor,
            SlotManagerConfiguration slotManagerConfiguration,
            SlotManagerMetricGroup slotManagerMetricGroup,
            ResourceTracker resourceTracker,
            TaskManagerTracker taskManagerTracker,
            SlotStatusSyncer slotStatusSyncer,
            ResourceAllocationStrategy resourceAllocationStrategy) {

        this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);

        Preconditions.checkNotNull(slotManagerConfiguration);
        this.taskManagerTimeout = slotManagerConfiguration.getTaskManagerTimeout();
        this.waitResultConsumedBeforeRelease =
                slotManagerConfiguration.isWaitResultConsumedBeforeRelease();
        this.requirementsCheckDelay =
                Preconditions.checkNotNull(slotManagerConfiguration.getRequirementCheckDelay());
        this.declareNeededResourceDelay =
                Preconditions.checkNotNull(
                        slotManagerConfiguration.getDeclareNeededResourceDelay());

        this.slotManagerMetricGroup = Preconditions.checkNotNull(slotManagerMetricGroup);

        this.resourceTracker = Preconditions.checkNotNull(resourceTracker);
        this.taskManagerTracker = Preconditions.checkNotNull(taskManagerTracker);
        this.slotStatusSyncer = Preconditions.checkNotNull(slotStatusSyncer);
        this.resourceAllocationStrategy = Preconditions.checkNotNull(resourceAllocationStrategy);

        this.maxTotalCpu = Preconditions.checkNotNull(slotManagerConfiguration.getMaxTotalCpu());
        this.maxTotalMem = Preconditions.checkNotNull(slotManagerConfiguration.getMaxTotalMem());

        resourceManagerId = null;
        resourceAllocator = null;
        resourceEventListener = null;
        mainThreadExecutor = null;
        clusterReconciliationCheck = null;
        requirementsCheckFuture = null;

        started = false;
    }

    @Override
    public void setFailUnfulfillableRequest(boolean failUnfulfillableRequest) {
        checkInit();
        // this sets up a grace period, e.g., when the cluster was started, to give task executors
        // time to connect
        sendNotEnoughResourceNotifications = failUnfulfillableRequest;

        if (failUnfulfillableRequest && !unfulfillableJobs.isEmpty()) {
            for (JobID jobId : unfulfillableJobs) {
                resourceEventListener.notEnoughResourceAvailable(
                        jobId, resourceTracker.getAcquiredResources(jobId));
            }
        }
    }

    @Override
    public void triggerResourceRequirementsCheck() {
        checkResourceRequirementsWithDelay();
    }

    // ---------------------------------------------------------------------------------------------
    // Component lifecycle methods
    // ---------------------------------------------------------------------------------------------

    /**
     * Starts the slot manager with the given leader id and resource manager actions.
     *
     * @param newResourceManagerId to use for communication with the task managers
     * @param newMainThreadExecutor to use to run code in the ResourceManager's main thread
     * @param newResourceAllocator to use for resource (de-)allocations
     * @param newBlockedTaskManagerChecker to query whether a task manager is blocked
     */
    @Override
    public void start(
            ResourceManagerId newResourceManagerId,
            Executor newMainThreadExecutor,
            ResourceAllocator newResourceAllocator,
            ResourceEventListener newResourceEventListener,
            BlockedTaskManagerChecker newBlockedTaskManagerChecker) {
        LOG.info("Starting the slot manager.");

        resourceManagerId = Preconditions.checkNotNull(newResourceManagerId);
        mainThreadExecutor = Preconditions.checkNotNull(newMainThreadExecutor);
        resourceAllocator = Preconditions.checkNotNull(newResourceAllocator);
        resourceEventListener = Preconditions.checkNotNull(newResourceEventListener);
        slotStatusSyncer.initialize(
                taskManagerTracker, resourceTracker, resourceManagerId, mainThreadExecutor);
        blockedTaskManagerChecker = Preconditions.checkNotNull(newBlockedTaskManagerChecker);

        started = true;

        if (resourceAllocator.isSupported()) {
            clusterReconciliationCheck =
                    scheduledExecutor.scheduleWithFixedDelay(
                            () -> mainThreadExecutor.execute(this::checkClusterReconciliation),
                            0L,
                            taskManagerTimeout.toMilliseconds(),
                            TimeUnit.MILLISECONDS);
        }

        registerSlotManagerMetrics();
    }

    private void registerSlotManagerMetrics() {
        slotManagerMetricGroup.gauge(
                MetricNames.TASK_SLOTS_AVAILABLE, () -> (long) getNumberFreeSlots());
        slotManagerMetricGroup.gauge(
                MetricNames.TASK_SLOTS_TOTAL, () -> (long) getNumberRegisteredSlots());
    }

    /** Suspends the component. This clears the internal state of the slot manager. */
    @Override
    public void suspend() {
        if (!started) {
            return;
        }

        LOG.info("Suspending the slot manager.");

        slotManagerMetricGroup.close();

        // stop the timeout checks for the TaskManagers
        if (clusterReconciliationCheck != null) {
            clusterReconciliationCheck.cancel(false);
            clusterReconciliationCheck = null;
        }

        slotStatusSyncer.close();
        taskManagerTracker.clear();
        resourceTracker.clear();

        unfulfillableJobs.clear();
        resourceManagerId = null;
        resourceAllocator = null;
        resourceEventListener = null;
        started = false;
    }

    /**
     * Closes the slot manager.
     *
     * @throws Exception if the close operation fails
     */
    @Override
    public void close() throws Exception {
        LOG.info("Closing the slot manager.");

        suspend();
    }

    // ---------------------------------------------------------------------------------------------
    // Public API
    // ---------------------------------------------------------------------------------------------

    @Override
    public void clearResourceRequirements(JobID jobId) {
        maybeReclaimInactiveSlots(jobId);
        jobMasterTargetAddresses.remove(jobId);
        resourceTracker.notifyResourceRequirements(jobId, Collections.emptyList());
        if (resourceAllocator.isSupported()) {
            taskManagerTracker.clearPendingAllocationsOfJob(jobId);
            checkResourcesNeedReconcile();
            declareNeededResourcesWithDelay();
        }
    }

    private void maybeReclaimInactiveSlots(JobID jobId) {
        // We don't notify the task manager tracker and resource tracker about the freeing of these
        // slots early, and rely on task managers to report the slots becoming available later to
        // keep the states consistent.
        if (!resourceTracker.getAcquiredResources(jobId).isEmpty()) {
            slotStatusSyncer.freeInactiveSlots(jobId);
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 处理请求的资源
    */
    @Override
    public void processResourceRequirements(ResourceRequirements resourceRequirements) {
        /** 检查基本配置是否为空 */
        checkInit();
        /** 跳过重复的空资源需求。 */
        if (resourceRequirements.getResourceRequirements().isEmpty()
                && resourceTracker.isRequirementEmpty(resourceRequirements.getJobId())) {
            // Skip duplicate empty resource requirements.
            return;
        }
        /**
         * 如果作业的总资源需求为空，则移除JobId,清除taskManagerTracker跟踪的TakManager的资源和插槽状态
         */
        if (resourceRequirements.getResourceRequirements().isEmpty()) {
            LOG.info("Clearing resource requirements of job {}", resourceRequirements.getJobId());
            jobMasterTargetAddresses.remove(resourceRequirements.getJobId());
            /**
             * ResourceAllocator是否支持释放回收资源，Standalone不支持
             */
            if (resourceAllocator.isSupported()) {
                /** 清除给定作业以前所有挂起的Slot分配记录 */
                taskManagerTracker.clearPendingAllocationsOfJob(resourceRequirements.getJobId());
            }
        } else {
            LOG.info(
                    "Received resource requirements from job {}: {}",
                    resourceRequirements.getJobId(),
                    resourceRequirements.getResourceRequirements());
            /**
             * 将JobID,JobMaster地址放入Map结构中
             * 2e827cfcdc7ac1a9d3ec0e92ecc88702,pekko.tcp://flink@localhost:6123/user/rpc/jobmanager_2
             */
            jobMasterTargetAddresses.put(
                    resourceRequirements.getJobId(), resourceRequirements.getTargetAddress());
        }
        /**
         * 调用resourceTracker.notifyResourceRequirements 通知跟踪器跟踪新的或更新的ResourceRequirements
         * 1.将JobId 放入如下数据结构
         * Map<JobID, JobScopedResourceTracker> trackers = new HashMap<>()
         * JobScopedResourceTracker跟踪单个作业的资源
         * 2.resourceRequirements 构建成如下结构 请求的CPU内存，numSlot数
         *  private final Map<ResourceProfile, Integer> resources;
         *  并添加到JobScopedResourceTracker.resourceRequirements对象中
         *  说到底还是跟踪资源每个作业的资源
         */
        resourceTracker.notifyResourceRequirements(
                resourceRequirements.getJobId(), resourceRequirements.getResourceRequirements());
        /**
         * 检查资源并申请
         */
        checkResourceRequirementsWithDelay();
    }

    @Override
    public RegistrationResult registerTaskManager(
            final TaskExecutorConnection taskExecutorConnection,
            SlotReport initialSlotReport,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile) {
        checkInit();
        LOG.info(
                "Registering task executor {} under {} at the slot manager.",
                taskExecutorConnection.getResourceID(),
                taskExecutorConnection.getInstanceID());

        // we identify task managers by their instance id
        if (taskManagerTracker
                .getRegisteredTaskManager(taskExecutorConnection.getInstanceID())
                .isPresent()) {
            LOG.debug(
                    "Task executor {} was already registered.",
                    taskExecutorConnection.getResourceID());
            reportSlotStatus(taskExecutorConnection.getInstanceID(), initialSlotReport);
            return RegistrationResult.IGNORED;
        } else {
            Optional<PendingTaskManager> matchedPendingTaskManagerOptional =
                    initialSlotReport.hasAllocatedSlot()
                            ? Optional.empty()
                            : findMatchingPendingTaskManager(
                                    totalResourceProfile, defaultSlotResourceProfile);

            if (!matchedPendingTaskManagerOptional.isPresent()
                    && isMaxTotalResourceExceededAfterAdding(totalResourceProfile)) {

                LOG.info(
                        "Can not register task manager {}. The max total resource limitation <{}, {}> is reached.",
                        taskExecutorConnection.getResourceID(),
                        maxTotalCpu,
                        maxTotalMem.toHumanReadableString());
                return RegistrationResult.REJECTED;
            }

            taskManagerTracker.addTaskManager(
                    taskExecutorConnection, totalResourceProfile, defaultSlotResourceProfile);

            if (initialSlotReport.hasAllocatedSlot()) {
                slotStatusSyncer.reportSlotStatus(
                        taskExecutorConnection.getInstanceID(), initialSlotReport);
            }

            if (matchedPendingTaskManagerOptional.isPresent()) {
                PendingTaskManager pendingTaskManager = matchedPendingTaskManagerOptional.get();
                allocateSlotsForRegisteredPendingTaskManager(
                        pendingTaskManager, taskExecutorConnection.getInstanceID());
                taskManagerTracker.removePendingTaskManager(
                        pendingTaskManager.getPendingTaskManagerId());
                return RegistrationResult.SUCCESS;
            }

            checkResourceRequirementsWithDelay();
            return RegistrationResult.SUCCESS;
        }
    }

    private void declareNeededResourcesWithDelay() {
        Preconditions.checkState(resourceAllocator.isSupported());

        if (declareNeededResourceDelay.toMillis() <= 0) {
            declareNeededResources();
        } else {
            if (declareNeededResourceFuture == null || declareNeededResourceFuture.isDone()) {
                declareNeededResourceFuture = new CompletableFuture<>();
                scheduledExecutor.schedule(
                        () ->
                                mainThreadExecutor.execute(
                                        () -> {
                                            declareNeededResources();
                                            Preconditions.checkNotNull(declareNeededResourceFuture)
                                                    .complete(null);
                                        }),
                        declareNeededResourceDelay.toMillis(),
                        TimeUnit.MILLISECONDS);
            }
        }
    }

    /** DO NOT call this method directly. Use {@link #declareNeededResourcesWithDelay()} instead. */
    private void declareNeededResources() {
        Map<InstanceID, WorkerResourceSpec> unWantedTaskManagers =
                taskManagerTracker.getUnWantedTaskManager();
        Map<WorkerResourceSpec, Set<InstanceID>> unWantedTaskManagerBySpec =
                unWantedTaskManagers.entrySet().stream()
                        .collect(
                                Collectors.groupingBy(
                                        Map.Entry::getValue,
                                        Collectors.mapping(Map.Entry::getKey, Collectors.toSet())));

        // registered TaskManagers except unwanted worker.
        Stream<WorkerResourceSpec> registeredTaskManagerStream =
                taskManagerTracker.getRegisteredTaskManagers().stream()
                        .filter(t -> !unWantedTaskManagers.containsKey(t.getInstanceId()))
                        .map(
                                t ->
                                        WorkerResourceSpec.fromTotalResourceProfile(
                                                t.getTotalResource(), t.getDefaultNumSlots()));
        // pending TaskManagers.
        Stream<WorkerResourceSpec> pendingTaskManagerStream =
                taskManagerTracker.getPendingTaskManagers().stream()
                        .map(
                                t ->
                                        WorkerResourceSpec.fromTotalResourceProfile(
                                                t.getTotalResourceProfile(), t.getNumSlots()));

        Map<WorkerResourceSpec, Integer> requiredWorkers =
                Stream.concat(registeredTaskManagerStream, pendingTaskManagerStream)
                        .collect(
                                Collectors.groupingBy(
                                        Function.identity(), Collectors.summingInt(e -> 1)));

        Set<WorkerResourceSpec> workerResourceSpecs = new HashSet<>(requiredWorkers.keySet());
        workerResourceSpecs.addAll(unWantedTaskManagerBySpec.keySet());

        List<ResourceDeclaration> resourceDeclarations = new ArrayList<>();
        workerResourceSpecs.forEach(
                spec ->
                        resourceDeclarations.add(
                                new ResourceDeclaration(
                                        spec,
                                        requiredWorkers.getOrDefault(spec, 0),
                                        unWantedTaskManagerBySpec.getOrDefault(
                                                spec, Collections.emptySet()))));

        resourceAllocator.declareResourceNeeded(resourceDeclarations);
    }

    private void allocateSlotsForRegisteredPendingTaskManager(
            PendingTaskManager pendingTaskManager, InstanceID instanceId) {
        Map<JobID, Map<InstanceID, ResourceCounter>> allocations =
                pendingTaskManager.getPendingSlotAllocationRecords().entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        (e) -> Collections.singletonMap(instanceId, e.getValue())));
        allocateSlotsAccordingTo(allocations);
    }

    private Optional<PendingTaskManager> findMatchingPendingTaskManager(
            ResourceProfile totalResourceProfile, ResourceProfile defaultSlotResourceProfile) {
        Collection<PendingTaskManager> matchedPendingTaskManagers =
                taskManagerTracker.getPendingTaskManagersByTotalAndDefaultSlotResourceProfile(
                        totalResourceProfile, defaultSlotResourceProfile);

        Optional<PendingTaskManager> matchedPendingTaskManagerIdsWithAllocatedSlots =
                matchedPendingTaskManagers.stream()
                        .filter(
                                (pendingTaskManager) ->
                                        !pendingTaskManager
                                                .getPendingSlotAllocationRecords()
                                                .isEmpty())
                        .findAny();

        if (matchedPendingTaskManagerIdsWithAllocatedSlots.isPresent()) {
            return matchedPendingTaskManagerIdsWithAllocatedSlots;
        } else {
            return matchedPendingTaskManagers.stream().findAny();
        }
    }

    @Override
    public boolean unregisterTaskManager(InstanceID instanceId, Exception cause) {
        checkInit();

        LOG.info("Unregistering task executor {} from the slot manager.", instanceId);

        if (taskManagerTracker.getRegisteredTaskManager(instanceId).isPresent()) {
            Set<AllocationID> allocatedSlots =
                    new HashSet<>(
                            taskManagerTracker
                                    .getRegisteredTaskManager(instanceId)
                                    .get()
                                    .getAllocatedSlots()
                                    .keySet());
            for (AllocationID allocationId : allocatedSlots) {
                slotStatusSyncer.freeSlot(allocationId);
            }
            taskManagerTracker.removeTaskManager(instanceId);
            if (!allocatedSlots.isEmpty()) {
                checkResourceRequirementsWithDelay();
            }

            return true;
        } else {
            LOG.debug(
                    "There is no task executor registered with instance ID {}. Ignoring this message.",
                    instanceId);

            return false;
        }
    }

    /**
     * Reports the current slot allocations for a task manager identified by the given instance id.
     *
     * @param instanceId identifying the task manager for which to report the slot status
     * @param slotReport containing the status for all of its slots
     * @return true if the slot status has been updated successfully, otherwise false
     */
    @Override
    public boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport) {
        checkInit();

        LOG.debug("Received slot report from instance {}: {}.", instanceId, slotReport);

        if (taskManagerTracker.getRegisteredTaskManager(instanceId).isPresent()) {
            if (!slotStatusSyncer.reportSlotStatus(instanceId, slotReport)) {
                checkResourceRequirementsWithDelay();
            }
            return true;
        } else {
            LOG.debug(
                    "Received slot report for unknown task manager with instance id {}. Ignoring this report.",
                    instanceId);

            return false;
        }
    }

    /**
     * Free the given slot from the given allocation. If the slot is still allocated by the given
     * allocation id, then the slot will be freed.
     *
     * @param slotId identifying the slot to free, will be ignored
     * @param allocationId with which the slot is presumably allocated
     */
    @Override
    public void freeSlot(SlotID slotId, AllocationID allocationId) {
        checkInit();
        LOG.debug("Freeing slot {}.", allocationId);

        if (taskManagerTracker.getAllocatedOrPendingSlot(allocationId).isPresent()) {
            slotStatusSyncer.freeSlot(allocationId);
            checkResourceRequirementsWithDelay();
        } else {
            LOG.debug(
                    "Trying to free a slot {} which has not been allocated. Ignoring this message.",
                    allocationId);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Requirement matching
    // ---------------------------------------------------------------------------------------------

    /**
     * Depending on the implementation of {@link ResourceAllocationStrategy}, checking resource
     * requirements and potentially making a re-allocation can be heavy. In order to cover more
     * changes with each check, thus reduce the frequency of unnecessary re-allocations, the checks
     * are performed with a slight delay.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 根据 ResourceAllocationStrategy 的实现，检查资源需求和可能进行重新分配可能会很繁重。
     * 为了在每次检查中覆盖更多的更改，从而减少不必要的重新分配的频率，检查的执行略有延迟。
    */
    private void checkResourceRequirementsWithDelay() {
        /**
         * 如果延迟时间小于等于0，则直接进入checkResourceRequirements 资源检查
         */
        if (requirementsCheckDelay.toMillis() <= 0) {
            checkResourceRequirements();
        } else {
            if (requirementsCheckFuture == null || requirementsCheckFuture.isDone()) {
                requirementsCheckFuture = new CompletableFuture<>();
                /**
                 * 线程池、调度器延迟执行checkResourceRequirements，资源检查
                 */
                scheduledExecutor.schedule(
                        () ->
                                mainThreadExecutor.execute(
                                        () -> {
                                            checkResourceRequirements();
                                            Preconditions.checkNotNull(requirementsCheckFuture)
                                                    .complete(null);
                                        }),
                        requirementsCheckDelay.toMillis(),
                        TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * DO NOT call this method directly. Use {@link #checkResourceRequirementsWithDelay()} instead.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 资源检查
    */
    private void checkResourceRequirements() {
        if (!started) {
            return;
        }
        /**
         * 从JobScopedResourceTracker结合中获取JobId,ResourceRequirement
         * 这里会去查看资源是否能够运行我们任务，如果资源能够则直接运行
         */
        Map<JobID, Collection<ResourceRequirement>> missingResources =
                resourceTracker.getMissingResources();
        /** missingResources如果为空则协调资源 */
        if (missingResources.isEmpty()) {
            if (resourceAllocator.isSupported()
                    && !taskManagerTracker.getPendingTaskManagers().isEmpty()) {
                taskManagerTracker.replaceAllPendingAllocations(Collections.emptyMap());
                checkResourcesNeedReconcile();
                declareNeededResourcesWithDelay();
            }
            return;
        }
        /** 将资源信息以日志形式打印出来 */
        logMissingAndAvailableResource(missingResources);
        /** 构建Map结构JobId,ResourceRequirements */
        missingResources =
                missingResources.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey, e -> new ArrayList<>(e.getValue())));
        /** 尝试做出分配决定以满足资源需求。该策略根据当前状态生成ResourceAllocationResult结果。 */
        final ResourceAllocationResult result =
                resourceAllocationStrategy.tryFulfillRequirements(
                        missingResources, taskManagerTracker, this::isBlockedTaskManager);

        // Allocate slots according to the result
        /** 根据结果分配插槽 */
        allocateSlotsAccordingTo(result.getAllocationsOnRegisteredResources());

        final Set<PendingTaskManagerId> failAllocations;
        /**
         *  ResourceAllocator是否支持释放回收资源，Standalone不支持
         *  为什么有下面的是因为如果有些没分配完成，则继续分配
         */

        if (resourceAllocator.isSupported()) {
            // Allocate task managers according to the result
            /**
             * 分配挂起的任务管理器，返回不能分配的挂起任务管理器的id。
             */
            failAllocations =
                    allocateTaskManagersAccordingTo(result.getPendingTaskManagersToAllocate());

            // Record slot allocation of pending task managers
            /**
             * 记录挂起任务管理器的插槽分配,就是分配及结果
             */
            final Map<PendingTaskManagerId, Map<JobID, ResourceCounter>>
                    pendingResourceAllocationResult =
                            new HashMap<>(result.getAllocationsOnPendingResources());
            pendingResourceAllocationResult.keySet().removeAll(failAllocations);
            taskManagerTracker.replaceAllPendingAllocations(pendingResourceAllocationResult);
        } else {
            failAllocations =
                    result.getPendingTaskManagersToAllocate().stream()
                            .map(PendingTaskManager::getPendingTaskManagerId)
                            .collect(Collectors.toSet());
        }
        /**
         * 将不能分配的挂起TaskManager的id。放入unfulfillableJobs
         */
        unfulfillableJobs.clear();
        unfulfillableJobs.addAll(result.getUnfulfillableJobs());
        for (PendingTaskManagerId pendingTaskManagerId : failAllocations) {
            unfulfillableJobs.addAll(
                    result.getAllocationsOnPendingResources().get(pendingTaskManagerId).keySet());
        }
        // Notify jobs that can not be fulfilled
        if (sendNotEnoughResourceNotifications) {
            for (JobID jobId : unfulfillableJobs) {
                LOG.warn("Could not fulfill resource requirements of job {}.", jobId);
                resourceEventListener.notEnoughResourceAvailable(
                        jobId, resourceTracker.getAcquiredResources(jobId));
            }
        }

        if (resourceAllocator.isSupported()) {
            checkResourcesNeedReconcile();
            declareNeededResourcesWithDelay();
        }
    }

    private void logMissingAndAvailableResource(
            Map<JobID, Collection<ResourceRequirement>> missingResources) {
        final StringJoiner lines = new StringJoiner(System.lineSeparator());
        lines.add("Matching resource requirements against available resources.");
        lines.add("Missing resources:");
        missingResources.forEach(
                (jobId, resources) -> {
                    lines.add("\t Job " + jobId);
                    resources.forEach(resource -> lines.add(String.format("\t\t%s", resource)));
                });
        lines.add("Current resources:");
        if (taskManagerTracker.getRegisteredTaskManagers().isEmpty()) {
            lines.add("\t(none)");
        } else {
            for (TaskManagerInfo taskManager : taskManagerTracker.getRegisteredTaskManagers()) {
                final ResourceID resourceId =
                        taskManager.getTaskExecutorConnection().getResourceID();
                lines.add("\tTaskManager " + resourceId);
                lines.add("\t\tAvailable: " + taskManager.getAvailableResource());
                lines.add("\t\tTotal:     " + taskManager.getTotalResource());
            }
        }
        LOG.info(lines.toString());
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 分配资源
    */
    private void allocateSlotsAccordingTo(Map<JobID, Map<InstanceID, ResourceCounter>> result) {
        /** 初始化Future列表 */
        final List<CompletableFuture<Void>> allocationFutures = new ArrayList<>();
        /** 遍历作业和资源映射 ResourceCounter Map<ResourceProfile, Integer> resources */
        for (Map.Entry<JobID, Map<InstanceID, ResourceCounter>> jobEntry : result.entrySet()) {
            final JobID jobID = jobEntry.getKey();
            /** 遍历实例和资源 */
            for (Map.Entry<InstanceID, ResourceCounter> tmEntry : jobEntry.getValue().entrySet()) {
                final InstanceID instanceID = tmEntry.getKey();
                for (Map.Entry<ResourceProfile, Integer> slotEntry :
                        tmEntry.getValue().getResourcesWithCount()) {
                    for (int i = 0; i < slotEntry.getValue(); ++i) {
                        allocationFutures.add(
                                /**
                                 * 分配槽位
                                 * SlotStatusSyncer负责分配/释放插槽并协调任务管理器的插槽状态
                                 * 调用SlotStatusSyncer.allocateSlot申请资源
                                 */
                                slotStatusSyncer.allocateSlot(
                                        instanceID,
                                        jobID,
                                        jobMasterTargetAddresses.get(jobID),
                                        slotEntry.getKey()));
                    }
                }
            }
        }
        /** 等待所有分配完成 */
        FutureUtils.combineAll(allocationFutures)
                /** 处理完成结果 */
                .whenCompleteAsync(
                        (s, t) -> {
                            if (t != null) {
                                // If there is allocation failure, we need to trigger it again.
                                checkResourceRequirementsWithDelay();
                            }
                        },
                        mainThreadExecutor);
    }

    /**
     * Allocate pending task managers, returns the ids of pending task managers that can not be
     * allocated.
     */
    private Set<PendingTaskManagerId> allocateTaskManagersAccordingTo(
            List<PendingTaskManager> pendingTaskManagers) {
        Preconditions.checkState(resourceAllocator.isSupported());
        final Set<PendingTaskManagerId> failedAllocations = new HashSet<>();
        for (PendingTaskManager pendingTaskManager : pendingTaskManagers) {
            if (!allocateResource(pendingTaskManager)) {
                failedAllocations.add(pendingTaskManager.getPendingTaskManagerId());
            }
        }
        return failedAllocations;
    }

    // ---------------------------------------------------------------------------------------------
    // Legacy APIs
    // ---------------------------------------------------------------------------------------------

    @Override
    public int getNumberRegisteredSlots() {
        return taskManagerTracker.getNumberRegisteredSlots();
    }

    @Override
    public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
        return taskManagerTracker.getNumberRegisteredSlotsOf(instanceId);
    }

    @Override
    public int getNumberFreeSlots() {
        return taskManagerTracker.getNumberFreeSlots();
    }

    @Override
    public int getNumberFreeSlotsOf(InstanceID instanceId) {
        return taskManagerTracker.getNumberFreeSlotsOf(instanceId);
    }

    @Override
    public ResourceProfile getRegisteredResource() {
        return taskManagerTracker.getRegisteredResource();
    }

    @Override
    public ResourceProfile getRegisteredResourceOf(InstanceID instanceID) {
        return taskManagerTracker.getRegisteredResourceOf(instanceID);
    }

    @Override
    public ResourceProfile getFreeResource() {
        return taskManagerTracker.getFreeResource();
    }

    @Override
    public ResourceProfile getFreeResourceOf(InstanceID instanceID) {
        return taskManagerTracker.getFreeResourceOf(instanceID);
    }

    @Override
    public Collection<SlotInfo> getAllocatedSlotsOf(InstanceID instanceID) {
        return taskManagerTracker.getRegisteredTaskManager(instanceID)
                .map(TaskManagerInfo::getAllocatedSlots).map(Map::values)
                .orElse(Collections.emptyList()).stream()
                .map(slot -> new SlotInfo(slot.getJobId(), slot.getResourceProfile()))
                .collect(Collectors.toList());
    }

    // ---------------------------------------------------------------------------------------------
    // Internal periodic check methods
    // ---------------------------------------------------------------------------------------------

    private void checkClusterReconciliation() {
        if (checkResourcesNeedReconcile()) {
            // only declare on needed.
            declareNeededResourcesWithDelay();
        }
    }

    private boolean checkResourcesNeedReconcile() {
        ResourceReconcileResult reconcileResult =
                resourceAllocationStrategy.tryReconcileClusterResources(taskManagerTracker);

        reconcileResult.getPendingTaskManagersToRelease().stream()
                .map(PendingTaskManager::getPendingTaskManagerId)
                .forEach(taskManagerTracker::removePendingTaskManager);

        for (TaskManagerInfo taskManagerToRelease : reconcileResult.getTaskManagersToRelease()) {
            if (waitResultConsumedBeforeRelease) {
                releaseIdleTaskExecutorIfPossible(taskManagerToRelease);
            } else {
                releaseIdleTaskExecutor(taskManagerToRelease.getInstanceId());
            }
        }

        reconcileResult.getPendingTaskManagersToAllocate().forEach(this::allocateResource);

        return reconcileResult.needReconcile();
    }

    private void releaseIdleTaskExecutorIfPossible(TaskManagerInfo taskManagerInfo) {
        final long idleSince = taskManagerInfo.getIdleSince();
        taskManagerInfo
                .getTaskExecutorConnection()
                .getTaskExecutorGateway()
                .canBeReleased()
                .thenAcceptAsync(
                        canBeReleased -> {
                            boolean stillIdle = idleSince == taskManagerInfo.getIdleSince();
                            if (stillIdle && canBeReleased) {
                                releaseIdleTaskExecutor(taskManagerInfo.getInstanceId());
                                declareNeededResourcesWithDelay();
                            }
                        },
                        mainThreadExecutor);
    }

    private void releaseIdleTaskExecutor(InstanceID taskManagerToRelease) {
        Preconditions.checkState(resourceAllocator.isSupported());
        taskManagerTracker.addUnWantedTaskManager(taskManagerToRelease);
    }

    private boolean allocateResource(PendingTaskManager pendingTaskManager) {
        Preconditions.checkState(resourceAllocator.isSupported());
        if (isMaxTotalResourceExceededAfterAdding(pendingTaskManager.getTotalResourceProfile())) {
            LOG.info(
                    "Could not allocate {}. Max total resource limitation <{}, {}> is reached.",
                    pendingTaskManager,
                    maxTotalCpu,
                    maxTotalMem.toHumanReadableString());
            return false;
        }

        taskManagerTracker.addPendingTaskManager(pendingTaskManager);
        return true;
    }

    @VisibleForTesting
    public long getTaskManagerIdleSince(InstanceID instanceId) {
        return taskManagerTracker
                .getRegisteredTaskManager(instanceId)
                .map(TaskManagerInfo::getIdleSince)
                .orElse(0L);
    }

    // ---------------------------------------------------------------------------------------------
    // Internal utility methods
    // ---------------------------------------------------------------------------------------------

    private void checkInit() {
        Preconditions.checkState(started, "The slot manager has not been started.");
        Preconditions.checkNotNull(resourceManagerId);
        Preconditions.checkNotNull(mainThreadExecutor);
        Preconditions.checkNotNull(resourceAllocator);
        Preconditions.checkNotNull(resourceEventListener);
    }

    private boolean isMaxTotalResourceExceededAfterAdding(ResourceProfile newResource) {
        final ResourceProfile totalResourceAfterAdding =
                newResource
                        .merge(taskManagerTracker.getRegisteredResource())
                        .merge(taskManagerTracker.getPendingResource());
        return totalResourceAfterAdding.getCpuCores().compareTo(maxTotalCpu) > 0
                || totalResourceAfterAdding.getTotalMemory().compareTo(maxTotalMem) > 0;
    }

    private boolean isBlockedTaskManager(ResourceID resourceID) {
        Preconditions.checkNotNull(blockedTaskManagerChecker);
        return blockedTaskManagerChecker.isBlockedTaskManager(resourceID);
    }
}
