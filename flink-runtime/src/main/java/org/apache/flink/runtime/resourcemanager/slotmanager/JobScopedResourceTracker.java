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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.slots.DefaultRequirementMatcher;
import org.apache.flink.runtime.slots.RequirementMatcher;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Tracks resource for a single job. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 跟踪单个作业的资源
*/
class JobScopedResourceTracker {

    private static final Logger LOG = LoggerFactory.getLogger(JobScopedResourceTracker.class);

    // only for logging purposes
    private final JobID jobId;
    /** 所需资源和所获取资源之间的双向映射。*/
    private final BiDirectionalResourceToRequirementMapping resourceToRequirementMapping =
            new BiDirectionalResourceToRequirementMapping();

    private final RequirementMatcher requirementMatcher = new DefaultRequirementMatcher();

    private ResourceCounter resourceRequirements = ResourceCounter.empty();
    private ResourceCounter excessResources = ResourceCounter.empty();

    JobScopedResourceTracker(JobID jobId) {
        this.jobId = Preconditions.checkNotNull(jobId);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 用来通知新的资源需求
    */
    public void notifyResourceRequirements(
            Collection<ResourceRequirement> newResourceRequirements) {
        /** 前置条件检查 */
        Preconditions.checkNotNull(newResourceRequirements);
        /**重置了 resourceRequirements(ResourceCounter) 变量 为一个空的资源计数器 */
        resourceRequirements = ResourceCounter.empty();
        /** 循环遍历 */
        for (ResourceRequirement newResourceRequirement : newResourceRequirements) {
            /**
             * 对于每个 ResourceRequirement，
             * 使用 resourceRequirements.add 方法将资源配置文件（ResourceProfile）和所需的槽位数量（numberOfRequiredSlots）
             * 添加到 resourceRequirements 计数器中。这实际上是在累加资源需求。
             */
            resourceRequirements =
                    resourceRequirements.add(
                            newResourceRequirement.getResourceProfile(),
                            newResourceRequirement.getNumberOfRequiredSlots());
        }
        /** 查找多余的槽位 */
        findExcessSlots();
        /** 尝试分配多余的槽位 */
        tryAssigningExcessSlots();
    }

    public void notifyAcquiredResource(ResourceProfile resourceProfile) {
        Preconditions.checkNotNull(resourceProfile);
        final Optional<ResourceProfile> matchingRequirement =
                findMatchingRequirement(resourceProfile);
        if (matchingRequirement.isPresent()) {
            resourceToRequirementMapping.incrementCount(
                    matchingRequirement.get(), resourceProfile, 1);
        } else {
            LOG.debug("Job {} acquired excess resource {}.", resourceProfile, jobId);
            excessResources = excessResources.add(resourceProfile, 1);
        }
    }

    private Optional<ResourceProfile> findMatchingRequirement(ResourceProfile resourceProfile) {
        return requirementMatcher.match(
                resourceProfile,
                resourceRequirements,
                resourceToRequirementMapping::getNumFulfillingResources);
    }

    public void notifyLostResource(ResourceProfile resourceProfile) {
        Preconditions.checkNotNull(resourceProfile);
        if (excessResources.getResourceCount(resourceProfile) > 0) {
            LOG.trace("Job {} lost excess resource {}.", jobId, resourceProfile);
            excessResources = excessResources.subtract(resourceProfile, 1);
            return;
        }

        Set<ResourceProfile> fulfilledRequirements =
                resourceToRequirementMapping
                        .getRequirementsFulfilledBy(resourceProfile)
                        .getResources();

        if (!fulfilledRequirements.isEmpty()) {
            // deduct the resource from any requirement
            // from a correctness standpoint the choice is arbitrary
            // from a resource utilization standpoint it could make sense to search for the
            // requirement with the largest
            // distance to the resource profile (i.e., the smallest requirement), but it may not
            // matter since we are
            // likely to get back a similarly-sized resource later on
            ResourceProfile assignedRequirement = fulfilledRequirements.iterator().next();

            resourceToRequirementMapping.decrementCount(assignedRequirement, resourceProfile, 1);

            tryAssigningExcessSlots();
        } else {
            throw new IllegalStateException(
                    String.format(
                            "Job %s lost a resource %s but no such resource was tracked.",
                            jobId, resourceProfile));
        }
    }

    public Collection<ResourceRequirement> getMissingResources() {
        final Collection<ResourceRequirement> missingResources = new ArrayList<>();
        for (Map.Entry<ResourceProfile, Integer> requirement :
                resourceRequirements.getResourcesWithCount()) {
            ResourceProfile requirementProfile = requirement.getKey();

            int numRequiredResources = requirement.getValue();
            int numAcquiredResources =
                    resourceToRequirementMapping.getNumFulfillingResources(requirementProfile);

            if (numAcquiredResources < numRequiredResources) {
                missingResources.add(
                        ResourceRequirement.create(
                                requirementProfile, numRequiredResources - numAcquiredResources));
            }
        }
        return missingResources;
    }

    public Collection<ResourceRequirement> getAcquiredResources() {
        final Set<ResourceProfile> knownResourceProfiles = new HashSet<>();
        knownResourceProfiles.addAll(resourceToRequirementMapping.getAllResourceProfiles());
        knownResourceProfiles.addAll(excessResources.getResources());

        final List<ResourceRequirement> acquiredResources = new ArrayList<>();
        for (ResourceProfile knownResourceProfile : knownResourceProfiles) {
            int numTotalAcquiredResources =
                    resourceToRequirementMapping.getNumFulfilledRequirements(knownResourceProfile)
                            + excessResources.getResourceCount(knownResourceProfile);
            ResourceRequirement resourceRequirement =
                    ResourceRequirement.create(knownResourceProfile, numTotalAcquiredResources);
            acquiredResources.add(resourceRequirement);
        }

        return acquiredResources;
    }

    public boolean isEmpty() {
        return resourceRequirements.isEmpty() && excessResources.isEmpty();
    }

    public boolean isRequirementEmpty() {
        return resourceRequirements.isEmpty();
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 定义一个私有方法，用于查找过剩的资源
    */
    private void findExcessSlots() {
        /** 创建一个ArrayList来存储过剩的资源   */
        final Collection<ExcessResource> excessResources = new ArrayList<>();


        /** 遍历所有的需求配置文件 */
        for (ResourceProfile requirementProfile :
                resourceToRequirementMapping.getAllRequirementProfiles()) {
            /** 获取当前需求配置文件所需的总资源数   */
            int numTotalRequiredResources =
                    resourceRequirements.getResourceCount(requirementProfile);
            /** 获取当前需求配置文件已满足的资源数   */
            int numTotalAcquiredResources =
                    resourceToRequirementMapping.getNumFulfillingResources(requirementProfile);
            /** 如果已满足的资源数大于所需的总资源数，说明有过剩资源   */
            if (numTotalAcquiredResources > numTotalRequiredResources) {
                /** 计算过剩资源的数量 */
                int numExcessResources = numTotalAcquiredResources - numTotalRequiredResources;
                /** 遍历所有满足当前需求配置文件的资源  */
                for (Map.Entry<ResourceProfile, Integer> acquiredResource :
                        resourceToRequirementMapping
                                .getResourcesFulfilling(requirementProfile)
                                .getResourcesWithCount()) {
                    /** 获取当前资源的配置文件 */
                    ResourceProfile acquiredResourceProfile = acquiredResource.getKey();
                    /** 获取当前资源的数量   */
                    int numAcquiredResources = acquiredResource.getValue();
                    /** 如果当前资源的数量小于或等于过剩资源的数量   */
                    if (numAcquiredResources <= numExcessResources) {
                        /** 将过剩的资源添加到excessResources集合中   */
                        excessResources.add(
                                new ExcessResource(
                                        requirementProfile,
                                        acquiredResourceProfile,
                                        numAcquiredResources));
                        /** 更新过剩资源的数量 */
                        numExcessResources -= numAcquiredResources;
                    } else {
                        /**
                         *  如果当前资源的数量大于过剩资源的数量
                         * 将过剩的资源添加到excessResources集合中，并跳出循环
                         */
                        excessResources.add(
                                new ExcessResource(
                                        requirementProfile,
                                        acquiredResourceProfile,
                                        numExcessResources));
                        break;
                    }
                }
            }
        }
        /** 如果存在过剩资源  */
        if (!excessResources.isEmpty()) {
            LOG.debug("Detected excess resources for job {}: {}", jobId, excessResources);
            /** 遍历过剩资源，对资源进行减量处理   */
            for (ExcessResource excessResource : excessResources) {
                /** 减少满足需求配置文件的资源数量 */
                resourceToRequirementMapping.decrementCount(
                        excessResource.requirementProfile,
                        excessResource.resourceProfile,
                        excessResource.numExcessResources);
                /** 更新过剩资源的统计信息 */
                this.excessResources =
                        this.excessResources.add(
                                excessResource.resourceProfile, excessResource.numExcessResources);
            }
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 尝试分配多余的slot
    */
    private void tryAssigningExcessSlots() {
        // 如果日志追踪功能已启用，则记录一条追踪日志
        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    "There are {} excess resources for job {} before re-assignment.",
                    excessResources.getTotalResourceCount(),
                    jobId);
        }
        // 创建一个空的资源计数器，用于记录已分配的资源
        ResourceCounter assignedResources = ResourceCounter.empty();
        // 遍历过剩资源的集合
        for (Map.Entry<ResourceProfile, Integer> excessResource :
                excessResources.getResourcesWithCount()) {
            // 循环分配过剩资源
            for (int i = 0; i < excessResource.getValue(); i++) {
                final ResourceProfile resourceProfile = excessResource.getKey();
                // 尝试找到与当前过剩资源匹配的作业需求
                final Optional<ResourceProfile> matchingRequirement =
                        findMatchingRequirement(resourceProfile);
                // 如果找到了匹配的作业需求
                if (matchingRequirement.isPresent()) {
                    // 在资源到需求的映射中增加计数
                    resourceToRequirementMapping.incrementCount(
                            matchingRequirement.get(), resourceProfile, 1);
                    // 在已分配的资源计数器中增加对应资源的计数
                    assignedResources = assignedResources.add(resourceProfile, 1);
                } else {
                    break;
                }
            }
        }
        // 遍历已分配的资源，并从过剩资源中减去这些资源
        for (Map.Entry<ResourceProfile, Integer> assignedResource :
                assignedResources.getResourcesWithCount()) {
            excessResources =
                    excessResources.subtract(
                            assignedResource.getKey(), assignedResource.getValue());
        }
         // 如果日志追踪功能已启用，则记录一条追踪日志
        if (LOG.isTraceEnabled()) {
            LOG.trace(
                    "There are {} excess resources for job {} after re-assignment.",
                    excessResources.getTotalResourceCount(),
                    jobId);
        }
    }

    private static class ExcessResource {
        private final ResourceProfile requirementProfile;
        private final ResourceProfile resourceProfile;
        private final int numExcessResources;

        private ExcessResource(
                ResourceProfile requirementProfile,
                ResourceProfile resourceProfile,
                int numExcessResources) {
            this.requirementProfile = requirementProfile;
            this.resourceProfile = resourceProfile;
            this.numExcessResources = numExcessResources;
        }

        @Override
        public String toString() {
            return "ExcessResource{"
                    + "numExcessResources="
                    + numExcessResources
                    + ", requirementProfile="
                    + requirementProfile
                    + ", resourceProfile="
                    + resourceProfile
                    + '}';
        }
    }
}
