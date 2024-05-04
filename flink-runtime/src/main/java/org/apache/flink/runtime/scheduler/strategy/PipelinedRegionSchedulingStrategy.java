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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.SchedulerOperations;
import org.apache.flink.util.IterableUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SchedulingStrategy} instance which schedules tasks in granularity of pipelined regions.
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * Pipeline 的粒度调度任务
*/
public class PipelinedRegionSchedulingStrategy implements SchedulingStrategy {

    private final SchedulerOperations schedulerOperations;

    private final SchedulingTopology schedulingTopology;

    /** External consumer regions of each ConsumedPartitionGroup. */
    private final Map<ConsumedPartitionGroup, Set<SchedulingPipelinedRegion>>
            partitionGroupConsumerRegions = new IdentityHashMap<>();

    private final Map<SchedulingPipelinedRegion, List<ExecutionVertexID>> regionVerticesSorted =
            new IdentityHashMap<>();

    /** All produced partition groups of one schedulingPipelinedRegion. */
    private final Map<SchedulingPipelinedRegion, Set<ConsumedPartitionGroup>>
            producedPartitionGroupsOfRegion = new IdentityHashMap<>();

    /** The ConsumedPartitionGroups which are produced by multiple regions. */
    /** 由多个区域生成的ConsumerdPartitionGroups */
    private final Set<ConsumedPartitionGroup> crossRegionConsumedPartitionGroups =
            Collections.newSetFromMap(new IdentityHashMap<>());

    private final Set<SchedulingPipelinedRegion> scheduledRegions =
            Collections.newSetFromMap(new IdentityHashMap<>());

    public PipelinedRegionSchedulingStrategy(
            final SchedulerOperations schedulerOperations,
            final SchedulingTopology schedulingTopology) {

        this.schedulerOperations = checkNotNull(schedulerOperations);
        this.schedulingTopology = checkNotNull(schedulingTopology);

        init();
    }

    private void init() {

        initCrossRegionConsumedPartitionGroups();

        initPartitionGroupConsumerRegions();

        initProducedPartitionGroupsOfRegion();

        for (SchedulingExecutionVertex vertex : schedulingTopology.getVertices()) {
            final SchedulingPipelinedRegion region =
                    schedulingTopology.getPipelinedRegionOfVertex(vertex.getId());
            regionVerticesSorted
                    .computeIfAbsent(region, r -> new ArrayList<>())
                    .add(vertex.getId());
        }
    }

    private void initProducedPartitionGroupsOfRegion() {
        for (SchedulingPipelinedRegion region : schedulingTopology.getAllPipelinedRegions()) {
            Set<ConsumedPartitionGroup> producedPartitionGroupsSetOfRegion = new HashSet<>();
            for (SchedulingExecutionVertex executionVertex : region.getVertices()) {
                producedPartitionGroupsSetOfRegion.addAll(
                        IterableUtils.toStream(executionVertex.getProducedResults())
                                .flatMap(
                                        partition ->
                                                partition.getConsumedPartitionGroups().stream())
                                .collect(Collectors.toSet()));
            }
            producedPartitionGroupsOfRegion.put(region, producedPartitionGroupsSetOfRegion);
        }
    }

    private void initCrossRegionConsumedPartitionGroups() {
        final Map<ConsumedPartitionGroup, Set<SchedulingPipelinedRegion>>
                producerRegionsByConsumedPartitionGroup = new IdentityHashMap<>();

        for (SchedulingPipelinedRegion pipelinedRegion :
                schedulingTopology.getAllPipelinedRegions()) {
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    pipelinedRegion.getAllNonPipelinedConsumedPartitionGroups()) {
                producerRegionsByConsumedPartitionGroup.computeIfAbsent(
                        consumedPartitionGroup, this::getProducerRegionsForConsumedPartitionGroup);
            }
        }

        for (SchedulingPipelinedRegion pipelinedRegion :
                schedulingTopology.getAllPipelinedRegions()) {
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    pipelinedRegion.getAllNonPipelinedConsumedPartitionGroups()) {
                final Set<SchedulingPipelinedRegion> producerRegions =
                        producerRegionsByConsumedPartitionGroup.get(consumedPartitionGroup);
                if (producerRegions.size() > 1 && producerRegions.contains(pipelinedRegion)) {
                    crossRegionConsumedPartitionGroups.add(consumedPartitionGroup);
                }
            }
        }
    }

    private Set<SchedulingPipelinedRegion> getProducerRegionsForConsumedPartitionGroup(
            ConsumedPartitionGroup consumedPartitionGroup) {
        final Set<SchedulingPipelinedRegion> producerRegions =
                Collections.newSetFromMap(new IdentityHashMap<>());
        for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
            producerRegions.add(getProducerRegion(partitionId));
        }
        return producerRegions;
    }

    private SchedulingPipelinedRegion getProducerRegion(IntermediateResultPartitionID partitionId) {
        return schedulingTopology.getPipelinedRegionOfVertex(
                schedulingTopology.getResultPartition(partitionId).getProducer().getId());
    }

    private void initPartitionGroupConsumerRegions() {
        for (SchedulingPipelinedRegion region : schedulingTopology.getAllPipelinedRegions()) {
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    region.getAllNonPipelinedConsumedPartitionGroups()) {
                if (crossRegionConsumedPartitionGroups.contains(consumedPartitionGroup)
                        || isExternalConsumedPartitionGroup(consumedPartitionGroup, region)) {
                    partitionGroupConsumerRegions
                            .computeIfAbsent(consumedPartitionGroup, group -> new HashSet<>())
                            .add(region);
                }
            }
        }
    }

    private Set<SchedulingPipelinedRegion> getBlockingDownstreamRegionsOfVertex(
            SchedulingExecutionVertex executionVertex) {
        return IterableUtils.toStream(executionVertex.getProducedResults())
                .filter(partition -> !partition.getResultType().canBePipelinedConsumed())
                .flatMap(partition -> partition.getConsumedPartitionGroups().stream())
                .filter(
                        group ->
                                crossRegionConsumedPartitionGroups.contains(group)
                                        || group.areAllPartitionsFinished())
                .flatMap(
                        partitionGroup ->
                                partitionGroupConsumerRegions
                                        .getOrDefault(partitionGroup, Collections.emptySet())
                                        .stream())
                .collect(Collectors.toSet());
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 实时的程序会通过PipelinedRegionSchedulingStrategy策略调用
    */
    @Override
    public void startScheduling() {
        /** 获取DefaultSchedulingPipelinedRegion ExecutionGraph执行拓扑图 */
        final Set<SchedulingPipelinedRegion> sourceRegions =
                IterableUtils.toStream(schedulingTopology.getAllPipelinedRegions())
                        /** 上下游消费类型不是 MUST_BE_PIPELINED(下游必须在上游运行时消耗)*/
                        .filter(this::isSourceRegion)
                        .collect(Collectors.toSet());
        /** 调度Regions */
        maybeScheduleRegions(sourceRegions);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 上下游消费类型不是 MUST_BE_PIPELINED(下游必须在上游运行时消耗)
    */
    private boolean isSourceRegion(SchedulingPipelinedRegion region) {
        /** 获取`region`对象所有不是Pipelined的被消耗分区组 */
        for (ConsumedPartitionGroup consumedPartitionGroup :
                region.getAllNonPipelinedConsumedPartitionGroups()) {
            if (crossRegionConsumedPartitionGroups.contains(consumedPartitionGroup)
                    || isExternalConsumedPartitionGroup(consumedPartitionGroup, region)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void restartTasks(final Set<ExecutionVertexID> verticesToRestart) {
        final Set<SchedulingPipelinedRegion> regionsToRestart =
                verticesToRestart.stream()
                        .map(schedulingTopology::getPipelinedRegionOfVertex)
                        .collect(Collectors.toSet());
        scheduledRegions.removeAll(regionsToRestart);
        maybeScheduleRegions(regionsToRestart);
    }

    @Override
    public void onExecutionStateChange(
            final ExecutionVertexID executionVertexId, final ExecutionState executionState) {
        if (executionState == ExecutionState.FINISHED) {
            maybeScheduleRegions(
                    getBlockingDownstreamRegionsOfVertex(
                            schedulingTopology.getVertex(executionVertexId)));
        }
    }

    @Override
    public void onPartitionConsumable(final IntermediateResultPartitionID resultPartitionId) {}
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 判断SchedulingPipelinedRegion是否是可运行状态
     * 对拓扑顺序排序。触发scheduleRegion调度region
    */
    private void maybeScheduleRegions(final Set<SchedulingPipelinedRegion> regions) {
        // 创建一个新的HashSet来存储需要被调度的Regions
        final Set<SchedulingPipelinedRegion> regionsToSchedule = new HashSet<>();
        // 初始化nextRegions为传入的regions，用于迭代处理
        Set<SchedulingPipelinedRegion> nextRegions = regions;
        // 当还有未处理的Region时，继续循环
        while (!nextRegions.isEmpty()) {
            nextRegions = addSchedulableAndGetNextRegions(nextRegions, regionsToSchedule);
        }
        // schedule regions in topological order.
        // 按照拓扑顺序对regionsToSchedule中的区域进行排序
        SchedulingStrategyUtils.sortPipelinedRegionsInTopologicalOrder(
                        schedulingTopology, regionsToSchedule)
                // 然后对排序后的每个区域调用scheduleRegion方法进行调度
                .forEach(this::scheduleRegion);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 此方法用于添加可调度区域并获取下一个待调度的区域
    */
    private Set<SchedulingPipelinedRegion> addSchedulableAndGetNextRegions(
            Set<SchedulingPipelinedRegion> currentRegions,
            Set<SchedulingPipelinedRegion> regionsToSchedule) {
        // 创建一个新的集合，用于存放下一个待调度的区域
        Set<SchedulingPipelinedRegion> nextRegions = new HashSet<>();
        // 创建一个缓存，用于存储已消费的分区组的可消费状态，避免重复计算
        final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache = new HashMap<>();
        // 创建一个集合，用于存储已经访问过的已消费的分区组，避免重复处理
        final Set<ConsumedPartitionGroup> visitedConsumedPartitionGroups = new HashSet<>();

        // 遍历当前待处理的区域集合
        for (SchedulingPipelinedRegion currentRegion : currentRegions) {
            // 判断当前区域是否可调度
            if (isRegionSchedulable(currentRegion, consumableStatusCache, regionsToSchedule)) {
                // 如果可调度，则将其加入待调度的区域集合
                regionsToSchedule.add(currentRegion);
                // 获取当前Region产生的分区组
                producedPartitionGroupsOfRegion
                        .getOrDefault(currentRegion, Collections.emptySet())
                        // 对每个产生的分区组进行处理
                        .forEach(
                                (producedPartitionGroup) -> {
                                    // 如果该分区组不支持如果该分区的上下游同时支持调度，则跳过
                                    if (!producedPartitionGroup
                                            .getResultPartitionType()
                                            .canBePipelinedConsumed()) {
                                        return;
                                    }
                                    // If this group has been visited, there is no need
                                    // to repeat the determination.
                                    // 如果该分区组已被访问过，则无需重复判断
                                    if (visitedConsumedPartitionGroups.contains(
                                            producedPartitionGroup)) {
                                        return;
                                    }
                                    // 将该分区组标记为已访问
                                    visitedConsumedPartitionGroups.add(producedPartitionGroup);
                                    // 获取能够消费该分区组的区域，并将它们添加到下一个待调度的区域集合中
                                    nextRegions.addAll(
                                            partitionGroupConsumerRegions.getOrDefault(
                                                    producedPartitionGroup,
                                                    Collections.emptySet()));
                                });
            }
        }
        // 返回下一个待调度的区域集合
        return nextRegions;
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 判断SchedulingPipelinedRegion是否是可运行状态
    */
    private boolean isRegionSchedulable(
            final SchedulingPipelinedRegion region,
            final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache,
            final Set<SchedulingPipelinedRegion> regionToSchedule) {
        return !regionToSchedule.contains(region)
                && !scheduledRegions.contains(region)
                && areRegionInputsAllConsumable(region, consumableStatusCache, regionToSchedule);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 调度Region
    */
    private void scheduleRegion(final SchedulingPipelinedRegion region) {
        /** 前置检查 判断SchedulingExecutionVertex是否等于 ExecutionState.CREATED*/
        checkState(
                areRegionVerticesAllInCreatedState(region),
                "BUG: trying to schedule a region which is not in CREATED state");
        /** 添加到  Set<SchedulingPipelinedRegion> scheduledRegions*/
        scheduledRegions.add(region);
        /**
         * SchedulerOperations allocateSlotsAndDeploy
         * 申请资源并部署
         */
        schedulerOperations.allocateSlotsAndDeploy(regionVerticesSorted.get(region));
    }

    private boolean areRegionInputsAllConsumable(
            final SchedulingPipelinedRegion region,
            final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache,
            final Set<SchedulingPipelinedRegion> regionToSchedule) {
        for (ConsumedPartitionGroup consumedPartitionGroup :
                region.getAllNonPipelinedConsumedPartitionGroups()) {
            if (crossRegionConsumedPartitionGroups.contains(consumedPartitionGroup)) {
                if (!isDownstreamOfCrossRegionConsumedPartitionSchedulable(
                        consumedPartitionGroup, region, regionToSchedule)) {
                    return false;
                }
            } else if (isExternalConsumedPartitionGroup(consumedPartitionGroup, region)) {
                if (!consumableStatusCache.computeIfAbsent(
                        consumedPartitionGroup,
                        (group) ->
                                isDownstreamConsumedPartitionGroupSchedulable(
                                        group, regionToSchedule))) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isDownstreamConsumedPartitionGroupSchedulable(
            final ConsumedPartitionGroup consumedPartitionGroup,
            final Set<SchedulingPipelinedRegion> regionToSchedule) {
        if (consumedPartitionGroup.getResultPartitionType().canBePipelinedConsumed()) {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                SchedulingPipelinedRegion producerRegion = getProducerRegion(partitionId);
                if (!scheduledRegions.contains(producerRegion)
                        && !regionToSchedule.contains(producerRegion)) {
                    return false;
                }
            }
        } else {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                if (schedulingTopology.getResultPartition(partitionId).getState()
                        != ResultPartitionState.ALL_DATA_PRODUCED) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isDownstreamOfCrossRegionConsumedPartitionSchedulable(
            final ConsumedPartitionGroup consumedPartitionGroup,
            final SchedulingPipelinedRegion pipelinedRegion,
            final Set<SchedulingPipelinedRegion> regionToSchedule) {
        if (consumedPartitionGroup.getResultPartitionType().canBePipelinedConsumed()) {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                if (isExternalConsumedPartition(partitionId, pipelinedRegion)) {
                    SchedulingPipelinedRegion producerRegion = getProducerRegion(partitionId);
                    if (!regionToSchedule.contains(producerRegion)
                            && !scheduledRegions.contains(producerRegion)) {
                        return false;
                    }
                }
            }
        } else {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                if (isExternalConsumedPartition(partitionId, pipelinedRegion)
                        && schedulingTopology.getResultPartition(partitionId).getState()
                                != ResultPartitionState.ALL_DATA_PRODUCED) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean areRegionVerticesAllInCreatedState(final SchedulingPipelinedRegion region) {
        for (SchedulingExecutionVertex vertex : region.getVertices()) {
            if (vertex.getState() != ExecutionState.CREATED) {
                return false;
            }
        }
        return true;
    }

    private boolean isExternalConsumedPartitionGroup(
            ConsumedPartitionGroup consumedPartitionGroup,
            SchedulingPipelinedRegion pipelinedRegion) {

        return isExternalConsumedPartition(consumedPartitionGroup.getFirst(), pipelinedRegion);
    }

    private boolean isExternalConsumedPartition(
            IntermediateResultPartitionID partitionId, SchedulingPipelinedRegion pipelinedRegion) {
        return !pipelinedRegion.contains(
                schedulingTopology.getResultPartition(partitionId).getProducer().getId());
    }

    @VisibleForTesting
    Set<ConsumedPartitionGroup> getCrossRegionConsumedPartitionGroups() {
        return Collections.unmodifiableSet(crossRegionConsumedPartitionGroups);
    }

    /** The factory for creating {@link PipelinedRegionSchedulingStrategy}. */
    public static class Factory implements SchedulingStrategyFactory {
        @Override
        public SchedulingStrategy createInstance(
                final SchedulerOperations schedulerOperations,
                final SchedulingTopology schedulingTopology) {
            return new PipelinedRegionSchedulingStrategy(schedulerOperations, schedulingTopology);
        }
    }
}
