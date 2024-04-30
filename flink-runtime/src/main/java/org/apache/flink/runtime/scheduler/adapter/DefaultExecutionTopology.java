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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.EdgeManager;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.failover.SchedulingPipelinedRegionComputeUtil;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalPipelinedRegion;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalTopology;
import org.apache.flink.runtime.jobgraph.topology.LogicalEdge;
import org.apache.flink.runtime.jobgraph.topology.LogicalVertex;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.scheduler.SchedulingTopologyListener;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.util.IterableUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Adapter of {@link ExecutionGraph} to {@link SchedulingTopology}. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 *  ExecutionGraph到 SchedulingTopology 的适配器。
*/
public class DefaultExecutionTopology implements SchedulingTopology {
    /** 构建日志对象 */
    private static final Logger LOG = LoggerFactory.getLogger(DefaultExecutionTopology.class);

    private final Map<ExecutionVertexID, DefaultExecutionVertex> executionVerticesById;

    private final List<DefaultExecutionVertex> executionVerticesList;

    private final Map<IntermediateResultPartitionID, DefaultResultPartition> resultPartitionsById;

    private final Map<ExecutionVertexID, DefaultSchedulingPipelinedRegion> pipelinedRegionsByVertex;

    private final List<DefaultSchedulingPipelinedRegion> pipelinedRegions;

    private final EdgeManager edgeManager;

    private final Supplier<List<ExecutionVertexID>> sortedExecutionVertexIds;

    private final Map<JobVertexID, DefaultLogicalPipelinedRegion>
            logicalPipelinedRegionsByJobVertexId;

    /** Listeners that will be notified whenever the scheduling topology is updated. */
    private final List<SchedulingTopologyListener> schedulingTopologyListeners = new ArrayList<>();

    private DefaultExecutionTopology(
            Supplier<List<ExecutionVertexID>> sortedExecutionVertexIds,
            EdgeManager edgeManager,
            Map<JobVertexID, DefaultLogicalPipelinedRegion> logicalPipelinedRegionsByJobVertexId) {
        this.sortedExecutionVertexIds = checkNotNull(sortedExecutionVertexIds);
        this.edgeManager = checkNotNull(edgeManager);
        this.logicalPipelinedRegionsByJobVertexId =
                checkNotNull(logicalPipelinedRegionsByJobVertexId);

        this.executionVerticesById = new HashMap<>();
        this.executionVerticesList = new ArrayList<>();
        this.resultPartitionsById = new HashMap<>();
        this.pipelinedRegionsByVertex = new HashMap<>();
        this.pipelinedRegions = new ArrayList<>();
    }

    @Override
    public Iterable<DefaultExecutionVertex> getVertices() {
        return Collections.unmodifiableList(executionVerticesList);
    }

    @Override
    public DefaultExecutionVertex getVertex(final ExecutionVertexID executionVertexId) {
        final DefaultExecutionVertex executionVertex = executionVerticesById.get(executionVertexId);
        if (executionVertex == null) {
            throw new IllegalArgumentException("can not find vertex: " + executionVertexId);
        }
        return executionVertex;
    }

    @Override
    public DefaultResultPartition getResultPartition(
            final IntermediateResultPartitionID intermediateResultPartitionId) {
        final DefaultResultPartition resultPartition =
                resultPartitionsById.get(intermediateResultPartitionId);
        if (resultPartition == null) {
            throw new IllegalArgumentException(
                    "can not find partition: " + intermediateResultPartitionId);
        }
        return resultPartition;
    }

    @Override
    public void registerSchedulingTopologyListener(SchedulingTopologyListener listener) {
        checkNotNull(listener);
        schedulingTopologyListeners.add(listener);
    }

    @Override
    public Iterable<DefaultSchedulingPipelinedRegion> getAllPipelinedRegions() {
        checkNotNull(pipelinedRegions);

        return Collections.unmodifiableCollection(pipelinedRegions);
    }

    @Override
    public DefaultSchedulingPipelinedRegion getPipelinedRegionOfVertex(
            final ExecutionVertexID vertexId) {
        checkNotNull(pipelinedRegionsByVertex);

        final DefaultSchedulingPipelinedRegion pipelinedRegion =
                pipelinedRegionsByVertex.get(vertexId);
        if (pipelinedRegion == null) {
            throw new IllegalArgumentException("Unknown execution vertex " + vertexId);
        }
        return pipelinedRegion;
    }

    public EdgeManager getEdgeManager() {
        return edgeManager;
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *
    */
    private static Map<JobVertexID, DefaultLogicalPipelinedRegion>
            computeLogicalPipelinedRegionsByJobVertexId(final ExecutionGraph executionGraph) {
        /**
         * 循环所有的ExecutionJobVertex获取所有的JobVertex
         */
        List<JobVertex> topologicallySortedJobVertices =
                /**
                 *  Iterable<ExecutionJobVertex> getVerticesTopologically();
                 *  获取ExecutionJobVertex
                 */
                IterableUtils.toStream(executionGraph.getVerticesTopologically())
                        /** 通过调用 getJobVertex 方法，获取 JobVertex 对象 */
                        .map(ExecutionJobVertex::getJobVertex)
                        /** 转换成List */
                        .collect(Collectors.toList());
        /**
         * topologicallySortedJobVertices作为参数
         * 构建Iterable<DefaultLogicalPipelinedRegion>
         */
        Iterable<DefaultLogicalPipelinedRegion> logicalPipelinedRegions =
                /** topologicallySortedJobVertices 作为参数直接调用构造方法构造 DefaultLogicalTopology*/
                DefaultLogicalTopology.fromTopologicallySortedJobVertices(
                                topologicallySortedJobVertices)
                        .getAllPipelinedRegions();
        /** 初始化一个空的 HashMap，用于存储 JobVertexID 到 DefaultLogicalPipelinedRegion 的映射 */
        Map<JobVertexID, DefaultLogicalPipelinedRegion> logicalPipelinedRegionsByJobVertexId =
                new HashMap<>();
        /**
         * 填充映射 Map<JobVertexID, DefaultLogicalPipelinedRegion>
         * 双层循环遍历所有的逻辑管道化区域以及每个区域中的逻辑顶点（LogicalVertex）。对于每个逻辑顶点，
         * 它将其 ID（JobVertexID）和所属的管道化区域添加到映射中。注意，这里可能存在多个逻辑顶点映射到同一个管道化区域的情况。
         */
        /** 循环 Iterable<DefaultLogicalPipelinedRegion> */
        for (DefaultLogicalPipelinedRegion logicalPipelinedRegion : logicalPipelinedRegions) {
            /** 循环获取每个 Vertex */
            for (LogicalVertex vertex : logicalPipelinedRegion.getVertices()) {
                /** 将id,DefaultLogicalPipelinedRegion 放入Map*/
                logicalPipelinedRegionsByJobVertexId.put(vertex.getId(), logicalPipelinedRegion);
            }
        }
        /** 返回Map<JobVertexID, DefaultLogicalPipelinedRegion>*/
        return logicalPipelinedRegionsByJobVertexId;
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *
    */
    public void notifyExecutionGraphUpdated(
            final DefaultExecutionGraph executionGraph,
            final List<ExecutionJobVertex> newlyInitializedJobVertices) {
        /** 校验executionGraph 是否为null */
        checkNotNull(executionGraph, "execution graph can not be null");
        /** 从 newlyInitializedJobVertices 中提取每个顶点的ID，并将它们收集到Set集合中。 */
        final Set<JobVertexID> newJobVertexIds =
                newlyInitializedJobVertices.stream()
                        .map(ExecutionJobVertex::getJobVertexId)
                        .collect(Collectors.toSet());

        // any mustBePipelinedConsumed input should be from within this new set so that existing
        // pipelined regions will not change
        /**
         * @授课老师(微信): yi_locus
         * email: 156184212@qq.com
         * 检查新初始化的作业顶点的输入是否满足特定的条件。下游必须在上游运行时消耗。
         * 它们确保任何 mustBePipelinedConsumed
         * 的输入都来自新集合中的顶点，这样现有的流水线区域就不会改变。
        */
        newlyInitializedJobVertices.stream()
                .map(ExecutionJobVertex::getJobVertex)
                .flatMap(v -> v.getInputs().stream())
                .map(JobEdge::getSource)
                .filter(r -> r.getResultType().mustBePipelinedConsumed())
                .map(IntermediateDataSet::getProducer)
                .map(JobVertex::getID)
                .forEach(id -> checkState(newJobVertexIds.contains(id)));
        /**
         * flatMap 将所有并行度顶点放入Iterable<ExecutionVertex>
         * 从每个 ExecutionJobVertex 中提取任务顶点，并将它们收集到一个列表中。
         */
        final Iterable<ExecutionVertex> newExecutionVertices =
                newlyInitializedJobVertices.stream()
                        .flatMap(jobVertex -> Stream.of(jobVertex.getTaskVertices()))
                        .collect(Collectors.toList());
        /** 新的执行顶点，并且生成与它们关联的结果分区。 */
        generateNewExecutionVerticesAndResultPartitions(newExecutionVertices);
        /** 生成新的Pipelined Regions */
        generateNewPipelinedRegions(newExecutionVertices);
        /** 确保同位置的顶点在同一区域 */
        ensureCoLocatedVerticesInSameRegion(pipelinedRegions, executionGraph);
        /** 通知调度拓扑已更新 */
        notifySchedulingTopologyUpdated(newExecutionVertices);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 通知所有注册的SchedulingTopologyListener监听器，调度拓扑已经更新，并传递了新执行顶点的ID列表。
    */
    private void notifySchedulingTopologyUpdated(Iterable<ExecutionVertex> newExecutionVertices) {
        /** 转换执行顶点为ID列表 */
        List<ExecutionVertexID> newVertexIds =
                IterableUtils.toStream(newExecutionVertices)
                        .map(ExecutionVertex::getID)
                        .collect(Collectors.toList());
        /** 遍历schedulingTopologyListeners集合中的每个SchedulingTopologyListener对象 */
        for (SchedulingTopologyListener listener : schedulingTopologyListeners) {
            /** 通知所有监听器 */
            listener.notifySchedulingTopologyUpdated(this, newVertexIds);
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 根据一个 DefaultExecutionGraph 对象来创建一个 DefaultExecutionTopology 对象。
     * DefaultExecutionGraph用于表示 Flink 作业的执行图的类，
     * DefaultExecutionTopology 则用于描述作业执行时的拓扑结构。
    */
    public static DefaultExecutionTopology fromExecutionGraph(
            DefaultExecutionGraph executionGraph) {
        checkNotNull(executionGraph, "execution graph can not be null");
        /** 从 executionGraph 对象中获取 EdgeManager 对象。EdgeManager 负责管理执行图中的边或者顶点之间的上下游关系。 */
        EdgeManager edgeManager = executionGraph.getEdgeManager();
        /**
         * 使用 lambda 表达式、edgeManager 通过 computeLogicalPipelinedRegionsByJobVertexId 方法计算得到的结果来创建
         * 新的 DefaultExecutionTopology 对象。
         */
        DefaultExecutionTopology schedulingTopology =
                new DefaultExecutionTopology(
                        () ->
                                IterableUtils.toStream(executionGraph.getAllExecutionVertices())
                                        .map(ExecutionVertex::getID)
                                        .collect(Collectors.toList()),
                        edgeManager,
                        computeLogicalPipelinedRegionsByJobVertexId(executionGraph));
        /**
         * 调用 schedulingTopology 的 notifyExecutionGraphUpdated 方法，
         * 通知它执行图已更新。传入的第二个参数是一个列表，
         * 该列表包含 executionGraph 中所有已初始化的执行作业顶点（ExecutionJobVertex）。
         */
        schedulingTopology.notifyExecutionGraphUpdated(
                executionGraph,
                IterableUtils.toStream(executionGraph.getVerticesTopologically())
                        .filter(ExecutionJobVertex::isInitialized)
                        .collect(Collectors.toList()));
        /** 返回拓扑结构 */
        return schedulingTopology;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 新的执行顶点，并且生成与它们关联的结果分区。
    */
    private void generateNewExecutionVerticesAndResultPartitions(
            Iterable<ExecutionVertex> newExecutionVertices) {
        /** 遍历新的执行顶点 */
        for (ExecutionVertex vertex : newExecutionVertices) {
            /***
             * 为当前顶点生成结果分区。这个方法需要当前顶点产生的分区以及一个方法引用，
             * 这个方法引用是用来从 edgeManager 获取消费顶点组的。
             * 基于Map<IntermediateResultPartitionID, IntermediateResultPartition> resultPartitions、
             * Map<IntermediateResultPartitionID, List<ConsumerVertexGroup>> partitionConsumers
             * 参数构建出DefaultResultPartition对象
             */
            List<DefaultResultPartition> producedPartitions =
                    generateProducedSchedulingResultPartition(
                            /** 获取vertex 对应的resultPartitions */
                            vertex.getProducedPartitions(),
                            /** 获取上游顶点输出的中间结果被哪个顶点组消费 ConsumerVertexGroup */
                            edgeManager::getConsumerVertexGroupsForPartition);
            /**
             * 将来生成的DefaultResultPartition映射到
             * Map<IntermediateResultPartitionID, DefaultResultPartition> resultPartitionsById 中
             * id顶点分区中对应的IntermediateResultPartitionID(存在分区概念) value=partition
             */
            producedPartitions.forEach(
                    partition -> resultPartitionsById.put(partition.getId(), partition));
            /** 构建新的 DefaultExecutionVertex */
            DefaultExecutionVertex schedulingVertex =
                    generateSchedulingExecutionVertex(
                            vertex,
                            producedPartitions,
                            edgeManager.getConsumedPartitionGroupsForVertex(vertex.getID()),
                            resultPartitionsById::get);
            /**
             * 将DefaultExecutionVertex到Map结构
             * Map<ExecutionVertexID, DefaultExecutionVertex> executionVerticesById;
             *
             */
            executionVerticesById.put(schedulingVertex.getId(), schedulingVertex);
        }
        /**
         * 清空旧的执行顶点列表
         * List<DefaultExecutionVertex> executionVerticesList
         */
        executionVerticesList.clear();
        /** 这个循环遍历sortedExecutionVertexIds列表中的每一个顶点ID。 */
        for (ExecutionVertexID vertexID : sortedExecutionVertexIds.get()) {
            /**
             * 对于每一个ID，它都从executionVerticesById这个Map中检索对应的DefaultExecutionVertex对象，
             * 并将其添加到executionVerticesList列表中。
             */
            executionVerticesList.add(executionVerticesById.get(vertexID));
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 构建DefaultResultPartition
    */
    private static List<DefaultResultPartition> generateProducedSchedulingResultPartition(
            Map<IntermediateResultPartitionID, IntermediateResultPartition>
                    producedIntermediatePartitions,
            Function<IntermediateResultPartitionID, List<ConsumerVertexGroup>>
                    partitionConsumerVertexGroupsRetriever) {
        /**
         * 初始化List<DefaultResultPartition>集合 大小与resultPartition大小一样
         */
        List<DefaultResultPartition> producedSchedulingPartitions =
                new ArrayList<>(producedIntermediatePartitions.size());
        /**
         * 循环构建DefaultResultPartition
         * 核心点
         * Supplier<List<ConsumerVertexGroup>> consumerVertexGroupsSupplier 顶点消费组 哪个顶点进行消费,
         * Supplier<List<ConsumedPartitionGroup>> consumerPartitionGroupSupplier 分区组，写到哪个分组
         */
        producedIntermediatePartitions
                .values()
                .forEach(
                        irp ->
                                producedSchedulingPartitions.add(
                                        new DefaultResultPartition(
                                                irp.getPartitionId(),
                                                irp.getIntermediateResult().getId(),
                                                irp.getResultType(),
                                                () ->
                                                        irp.hasDataAllProduced()
                                                                ? ResultPartitionState
                                                                        .ALL_DATA_PRODUCED
                                                                : ResultPartitionState.CREATED,
                                                () ->
                                                        partitionConsumerVertexGroupsRetriever
                                                                .apply(irp.getPartitionId()),
                                                irp::getConsumedPartitionGroups)));
        /** 返回producedSchedulingPartitions List结合 */
        return producedSchedulingPartitions;
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 构建DefaultExecutionVertex
    */
    private static DefaultExecutionVertex generateSchedulingExecutionVertex(
            ExecutionVertex vertex,
            List<DefaultResultPartition> producedPartitions,
            List<ConsumedPartitionGroup> consumedPartitionGroups,
            Function<IntermediateResultPartitionID, DefaultResultPartition>
                    resultPartitionRetriever) {
        /**
         * 创建一个新的DefaultExecutionVertex对象，命名为schedulingVertex。
         * this.executionVertexId 使用vertex.getID()作为该顶点的ID。
         * this.stateSupplier = checkNotNull(stateSupplier); vertex::getExecutionState作为获取执行状态的方式。
         * this.producedResults = checkNotNull(producedPartitions); 将producedPartitions列表作为该顶点产生的分区。
         * this.consumedPartitionGroups = checkNotNull(consumedPartitionGroups); 将consumedPartitionGroups列表作为该顶点消费的分区组。
         * this.resultPartitionRetriever = checkNotNull(resultPartitionRetriever); 将resultPartitionRetriever作为检索结果分区的函数。
         */
        DefaultExecutionVertex schedulingVertex =
                new DefaultExecutionVertex(
                        vertex.getID(),
                        producedPartitions,
                        vertex::getExecutionState,
                        consumedPartitionGroups,
                        resultPartitionRetriever);
        /**
         * 每一个产生的分区都会设置其生产者为schedulingVertex
         * 每个分区的生产者都是DefaultExecutionVertex
         */
        producedPartitions.forEach(partition -> partition.setProducer(schedulingVertex));
        /** 返回 DefaultExecutionVertex */
        return schedulingVertex;
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 生成新的Pipelined Regions
    */
    private void generateNewPipelinedRegions(Iterable<ExecutionVertex> newExecutionVertices) {
        /** 转换执行顶点为调度执行顶点 */
        final Iterable<DefaultExecutionVertex> newSchedulingExecutionVertices =
                IterableUtils.toStream(newExecutionVertices)
                        .map(ExecutionVertex::getID)
                        .map(executionVerticesById::get)
                        .collect(Collectors.toList());
        /**
         * 首先初始化一个Map
         */
        Map<DefaultLogicalPipelinedRegion, List<DefaultExecutionVertex>>
                sortedExecutionVerticesInPipelinedRegion = new IdentityHashMap<>();
        /** 遍历Iterable<DefaultExecutionVertex>中的每个顶点 */
        for (DefaultExecutionVertex schedulingVertex : newSchedulingExecutionVertices) {
            /**
             * 通过顶点的作业顶点ID（getJobVertexId()）从logicalPipelinedRegionsByJobVertexId中检索对应的DefaultLogicalPipelinedRegion。
             * 如果对应的管道化区域在Map中不存在，则使用computeIfAbsent方法创建一个新的空列表，并将当前顶点添加到该列表中。
             */
            sortedExecutionVerticesInPipelinedRegion
                    .computeIfAbsent(
                            logicalPipelinedRegionsByJobVertexId.get(
                                    schedulingVertex.getId().getJobVertexId()),
                            ignore -> new ArrayList<>())
                    .add(schedulingVertex);
        }
        /** 记录构建DefaultSchedulingPipelinedRegion的开始时间 */
        long buildRegionsStartTime = System.nanoTime();
        /** 初始化SchedulingExecutionVertex集合 */
        Set<Set<SchedulingExecutionVertex>> rawPipelinedRegions =
                Collections.newSetFromMap(new IdentityHashMap<>());

        // A SchedulingPipelinedRegion can be derived from just one LogicalPipelinedRegion.
        // Thus, we can traverse all LogicalPipelinedRegions and convert them into
        // SchedulingPipelinedRegions one by one. The LogicalPipelinedRegions and
        // SchedulingPipelinedRegions are both connected with inter-region blocking edges.
        /**
         * SchedulelingPipelinedRegion只能从一个LogicalPipelinedRegion派生。
         * 因此，我们可以遍历所有LogicalPipelinedRegions，
         * 并将它们逐个转换为SchedulengPipelinedRegios。
         * LogicalPipelinedRegions和SchedulengPipelinedregions都通过区域间阻塞边连接。
         */
        /**
         * 构建SchedulingExecutionVertex
         * 循环LogicalPipelinedRegions 来进行构建
         * 遍历sortedExecutionVerticesInPipelinedRegion的执行顶点
         */
        for (Map.Entry<DefaultLogicalPipelinedRegion, List<DefaultExecutionVertex>> entry :
                sortedExecutionVerticesInPipelinedRegion.entrySet()) {
            /** 获取逻辑PipeLinedRegion */
            DefaultLogicalPipelinedRegion logicalPipelinedRegion = entry.getKey();
            /** 获取逻辑PipelinedRegion对应的顶点*/
            List<DefaultExecutionVertex> schedulingExecutionVertices = entry.getValue();
            /** 检查DefaultLogicalPipelinedRegion是否包含Region内所有到所有边。 */
            if (containsIntraRegionAllToAllEdge(logicalPipelinedRegion)) {
                // For edges inside one LogicalPipelinedRegion, if there is any all-to-all edge, it
                // could be under two circumstances:
                // 对于一个LogicalPipelinedRegion内的边，如果存在任何“全对全”边，则
                //可能在两种情况下：
                // 1. Pipelined all-to-all edge:
                //     Pipelined all-to-all edge will connect all vertices pipelined. Therefore,
                // all execution vertices derived from this LogicalPipelinedRegion should be in one
                // SchedulingPipelinedRegion.
                // 1.管道连接所有到所有边：管道连接所有边缘将连接管道连接的所有顶点。
                // 因此，从该LogicalPipelinedRegion派生的所有执行顶点都应位于一个SchedulelingPipelinedRegion中。
                //
                // 2. Blocking all-to-all edge:
                //     For intra-region blocking all-to-all edge, we must make sure all the vertices
                // are inside one SchedulingPipelinedRegion, so that there will be no deadlock
                // happens during scheduling. For more details about this case, please refer to
                // FLINK-17330 (https://issues.apache.org/jira/browse/FLINK-17330).
                // 2.阻塞所有到所有边：对于区域内阻塞所有到全部边，
                // 我们必须确保所有顶点都在一个SchedulelingPipelinedRegion内，
                // 这样在调度过程中就不会发生死锁。有关此案例的更多详细信息，请参阅
                //
                // Therefore, if a LogicalPipelinedRegion contains any intra-region all-to-all
                // edge, we just convert the entire LogicalPipelinedRegion to a sole
                // SchedulingPipelinedRegion directly.
                //因此，如果LogicalPipelinedRegion包含任何区域内的all-to-all
                //边缘，我们只需将整个LogicalPipelinedRegion转换为唯一
                //直接安排管道注册。
                /** Set<Set<SchedulingExecutionVertex>> rawPipelinedRegions */
                rawPipelinedRegions.add(new HashSet<>(schedulingExecutionVertices));
            } else {
                // If there are only pointwise edges inside the LogicalPipelinedRegion, we can use
                // SchedulingPipelinedRegionComputeUtil to compute the regions with O(N) computation
                // complexity.
                /** 继续进行计算并且最终添加到*/
                /**
                 * 继续进行计算并且最终添加到集合中
                 * Set<Set<SchedulingExecutionVertex>> rawPipelinedRegions
                 */
                rawPipelinedRegions.addAll(
                        SchedulingPipelinedRegionComputeUtil.computePipelinedRegions(
                                schedulingExecutionVertices,
                                executionVerticesById::get,
                                resultPartitionsById::get));
            }
        }
        /**
         * 循环遍历了rawPipelinedRegions集合，其中rawPipelinedRegion是原始管道化区域，
         */
        for (Set<? extends SchedulingExecutionVertex> rawPipelinedRegion : rawPipelinedRegions) {
            //noinspection unchecked
            /** 每个原始管道化区域，代码创建了一个DefaultSchedulingPipelinedRegion对象。 */
            final DefaultSchedulingPipelinedRegion pipelinedRegion =
                    new DefaultSchedulingPipelinedRegion(
                            (Set<DefaultExecutionVertex>) rawPipelinedRegion,
                            resultPartitionsById::get);
            /**
             * 创建的pipelinedRegion对象被添加到pipelinedRegions集合中，以便后续使用或处理。
             */
            pipelinedRegions.add(pipelinedRegion);
            /**
             * 循环遍历原始管道化区域中的每个执行顶点。对于每个执行顶点，
             * 代码将其ID与对应的pipelinedRegion对象关联起来，
             * 并存储在pipelinedRegionsByVertex映射中。
             * 这样，后续可以通过执行顶点的ID快速查找它所属的管道化区域。
             */
            for (SchedulingExecutionVertex executionVertex : rawPipelinedRegion) {
                pipelinedRegionsByVertex.put(executionVertex.getId(), pipelinedRegion);
            }
        }
        /**
         * 代码计算了从buildRegionsStartTime开始到当前时间点构建管道化区域的总耗时（以毫秒为单位）。
         * 这个耗时可能用于性能监控或日志记录，
         */
        long buildRegionsDuration = (System.nanoTime() - buildRegionsStartTime) / 1_000_000;
        LOG.info(
                "Built {} new pipelined regions in {} ms, total {} pipelined regions currently.",
                rawPipelinedRegions.size(),
                buildRegionsDuration,
                pipelinedRegions.size());
    }

    /**
     * Check if the {@link DefaultLogicalPipelinedRegion} contains intra-region all-to-all edges or
     * not.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 检查DefaultLogicalPipelinedRegion是否包含Region内所有到所有边。
    */
    private static boolean containsIntraRegionAllToAllEdge(
            DefaultLogicalPipelinedRegion logicalPipelinedRegion) {
        for (LogicalVertex vertex : logicalPipelinedRegion.getVertices()) {
            for (LogicalEdge inputEdge : vertex.getInputs()) {
                if (inputEdge.getDistributionPattern() == DistributionPattern.ALL_TO_ALL
                        && logicalPipelinedRegion.contains(inputEdge.getProducerVertexId())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Co-location constraints are only used for iteration head and tail. A paired head and tail
     * needs to be in the same pipelined region so that they can be restarted together.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 同位置约束仅用于迭代头和迭代尾。成对的头和尾需要在同一管道区域中，这样它们才能一起重新启动。
    */
    private static void ensureCoLocatedVerticesInSameRegion(
            List<DefaultSchedulingPipelinedRegion> pipelinedRegions,
            ExecutionGraph executionGraph) {
        /**
         * 创建了一个HashMap，键是CoLocationConstraint对象，
         * 值是DefaultSchedulingPipelinedRegion对象。
         * 这个映射将用于跟踪每个约束所关联的管道化区域。
         */
        final Map<CoLocationConstraint, DefaultSchedulingPipelinedRegion> constraintToRegion =
                new HashMap<>();
        /** 该循环遍历pipelinedRegions列表中的每个管道化区域。 */
        for (DefaultSchedulingPipelinedRegion region : pipelinedRegions) {
            /** 每个管道化区域，代码遍历其中的所有执行顶点 */
            for (DefaultExecutionVertex vertex : region.getVertices()) {
                /** 获取执行顶点的位置约束 */
                final CoLocationConstraint constraint =
                        getCoLocationConstraint(vertex.getId(), executionGraph);
                /**
                 * 如果执行顶点具有位置约束，代码会检查这个约束是否已经与某个管道化区域相关联。
                 * 如果已关联且不是当前遍历的region，则checkState会抛出异常，
                 * 因为共位置的任务必须在相同的管道化区域内。
                 */
                if (constraint != null) {
                    final DefaultSchedulingPipelinedRegion regionOfConstraint =
                            constraintToRegion.get(constraint);
                    checkState(
                            regionOfConstraint == null || regionOfConstraint == region,
                            "co-located tasks must be in the same pipelined region");
                    /**
                     * 如果约束没有与任何区域关联或者与当前遍历的区域匹配，
                     * 代码会使用putIfAbsent方法尝试将约束和当前区域关联起来。
                     * 如果约束已经存在，则putIfAbsent不会做任何事情。
                     */
                    constraintToRegion.putIfAbsent(constraint, region);
                }
            }
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 用于获取与特定执行顶点关联的共置约束。
     * 共置约束通常用于在分布式计算环境中确保某些任务（或子任务）运行在同一计算节点上
     * 以提高通信效率或数据局部性。
    */
    private static CoLocationConstraint getCoLocationConstraint(
            ExecutionVertexID executionVertexId, ExecutionGraph executionGraph) {

        CoLocationGroup coLocationGroup =
                /** 判断作业顶点是否为空 */
                Objects.requireNonNull(
                                executionGraph.getJobVertex(executionVertexId.getJobVertexId()))
                        /** 获取 CoLocationGroup*/
                        .getCoLocationGroup();
        /**
         * 检查共置组是否为 null。如果为 null，则返回 null，表示没有共置约束。
         * 否则，调用 coLocationGroup.getLocationConstraint(subtaskIndex) 来获取与给定执行顶点ID的子任务索引关联的共置约束。
         */
        return coLocationGroup == null
                ? null
                : coLocationGroup.getLocationConstraint(executionVertexId.getSubtaskIndex());
    }
}
