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
 * limitations under the License
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** Utilities for building {@link EdgeManager}. */
public class EdgeManagerBuildUtil {

    /**
     * Calculate the connections between {@link ExecutionJobVertex} and {@link IntermediateResult} *
     * based on the {@link DistributionPattern}.
     *
     * @param vertex the downstream consumer {@link ExecutionJobVertex}
     * @param intermediateResult the upstream consumed {@link IntermediateResult}
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 基于  DistributionPattern 计算  ExecutionJobVertex和  IntermediateResult之间的连接。
    */
    static void connectVertexToResult(
            ExecutionJobVertex vertex, IntermediateResult intermediateResult) {
        /** 从intermediateResult中获取分发模式。分发模式定义了数据如何在不同的任务之间分发和传输。 */
        final DistributionPattern distributionPattern =
                intermediateResult.getConsumingDistributionPattern();
        /**
         * 从任务顶点的图（vertex.getGraph()）中获取关于此顶点对中间结果输入的信息。
         * 包括如何读取或处理这个中间结果。
         */
        final JobVertexInputInfo jobVertexInputInfo =
                vertex.getGraph()
                        .getJobVertexInputInfo(vertex.getJobVertexId(), intermediateResult.getId());

        switch (distributionPattern) {
            /**
             * 如果分发模式是POINTWISE，则调用connectPointwise方法来进行点对点的连接。
             * 这通常意味着每个任务顶点会接收到特定数量的中间结果，通常是与任务顶点数量相同的中间结果。
             */
            case POINTWISE:
                connectPointwise(vertex, intermediateResult, jobVertexInputInfo);
                break;
            /**
             * 如果分发模式是ALL_TO_ALL，则调用connectAllToAll方法来进行全对全的连接。
             * 这通常意味着每个任务顶点都会接收到所有中间结果，每个中间结果也会被所有任务顶点接收。
             */
            case ALL_TO_ALL:
                connectAllToAll(vertex, intermediateResult, jobVertexInputInfo);
                break;
            /**
             * 如果分发模式不是POINTWISE或ALL_TO_ALL，则抛出IllegalArgumentException，表示不识别该分发模式。
             */
            default:
                throw new IllegalArgumentException("Unrecognized distribution pattern.");
        }
    }

    /**
     * Given parallelisms of two job vertices, compute the max number of edges connected to a target
     * execution vertex from the source execution vertices. Note that edge is considered undirected
     * here. It can be an edge connected from an upstream job vertex to a downstream job vertex, or
     * in a reversed way.
     *
     * @param targetParallelism parallelism of the target job vertex.
     * @param sourceParallelism parallelism of the source job vertex.
     * @param distributionPattern the {@link DistributionPattern} of the connecting edge.
     */
    public static int computeMaxEdgesToTargetExecutionVertex(
            int targetParallelism, int sourceParallelism, DistributionPattern distributionPattern) {
        switch (distributionPattern) {
            case POINTWISE:
                return (sourceParallelism + targetParallelism - 1) / targetParallelism;
            case ALL_TO_ALL:
                return sourceParallelism;
            default:
                throw new IllegalArgumentException("Unrecognized distribution pattern.");
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 设置ALL_TO_ALL分发模式下顶点和边的链接
     */
    private static void connectAllToAll(
            ExecutionJobVertex jobVertex,
            IntermediateResult result,
            JobVertexInputInfo jobVertexInputInfo) {
        // check the vertex input info is legal
        jobVertexInputInfo
                .getExecutionVertexInputInfos()
                .forEach(
                        executionVertexInputInfo -> {
                            IndexRange partitionRange =
                                    executionVertexInputInfo.getPartitionIndexRange();
                            checkArgument(partitionRange.getStartIndex() == 0);
                            checkArgument(
                                    partitionRange.getEndIndex()
                                            == (result.getNumberOfAssignedPartitions() - 1));
                        });

        connectInternal(
                Arrays.asList(jobVertex.getTaskVertices()),
                Arrays.asList(result.getPartitions()),
                result.getResultType(),
                jobVertex.getGraph().getEdgeManager());
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 设置POINTWISE分发模式下顶点和边的链接
    */
    private static void connectPointwise(
            ExecutionJobVertex jobVertex,
            IntermediateResult result,
            JobVertexInputInfo jobVertexInputInfo) {
        /**
         * 初始化一个LinkedHashMap，键是IndexRange（索引范围），值是List<Integer>（消费者索引列表）。
         * 这个映射用来记录每个索引范围（即分区）对应的消费者（子任务）索引。
         */
        Map<IndexRange, List<Integer>> consumersByPartition = new LinkedHashMap<>();
        /**
         * 遍历任务顶点的所有执行顶点输入信息，
         */
        for (ExecutionVertexInputInfo executionVertexInputInfo :
                jobVertexInputInfo.getExecutionVertexInputInfos()) {
            /** 提取每个执行顶点的子任务索引  */
            int consumerIndex = executionVertexInputInfo.getSubtaskIndex();
            /** 分区索引范围 */
            IndexRange range = executionVertexInputInfo.getPartitionIndexRange();
            /**
             * 并将子任务索引添加到对应分区索引范围的消费者列表中。
             */
            consumersByPartition.compute(
                    range,
                    (ignore, consumers) -> {
                        if (consumers == null) {
                            consumers = new ArrayList<>();
                        }
                        consumers.add(consumerIndex);
                        return consumers;
                    });
        }

        consumersByPartition.forEach(
                (range, subtasks) -> {
                    List<ExecutionVertex> taskVertices = new ArrayList<>();
                    List<IntermediateResultPartition> partitions = new ArrayList<>();
                    for (int index : subtasks) {
                        taskVertices.add(jobVertex.getTaskVertices()[index]);
                    }
                    for (int i = range.getStartIndex(); i <= range.getEndIndex(); ++i) {
                        partitions.add(result.getPartitions()[i]);
                    }
                    connectInternal(
                            taskVertices,
                            partitions,
                            result.getResultType(),
                            jobVertex.getGraph().getEdgeManager());
                });
    }

    /** Connect all execution vertices to all partitions. */
    private static void connectInternal(
            List<ExecutionVertex> taskVertices,
            List<IntermediateResultPartition> partitions,
            ResultPartitionType resultPartitionType,
            EdgeManager edgeManager) {
        checkState(!taskVertices.isEmpty());
        checkState(!partitions.isEmpty());

        ConsumedPartitionGroup consumedPartitionGroup =
                createAndRegisterConsumedPartitionGroupToEdgeManager(
                        taskVertices.size(), partitions, resultPartitionType, edgeManager);
        for (ExecutionVertex ev : taskVertices) {
            ev.addConsumedPartitionGroup(consumedPartitionGroup);
        }

        List<ExecutionVertexID> consumerVertices =
                taskVertices.stream().map(ExecutionVertex::getID).collect(Collectors.toList());
        ConsumerVertexGroup consumerVertexGroup =
                ConsumerVertexGroup.fromMultipleVertices(consumerVertices, resultPartitionType);
        for (IntermediateResultPartition partition : partitions) {
            partition.addConsumers(consumerVertexGroup);
        }

        consumedPartitionGroup.setConsumerVertexGroup(consumerVertexGroup);
        consumerVertexGroup.setConsumedPartitionGroup(consumedPartitionGroup);
    }

    private static ConsumedPartitionGroup createAndRegisterConsumedPartitionGroupToEdgeManager(
            int numConsumers,
            List<IntermediateResultPartition> partitions,
            ResultPartitionType resultPartitionType,
            EdgeManager edgeManager) {
        List<IntermediateResultPartitionID> partitionIds =
                partitions.stream()
                        .map(IntermediateResultPartition::getPartitionId)
                        .collect(Collectors.toList());
        ConsumedPartitionGroup consumedPartitionGroup =
                ConsumedPartitionGroup.fromMultiplePartitions(
                        numConsumers, partitionIds, resultPartitionType);
        finishAllDataProducedPartitions(partitions, consumedPartitionGroup);
        edgeManager.registerConsumedPartitionGroup(consumedPartitionGroup);
        return consumedPartitionGroup;
    }

    private static void finishAllDataProducedPartitions(
            List<IntermediateResultPartition> partitions,
            ConsumedPartitionGroup consumedPartitionGroup) {
        for (IntermediateResultPartition partition : partitions) {
            // this is for dynamic graph as consumedPartitionGroup has not been created when the
            // partition becomes finished.
            if (partition.hasDataAllProduced()) {
                consumedPartitionGroup.partitionFinished();
            }
        }
    }
}
