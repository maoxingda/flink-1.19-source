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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.EdgeManagerBuildUtil;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.TaskInputsOutputsDescriptor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Util to analyze inputs & outputs of {@link ExecutionJobVertex} and calculate network memory
 * requirement for slot sharing group (SSG).
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 分析 ExecutionJobVertex 的输入和输出，并计算插槽共享组（SSG）的网络内存需求。
*/
public class SsgNetworkMemoryCalculationUtils {

    /**
     * Calculates network memory requirement of {@link ExecutionJobVertex} and update {@link
     * ResourceProfile} of corresponding slot sharing group.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 计算ExecutionJobVertex的网络内存需求，并更新相应插槽共享组的ResourceProfile。
    */
    public static void enrichNetworkMemory(
            SlotSharingGroup ssg,
            Function<JobVertexID, ExecutionJobVertex> ejvs,
            ShuffleMaster<?> shuffleMaster) {
        /** 获取原始资源配置 */
        ResourceProfile original = ssg.getResourceProfile();

        // Updating network memory for UNKNOWN is also beneficial, but currently it's not
        // supported and the enriching logic only works for 'fine-grained resource management'.
        /**
         * 如果原始资源配置为 UNKNOWN 或其网络内存不为零，则方法直接返回，不进行任何操作。
         */
        if (original.equals(ResourceProfile.UNKNOWN)
                || !original.getNetworkMemory().equals(MemorySize.ZERO)) {
            return;
        }
        /** 初始化一个 networkMemory 变量，用于存储计算得到的总网络内存大小。 */
        MemorySize networkMemory = MemorySize.ZERO;
        /** 遍历 ssg 中的所有作业顶点 ID */
        for (JobVertexID jvId : ssg.getJobVertexIds()) {
            /** 使用 ejvs 函数获取对应的 ExecutionJobVertex */
            ExecutionJobVertex ejv = ejvs.apply(jvId);
            /**
             * 构建TaskInputsOutputsDescriptor
             */
            TaskInputsOutputsDescriptor desc = buildTaskInputsOutputsDescriptor(ejv, ejvs);
            /**
             * 使用 shuffleMaster 的 computeShuffleMemorySizeForTask 方法计算每个任务所需的 Shuffle 内存大小，
             * 并累加到 networkMemory 变量中。
             */
            MemorySize requiredNetworkMemory = shuffleMaster.computeShuffleMemorySizeForTask(desc);
            networkMemory = networkMemory.add(requiredNetworkMemory);
        }
        /**
         * 使用 ResourceProfile.newBuilder 方法构建一个新的 ResourceProfile 对象，
         * 该对象具有与原始配置相同的 CPU 核心数、任务堆内存、任务非堆内存、管理内存和扩展资源，
         * 但网络内存被设置为之前计算得到的 networkMemory。
         * 然后，使用 setResourceProfile 方法将这个新的资源配置设置到 ssg 中。
         */
        ResourceProfile enriched =
                ResourceProfile.newBuilder()
                        .setCpuCores(original.getCpuCores())
                        .setTaskHeapMemory(original.getTaskHeapMemory())
                        .setTaskOffHeapMemory(original.getTaskOffHeapMemory())
                        .setManagedMemory(original.getManagedMemory())
                        .setNetworkMemory(networkMemory)
                        .setExtendedResources(original.getExtendedResources().values())
                        .build();
        ssg.setResourceProfile(enriched);
    }

    private static TaskInputsOutputsDescriptor buildTaskInputsOutputsDescriptor(
            ExecutionJobVertex ejv, Function<JobVertexID, ExecutionJobVertex> ejvs) {

        Map<IntermediateDataSetID, Integer> partitionReuseCount = getPartitionReuseCount(ejv);
        Map<IntermediateDataSetID, Integer> maxInputChannelNums = new HashMap<>();
        Map<IntermediateDataSetID, Integer> maxSubpartitionNums = new HashMap<>();
        Map<IntermediateDataSetID, ResultPartitionType> inputPartitionTypes = new HashMap<>();
        Map<IntermediateDataSetID, ResultPartitionType> partitionTypes = new HashMap<>();

        if (ejv.getGraph().isDynamic()) {
            getMaxInputChannelInfoForDynamicGraph(ejv, maxInputChannelNums, inputPartitionTypes);
            getMaxSubpartitionInfoForDynamicGraph(ejv, maxSubpartitionNums, partitionTypes);
        } else {
            getMaxInputChannelInfo(ejv, maxInputChannelNums, inputPartitionTypes);
            getMaxSubpartitionInfo(ejv, maxSubpartitionNums, partitionTypes, ejvs);
        }

        JobVertex jv = ejv.getJobVertex();

        return TaskInputsOutputsDescriptor.from(
                jv.getNumberOfInputs(),
                maxInputChannelNums,
                partitionReuseCount,
                maxSubpartitionNums,
                inputPartitionTypes,
                partitionTypes);
    }

    private static Map<IntermediateDataSetID, Integer> getPartitionReuseCount(
            ExecutionJobVertex ejv) {
        Map<IntermediateDataSetID, Integer> partitionReuseCount = new HashMap<>();
        for (IntermediateResult intermediateResult : ejv.getInputs()) {
            partitionReuseCount.merge(intermediateResult.getId(), 1, Integer::sum);
        }
        return partitionReuseCount;
    }

    private static void getMaxInputChannelInfo(
            ExecutionJobVertex ejv,
            Map<IntermediateDataSetID, Integer> maxInputChannelNums,
            Map<IntermediateDataSetID, ResultPartitionType> inputPartitionTypes) {

        List<JobEdge> inputEdges = ejv.getJobVertex().getInputs();

        for (int i = 0; i < inputEdges.size(); i++) {
            JobEdge inputEdge = inputEdges.get(i);
            IntermediateResult consumedResult = ejv.getInputs().get(i);

            // the inputs order should match in JobGraph and ExecutionGraph
            checkState(consumedResult.getId().equals(inputEdge.getSourceId()));

            int maxNum =
                    EdgeManagerBuildUtil.computeMaxEdgesToTargetExecutionVertex(
                            ejv.getParallelism(),
                            consumedResult.getNumberOfAssignedPartitions(),
                            inputEdge.getDistributionPattern());
            maxInputChannelNums.put(consumedResult.getId(), maxNum);
            inputPartitionTypes.putIfAbsent(consumedResult.getId(), consumedResult.getResultType());
        }
    }

    private static void getMaxSubpartitionInfo(
            ExecutionJobVertex ejv,
            Map<IntermediateDataSetID, Integer> maxSubpartitionNums,
            Map<IntermediateDataSetID, ResultPartitionType> partitionTypes,
            Function<JobVertexID, ExecutionJobVertex> ejvs) {
        List<IntermediateDataSet> producedDataSets = ejv.getJobVertex().getProducedDataSets();

        checkState(!ejv.getGraph().isDynamic(), "Only support non-dynamic graph.");
        for (IntermediateDataSet producedDataSet : producedDataSets) {
            int maxNum = 0;
            List<JobEdge> outputEdges = producedDataSet.getConsumers();

            if (!outputEdges.isEmpty()) {
                // for non-dynamic graph, the consumer vertices' parallelisms and distribution
                // patterns must be the same
                JobEdge outputEdge = outputEdges.get(0);
                ExecutionJobVertex consumerJobVertex = ejvs.apply(outputEdge.getTarget().getID());
                maxNum =
                        EdgeManagerBuildUtil.computeMaxEdgesToTargetExecutionVertex(
                                ejv.getParallelism(),
                                consumerJobVertex.getParallelism(),
                                outputEdge.getDistributionPattern());
            }
            maxSubpartitionNums.put(producedDataSet.getId(), maxNum);
            partitionTypes.putIfAbsent(producedDataSet.getId(), producedDataSet.getResultType());
        }
    }

    @VisibleForTesting
    static void getMaxInputChannelInfoForDynamicGraph(
            ExecutionJobVertex ejv,
            Map<IntermediateDataSetID, Integer> maxInputChannelNums,
            Map<IntermediateDataSetID, ResultPartitionType> inputPartitionTypes) {

        for (ExecutionVertex vertex : ejv.getTaskVertices()) {
            for (ConsumedPartitionGroup partitionGroup : vertex.getAllConsumedPartitionGroups()) {

                IntermediateResultPartition resultPartition =
                        ejv.getGraph().getResultPartitionOrThrow((partitionGroup.getFirst()));
                IndexRange subpartitionIndexRange =
                        vertex.getExecutionVertexInputInfo(
                                        resultPartition.getIntermediateResult().getId())
                                .getSubpartitionIndexRange();

                maxInputChannelNums.merge(
                        partitionGroup.getIntermediateDataSetID(),
                        subpartitionIndexRange.size() * partitionGroup.size(),
                        Integer::max);
                inputPartitionTypes.putIfAbsent(
                        partitionGroup.getIntermediateDataSetID(),
                        partitionGroup.getResultPartitionType());
            }
        }
    }

    private static void getMaxSubpartitionInfoForDynamicGraph(
            ExecutionJobVertex ejv,
            Map<IntermediateDataSetID, Integer> maxSubpartitionNums,
            Map<IntermediateDataSetID, ResultPartitionType> partitionTypes) {

        for (IntermediateResult intermediateResult : ejv.getProducedDataSets()) {
            final int maxNum =
                    Arrays.stream(intermediateResult.getPartitions())
                            .map(IntermediateResultPartition::getNumberOfSubpartitions)
                            .reduce(0, Integer::max);
            maxSubpartitionNums.put(intermediateResult.getId(), maxNum);
            partitionTypes.putIfAbsent(
                    intermediateResult.getId(), intermediateResult.getResultType());
        }
    }

    /** Private default constructor to avoid being instantiated. */
    private SsgNetworkMemoryCalculationUtils() {}
}
