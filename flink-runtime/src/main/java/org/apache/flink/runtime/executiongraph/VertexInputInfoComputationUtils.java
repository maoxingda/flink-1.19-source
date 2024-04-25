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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Util to compute {@link JobVertexInputInfo}s for execution job vertex. */
public class VertexInputInfoComputationUtils {

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *
    */
    public static Map<IntermediateDataSetID, JobVertexInputInfo> computeVertexInputInfos(
            ExecutionJobVertex ejv,
            Function<IntermediateDataSetID, IntermediateResult> intermediateResultRetriever)
            throws JobException {
        /** 校验 并行度是否大于0 */
        checkState(ejv.isParallelismDecided());
        /** 创建一个空的ArrayList来存储中间结果的信息 */
        final List<IntermediateResultInfo> intermediateResultInfos = new ArrayList<>();
        /**
         * 循环JobVertex的输入，也就是JobEdge
         */
        for (JobEdge edge : ejv.getJobVertex().getInputs()) {
            /** 得到IntermediateResult */
            IntermediateResult ires = intermediateResultRetriever.apply(edge.getSourceId());
            /** 如果IntermediateResult 为空抛出异常 */
            if (ires == null) {
                throw new JobException(
                        "Cannot connect this job graph to the previous graph. No previous intermediate result found for ID "
                                + edge.getSourceId());
            }
            /** 将intermediateResult封装到IntermediateResultWrapper 添加到集合中 */
            intermediateResultInfos.add(new IntermediateResultWrapper(ires));
        }
        /**
         * 继续转换最终构建为Map<IntermediateDataSetID, JobVertexInputInfo>
         * 参数为并行度，IntermediateResult,是否支持动态设置并行度
         */
        return computeVertexInputInfos(
                ejv.getParallelism(), intermediateResultInfos, ejv.getGraph().isDynamic());
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 基于IntermediateResult构建出Map<IntermediateDataSetID, JobVertexInputInfo>
    */
    public static Map<IntermediateDataSetID, JobVertexInputInfo> computeVertexInputInfos(
            int parallelism,
            List<? extends IntermediateResultInfo> inputs,
            boolean isDynamicGraph) {
        /** 检查并行度 是否 >0 */
        checkArgument(parallelism > 0);
        /**
         * 创建Map<IntermediateDataSetID, JobVertexInputInfo>结构对象
         */
        final Map<IntermediateDataSetID, JobVertexInputInfo> jobVertexInputInfos =
                new LinkedHashMap<>();
        /**
         * 循环IntermediateResultInfo
          */
        for (IntermediateResultInfo input : inputs) {
            /** 获取并行度 */
            int sourceParallelism = input.getNumPartitions();
            /** 如果输入是Pointwise 进入if 否则 else*/
            if (input.isPointwise()) {
                /**
                 * 构建jobVertexInputInfos
                 * input.getResultId() = key
                 */
                jobVertexInputInfos.putIfAbsent(
                        input.getResultId(),
                        computeVertexInputInfoForPointwise(
                                sourceParallelism,
                                parallelism,
                                input::getNumSubpartitions,
                                isDynamicGraph));
            } else {
                jobVertexInputInfos.putIfAbsent(
                        input.getResultId(),
                        computeVertexInputInfoForAllToAll(
                                sourceParallelism,
                                parallelism,
                                input::getNumSubpartitions,
                                isDynamicGraph,
                                input.isBroadcast()));
            }
        }

        return jobVertexInputInfos;
    }

    /**
     * Compute the {@link JobVertexInputInfo} for a {@link DistributionPattern#POINTWISE} edge. This
     * computation algorithm will evenly distribute subpartitions to downstream subtasks according
     * to the number of subpartitions. Different downstream subtasks consume roughly the same number
     * of subpartitions.
     *
     * @param sourceCount the parallelism of upstream
     * @param targetCount the parallelism of downstream
     * @param numOfSubpartitionsRetriever a retriever to get the number of subpartitions
     * @param isDynamicGraph whether is dynamic graph
     * @return the computed {@link JobVertexInputInfo}
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * POINTWISE 边的 对应的 JobVertexInputInfo}。
     * 将根据子分区的数量将子分区均匀地分配给下游的子任务。
     * 不同的下游子任务消耗的子分区数量大致相同。
    */
    static JobVertexInputInfo computeVertexInputInfoForPointwise(
            int sourceCount,
            int targetCount,
            Function<Integer, Integer> numOfSubpartitionsRetriever,
            boolean isDynamicGraph) {
        /**
         * 创建List<ExecutionVertexInputInfo> 对象
         */
        final List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();
        /**
         * 如果上游并行度 >= 下游并行度
         */
        if (sourceCount >= targetCount) {
            for (int index = 0; index < targetCount; index++) {

                int start = index * sourceCount / targetCount;
                int end = (index + 1) * sourceCount / targetCount;
                /** 计算每个下游顶点应该消费的上游分区的范围（start到end-1） */
                IndexRange partitionRange = new IndexRange(start, end - 1);
                IndexRange subpartitionRange =
                        computeConsumedSubpartitionRange(
                                index,
                                1,
                                () -> numOfSubpartitionsRetriever.apply(start),
                                isDynamicGraph,
                                false);
                executionVertexInputInfos.add(
                        new ExecutionVertexInputInfo(index, partitionRange, subpartitionRange));
            }
        } else {
            for (int partitionNum = 0; partitionNum < sourceCount; partitionNum++) {

                int start = (partitionNum * targetCount + sourceCount - 1) / sourceCount;
                int end = ((partitionNum + 1) * targetCount + sourceCount - 1) / sourceCount;
                int numConsumers = end - start;

                IndexRange partitionRange = new IndexRange(partitionNum, partitionNum);
                // Variable used in lambda expression should be final or effectively final
                final int finalPartitionNum = partitionNum;
                for (int i = start; i < end; i++) {
                    IndexRange subpartitionRange =
                            computeConsumedSubpartitionRange(
                                    i,
                                    numConsumers,
                                    () -> numOfSubpartitionsRetriever.apply(finalPartitionNum),
                                    isDynamicGraph,
                                    false);
                    executionVertexInputInfos.add(
                            new ExecutionVertexInputInfo(i, partitionRange, subpartitionRange));
                }
            }
        }
        return new JobVertexInputInfo(executionVertexInputInfos);
    }

    /**
     * Compute the {@link JobVertexInputInfo} for a {@link DistributionPattern#ALL_TO_ALL} edge.
     * This computation algorithm will evenly distribute subpartitions to downstream subtasks
     * according to the number of subpartitions. Different downstream subtasks consume roughly the
     * same number of subpartitions.
     *
     * @param sourceCount the parallelism of upstream
     * @param targetCount the parallelism of downstream
     * @param numOfSubpartitionsRetriever a retriever to get the number of subpartitions
     * @param isDynamicGraph whether is dynamic graph
     * @param isBroadcast whether the edge is broadcast
     * @return the computed {@link JobVertexInputInfo}
     */
    static JobVertexInputInfo computeVertexInputInfoForAllToAll(
            int sourceCount,
            int targetCount,
            Function<Integer, Integer> numOfSubpartitionsRetriever,
            boolean isDynamicGraph,
            boolean isBroadcast) {
        final List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();
        IndexRange partitionRange = new IndexRange(0, sourceCount - 1);
        for (int i = 0; i < targetCount; ++i) {
            IndexRange subpartitionRange =
                    computeConsumedSubpartitionRange(
                            i,
                            targetCount,
                            () -> numOfSubpartitionsRetriever.apply(0),
                            isDynamicGraph,
                            isBroadcast);
            executionVertexInputInfos.add(
                    new ExecutionVertexInputInfo(i, partitionRange, subpartitionRange));
        }
        return new JobVertexInputInfo(executionVertexInputInfos);
    }

    /**
     * Compute the consumed subpartition range for a subtask. This computation algorithm will evenly
     * distribute subpartitions to downstream subtasks according to the number of subpartitions.
     * Different downstream subtasks consume roughly the same number of subpartitions.
     *
     * @param consumerSubtaskIndex the subtask index
     * @param numConsumers the total number of consumers
     * @param numOfSubpartitionsSupplier a supplier to get the number of subpartitions
     * @param isDynamicGraph whether is dynamic graph
     * @param isBroadcast whether the edge is broadcast
     * @return the computed subpartition range
     */
    @VisibleForTesting
    static IndexRange computeConsumedSubpartitionRange(
            int consumerSubtaskIndex,
            int numConsumers,
            Supplier<Integer> numOfSubpartitionsSupplier,
            boolean isDynamicGraph,
            boolean isBroadcast) {
        int consumerIndex = consumerSubtaskIndex % numConsumers;
        if (!isDynamicGraph) {
            return new IndexRange(consumerIndex, consumerIndex);
        } else {
            int numSubpartitions = numOfSubpartitionsSupplier.get();
            if (isBroadcast) {
                // broadcast results have only one subpartition, and be consumed multiple times.
                checkArgument(numSubpartitions == 1);
                return new IndexRange(0, 0);
            } else {
                checkArgument(consumerIndex < numConsumers);
                checkArgument(numConsumers <= numSubpartitions);

                int start = consumerIndex * numSubpartitions / numConsumers;
                int nextStart = (consumerIndex + 1) * numSubpartitions / numConsumers;

                return new IndexRange(start, nextStart - 1);
            }
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 包装类，内部存放IntermediateResult
    */
    private static class IntermediateResultWrapper implements IntermediateResultInfo {
        private final IntermediateResult intermediateResult;

        IntermediateResultWrapper(IntermediateResult intermediateResult) {
            this.intermediateResult = checkNotNull(intermediateResult);
        }

        @Override
        public IntermediateDataSetID getResultId() {
            return intermediateResult.getId();
        }

        @Override
        public boolean isBroadcast() {
            return intermediateResult.isBroadcast();
        }

        @Override
        public boolean isPointwise() {
            return intermediateResult.getConsumingDistributionPattern()
                    == DistributionPattern.POINTWISE;
        }

        @Override
        public int getNumPartitions() {
            return intermediateResult.getNumberOfAssignedPartitions();
        }

        @Override
        public int getNumSubpartitions(int partitionIndex) {
            // Note that this method should only be called for dynamic graph.This method is used to
            // compute which sub-partitions a consumer vertex should consume, however, for
            // non-dynamic graph it is not needed, and the number of sub-partitions is not decided
            // at this stage, due to the execution edge are not created.
            checkState(
                    intermediateResult.getProducer().getGraph().isDynamic(),
                    "This method should only be called for dynamic graph.");
            return intermediateResult.getPartitions()[partitionIndex].getNumberOfSubpartitions();
        }
    }

    /** Private default constructor to avoid being instantiated. */
    private VertexInputInfoComputationUtils() {}
}
