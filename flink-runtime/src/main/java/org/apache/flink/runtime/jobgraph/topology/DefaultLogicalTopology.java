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

package org.apache.flink.runtime.jobgraph.topology;

import org.apache.flink.runtime.executiongraph.failover.LogicalPipelinedRegionComputeUtil;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link LogicalTopology}. It is an adapter of {@link JobGraph}. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * JobGraph对应的 逻辑拓扑
*/
public class DefaultLogicalTopology implements LogicalTopology {
    /**
     * LogicalTopology 中的顶点LogicalVertex ，即  JobVertex 内部封装了JobVertex。
     */
    private final List<DefaultLogicalVertex> verticesSorted;
    /**
     * LogicalTopology 中的顶点LogicalVertex ，即  JobVertex 内部封装了JobVertex。
     */
    private final Map<JobVertexID, DefaultLogicalVertex> idToVertexMap;
    /**
     * 表示由  LogicalVertex 生成的数据集，即  IntermediateDataSet  内部封装了IntermediateDataSet。
     */
    private final Map<IntermediateDataSetID, DefaultLogicalResult> idToResultMap;

    private DefaultLogicalTopology(final List<JobVertex> jobVertices) {
        /** 校验是否为null */
        checkNotNull(jobVertices);
        /** 初始化Map、List*/
        this.verticesSorted = new ArrayList<>(jobVertices.size());
        this.idToVertexMap = new HashMap<>();
        this.idToResultMap = new HashMap<>();
        /**
         * 调用 buildVerticesAndResults(jobVertices) 方法，传入 jobVertices 列表。
         * 这个方法的具体实现没有给出，但我们可以推测它会处理传入的作业顶点列表，
         * 并填充 verticesSorted、idToVertexMap 和 idToResultMap 这三个成员变量。
         */
        buildVerticesAndResults(jobVertices);
    }

    public static DefaultLogicalTopology fromJobGraph(final JobGraph jobGraph) {
        checkNotNull(jobGraph);

        return fromTopologicallySortedJobVertices(
                jobGraph.getVerticesSortedTopologicallyFromSources());
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 基于List<JobVertex> jobVertices构建DefaultLogicalTopology
    */
    public static DefaultLogicalTopology fromTopologicallySortedJobVertices(
            final List<JobVertex> jobVertices) {
        /** 它创建了一个新的 DefaultLogicalTopology 对象，并将传入的 jobVertices 列表作为构造函数的参数。 */
        return new DefaultLogicalTopology(jobVertices);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 初始化 Map<JobVertexID, DefaultLogicalVertex> idToVertexMap
     * 初始化 Map<IntermediateDataSetID, DefaultLogicalResult> idToResultMap
    */
    private void buildVerticesAndResults(final Iterable<JobVertex> topologicallySortedJobVertices) {
        /** 定义两个函数时接口实例*/
        /** 接收 JobVertexID 返回DefaultLogicalVertex 对象 */
        final Function<JobVertexID, DefaultLogicalVertex> vertexRetriever = this::getVertex;
        /** 接收 IntermediateDataSetID 作为参数，返回 DefaultLogicalResult 对象。 */
        final Function<IntermediateDataSetID, DefaultLogicalResult> resultRetriever =
                this::getResult;

        /** 循环 Iterable<JobVertex> */
        for (JobVertex jobVertex : topologicallySortedJobVertices) {
            /** 为每个 JobVertex 创建一个 DefaultLogicalVertex 对象，并将 resultRetriever 作为参数传递给它的构造函数。 */
            final DefaultLogicalVertex logicalVertex =
                    new DefaultLogicalVertex(jobVertex, resultRetriever);
            /** 将新创建的 logicalVertex 添加到 verticesSorted 列表中。 */
            this.verticesSorted.add(logicalVertex);
            /** 将 logicalVertex 的 ID 作为键，logicalVertex 对象本身作为值，添加到 idToVertexMap 映射中。 */
            this.idToVertexMap.put(logicalVertex.getId(), logicalVertex);
            /** 循环 List<IntermediateDataSet>*/
            for (IntermediateDataSet intermediateDataSet : jobVertex.getProducedDataSets()) {
                /** 对于 jobVertex 产生的每个 IntermediateDataSet，创建一个 DefaultLogicalResult 对象，*/
                final DefaultLogicalResult logicalResult =
                        new DefaultLogicalResult(intermediateDataSet, vertexRetriever);
                /** 并将 vertexRetriever 作为参数传递给它的构造函数。然后，将 logicalResult 的 ID 作为键，
                 * logicalResult 对象本身作为值，添加到 idToResultMap 映射中。 */
                idToResultMap.put(logicalResult.getId(), logicalResult);
            }
        }
    }

    @Override
    public Iterable<DefaultLogicalVertex> getVertices() {
        return verticesSorted;
    }

    public DefaultLogicalVertex getVertex(final JobVertexID vertexId) {
        return Optional.ofNullable(idToVertexMap.get(vertexId))
                .orElseThrow(
                        () -> new IllegalArgumentException("can not find vertex: " + vertexId));
    }

    private DefaultLogicalResult getResult(final IntermediateDataSetID resultId) {
        return Optional.ofNullable(idToResultMap.get(resultId))
                .orElseThrow(
                        () -> new IllegalArgumentException("can not find result: " + resultId));
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 构建Iterable<DefaultLogicalPipelinedRegion>
    */
    @Override
    public Iterable<DefaultLogicalPipelinedRegion> getAllPipelinedRegions() {
        final Set<Set<LogicalVertex>> regionsRaw =
                LogicalPipelinedRegionComputeUtil.computePipelinedRegions(verticesSorted);
        /** 创建一个新的 HashSet，用于存储 DefaultLogicalPipelinedRegion 对象。 */
        final Set<DefaultLogicalPipelinedRegion> regions = new HashSet<>();
        /** 循环 regionRaw */
        for (Set<LogicalVertex> regionVertices : regionsRaw) {
            /**
             * 将Set<LogicalVertex> 封装为DefaultLogicalPipelinedRegion
             * 添加到set集合
             */
            regions.add(new DefaultLogicalPipelinedRegion(regionVertices));
        }
        /** Set<DefaultLogicalPipelinedRegion> */
        return regions;
    }
}
