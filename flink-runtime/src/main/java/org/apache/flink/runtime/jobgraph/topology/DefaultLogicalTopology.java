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
public class DefaultLogicalTopology implements LogicalTopology {
    /**
     * 用于存储 DefaultLogicalVertex 类型的对象。
     * 基于JobVertex 对象的某种逻辑表示或封装
     */
    private final List<DefaultLogicalVertex> verticesSorted;
    /**
     * 它的键是 JobVertexID 类型的对象，值是 DefaultLogicalVertex 类型的对象。
     * 映射允许你通过作业顶点的 ID 快速查找对应的逻辑顶点。
     */
    private final Map<JobVertexID, DefaultLogicalVertex> idToVertexMap;
    /**
     * 它的键是 IntermediateDataSetID 类型的对象，值是 DefaultLogicalResult 类型的对象。
     * 这个映射可能用于存储中间数据集 ID 与它们对应的逻辑结果之间的映射关系。
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

    private void buildVerticesAndResults(final Iterable<JobVertex> topologicallySortedJobVertices) {
        final Function<JobVertexID, DefaultLogicalVertex> vertexRetriever = this::getVertex;
        final Function<IntermediateDataSetID, DefaultLogicalResult> resultRetriever =
                this::getResult;

        for (JobVertex jobVertex : topologicallySortedJobVertices) {
            final DefaultLogicalVertex logicalVertex =
                    new DefaultLogicalVertex(jobVertex, resultRetriever);
            this.verticesSorted.add(logicalVertex);
            this.idToVertexMap.put(logicalVertex.getId(), logicalVertex);

            for (IntermediateDataSet intermediateDataSet : jobVertex.getProducedDataSets()) {
                final DefaultLogicalResult logicalResult =
                        new DefaultLogicalResult(intermediateDataSet, vertexRetriever);
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

    @Override
    public Iterable<DefaultLogicalPipelinedRegion> getAllPipelinedRegions() {
        final Set<Set<LogicalVertex>> regionsRaw =
                LogicalPipelinedRegionComputeUtil.computePipelinedRegions(verticesSorted);

        final Set<DefaultLogicalPipelinedRegion> regions = new HashSet<>();
        for (Set<LogicalVertex> regionVertices : regionsRaw) {
            regions.add(new DefaultLogicalPipelinedRegion(regionVertices));
        }
        return regions;
    }
}
