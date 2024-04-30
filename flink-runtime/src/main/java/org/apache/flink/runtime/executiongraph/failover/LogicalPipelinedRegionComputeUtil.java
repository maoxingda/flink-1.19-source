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

package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.runtime.jobgraph.topology.LogicalPipelinedRegion;
import org.apache.flink.runtime.jobgraph.topology.LogicalResult;
import org.apache.flink.runtime.jobgraph.topology.LogicalVertex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.runtime.executiongraph.VertexGroupComputeUtil.uniqueVertexGroups;

/** Utils for computing {@link LogicalPipelinedRegion}s. */
public final class LogicalPipelinedRegionComputeUtil {

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 构建出Set<Set<LogicalVertex>>
    */
    public static Set<Set<LogicalVertex>> computePipelinedRegions(
            final Iterable<? extends LogicalVertex> topologicallySortedVertices) {
        /**
         * 构建 Map<LogicalVertex, Set<LogicalVertex>>对象
         */
        final Map<LogicalVertex, Set<LogicalVertex>> vertexToRegion =
                PipelinedRegionComputeUtil.buildRawRegions(
                        topologicallySortedVertices,
                        LogicalPipelinedRegionComputeUtil::getMustBePipelinedConsumedResults);

        // Since LogicalTopology is a DAG, there is no need to do cycle detection nor to merge
        // regions on cycles.
        return uniqueVertexGroups(vertexToRegion);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 传入参数LogicalVertex vertex 顶点,Iterable<LogicalResult> 对象
     * LogicalResult 表示由  LogicalVertex 生成的数据集，即  IntermediateDataSet}。
    */
    private static Iterable<LogicalResult> getMustBePipelinedConsumedResults(LogicalVertex vertex) {
        /**
         * 创建一个新的 ArrayList 来存储DefaultLogicalVertex消费的 LogicalResult 对象。
         */
        List<LogicalResult> mustBePipelinedConsumedResults = new ArrayList<>();
        /**
         * 1.vertex.getConsumedResults(),获取的LogicalVertex要消费的最终数据（也就是上游JobEdge对应的输入）
         * 返回LogicalResult类型
         */
        for (LogicalResult consumedResult : vertex.getConsumedResults()) {
            /** 如果该分区的上下游同时支持调度，则返回。 */
            if (consumedResult.getResultType().mustBePipelinedConsumed()) {
                /** 添加到 List<LogicalResult>*/
                mustBePipelinedConsumedResults.add(consumedResult);
            }
        }
        /** 返回 */
        return mustBePipelinedConsumedResults;
    }

    private LogicalPipelinedRegionComputeUtil() {}
}
