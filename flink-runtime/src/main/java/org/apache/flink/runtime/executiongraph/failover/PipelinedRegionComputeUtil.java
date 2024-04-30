/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.runtime.executiongraph.VertexGroupComputeUtil;
import org.apache.flink.runtime.topology.Result;
import org.apache.flink.runtime.topology.Vertex;

import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/** Common utils for computing pipelined regions. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 用于计算Pipeline Region区域的通用utils
*/
public final class PipelinedRegionComputeUtil {
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *
    */
    static <V extends Vertex<?, ?, V, R>, R extends Result<?, ?, V, R>>
            Map<V, Set<V>> buildRawRegions(
                    final Iterable<? extends V> topologicallySortedVertices,
                    final Function<V, Iterable<R>> getMustBePipelinedConsumedResults) {
        /** 创建一个IdentityHashMap来存储每个顶点及其所属区域的映射。方便下面使用 */
        final Map<V, Set<V>> vertexToRegion = new IdentityHashMap<>();

        // iterate all the vertices which are topologically sorted
        /** 迭代拓扑排序的所有顶点 */
        for (V vertex : topologicallySortedVertices) {
            /**
             * 为每个顶点创建一个新的区域集合，并将该顶点添加到该集合中。
             * 将顶点及其区域集合添加到vertexToRegion映射中。
             */
            /** 为每个顶点创建一个区域集合 Set*/
            Set<V> currentRegion = new HashSet<>();
            /** 将顶点 DefaultLogicalVertex 添加到 set中*/
            currentRegion.add(vertex);
            /**
             * 将顶点及其区域集合添加到vertexToRegion映射中
             * Map<DefaultLogicalVertex, Set<DefaultLogicalVertex>> vertexToRegion
             * */
            vertexToRegion.put(vertex, currentRegion);

            // Each vertex connected through not mustBePipelined consumingConstraint is considered
            // as a
            // single region.
            /**
             * 通过顶点获取顶点要消费的上游分区结果
             */
            for (R consumedResult : getMustBePipelinedConsumedResults.apply(vertex)) {
                /** 获取上游LogicalVertext */
                final V producerVertex = consumedResult.getProducer();
                /** 获取上游对应的set Region*/
                final Set<V> producerRegion = vertexToRegion.get(producerVertex);
                /** 如果为空 则抛出异常*/
                if (producerRegion == null) {
                    throw new IllegalStateException(
                            "Producer task "
                                    + producerVertex.getId()
                                    + " failover region is null"
                                    + " while calculating failover region for the consumer task "
                                    + vertex.getId()
                                    + ". This should be a failover region building bug.");
                }

                // check if it is the same as the producer region, if so skip the merge
                // this check can significantly reduce compute complexity in All-to-All
                // PIPELINED edge case
                /**
                 * 判断它与上游的Region是否相同，如果不相同
                 */
                if (currentRegion != producerRegion) {
                    /** 合并DefaultLogicalVertex*/
                    currentRegion =
                            VertexGroupComputeUtil.mergeVertexGroups(
                                    currentRegion, producerRegion, vertexToRegion);
                }
            }
        }
        /** 返回 Map<DefaultLogicalVertex, Set<DefaultLogicalVertex>> vertexToRegion*/
        return vertexToRegion;
    }

    private PipelinedRegionComputeUtil() {}
}
