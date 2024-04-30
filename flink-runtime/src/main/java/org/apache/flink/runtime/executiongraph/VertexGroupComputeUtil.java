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

package org.apache.flink.runtime.executiongraph;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

/** Common utils for processing vertex groups. */
public final class VertexGroupComputeUtil {
   /**
    * @授课老师(微信): yi_locus
    * email: 156184212@qq.com
    * 将较小的组合并到较大的组中以降低成本
   */
    public static <V> Set<V> mergeVertexGroups(
            final Set<V> group1, final Set<V> group2, final Map<V, Set<V>> vertexToGroup) {

        // merge the smaller group into the larger one to reduce the cost
        /** 定义大小两个 Set<DefaultLogicalVertex> 集合*/
        final Set<V> smallerSet;
        final Set<V> largerSet;
        /** 根据两个set集合大小 赋值给上面声明的结合 */
        if (group1.size() < group2.size()) {
            smallerSet = group1;
            largerSet = group2;
        } else {
            smallerSet = group2;
            largerSet = group1;
        }
        /** 对于 smallerSet 中的每个顶点 v，该代码将 v 映射到 largerSet。 */
        for (V v : smallerSet) {
            /** 添加到 Map<DefaultLogicalVertex,Set<DefaultLogicalVertex>> */
            vertexToGroup.put(v, largerSet);
        }
        /** smallerSet 中的所有元素添加到 largerSet 中 */
        largerSet.addAll(smallerSet);
        return largerSet;
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *
    */
    public static <V> Set<Set<V>> uniqueVertexGroups(final Map<V, Set<V>> vertexToGroup) {
        /**
         *  Collections.newSetFromMap 方法来创建一个基于 IdentityHashMap 的集合。IdentityHashMap
         *  是一个特殊的哈希映射，它使用系统身份哈希码（即对象的内存地址）而不是对象的 hashCode 方法来确定键的唯一性。
         */
        final Set<Set<V>> distinctGroups = Collections.newSetFromMap(new IdentityHashMap<>());
        /** 添加所有顶点集合到唯一的集合集合中 */
        distinctGroups.addAll(vertexToGroup.values());
        /** 返回 Set<Set<DefaultLogicalVertex>*/
        return distinctGroups;
    }

    private VertexGroupComputeUtil() {}
}
