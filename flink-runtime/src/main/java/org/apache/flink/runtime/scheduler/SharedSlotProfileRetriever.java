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

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.Set;

/** Computes a {@link SlotProfile} to allocate a slot for executions, sharing the slot. */
@FunctionalInterface
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 计算SlotProfile 以分配用于执行的插槽，共享该插槽。
*/
interface SharedSlotProfileRetriever {
    /**
     * Computes a {@link SlotProfile} of an execution slot sharing group.
     *
     * @param executionSlotSharingGroup executions sharing the slot.
     * @param physicalSlotResourceProfile {@link ResourceProfile} of the slot.
     * @return {@link SlotProfile} to allocate for the {@code executionSlotSharingGroup}.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 计算执行Slot共享组的  SlotProfile。
    */
    SlotProfile getSlotProfile(
            ExecutionSlotSharingGroup executionSlotSharingGroup,
            ResourceProfile physicalSlotResourceProfile);

    @FunctionalInterface
    interface SharedSlotProfileRetrieverFactory {
        SharedSlotProfileRetriever createFromBulk(Set<ExecutionVertexID> bulk);
    }
}
