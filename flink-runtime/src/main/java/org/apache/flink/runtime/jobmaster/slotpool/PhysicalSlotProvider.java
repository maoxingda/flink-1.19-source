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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.jobmaster.SlotRequestId;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** The provider serves physical slot requests. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 提供物理Slot请求。
*/
public interface PhysicalSlotProvider {

    /**
     * Submit requests to allocate physical slots.
     *
     * <p>The physical slot can be either allocated from the slots, which are already available for
     * the job, or a new one can be requested from the resource manager.
     *
     * @param physicalSlotRequests physicalSlotRequest slot requirements
     * @return futures of the allocated slots
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 提交分配物理Slot的请求。
    */
    Map<SlotRequestId, CompletableFuture<PhysicalSlotRequest.Result>> allocatePhysicalSlots(
            Collection<PhysicalSlotRequest> physicalSlotRequests);

    /**
     * Cancels the slot request with the given {@link SlotRequestId}.
     *
     * <p>If the request is already fulfilled with a physical slot, the slot will be released.
     *
     * @param slotRequestId identifying the slot request to cancel
     * @param cause of the cancellation
     */
    void cancelSlotRequest(SlotRequestId slotRequestId, Throwable cause);

    /**
     * Disables batch slot request timeout check. Invoked when someone else wants to take over the
     * timeout check responsibility.
     */
    void disableBatchSlotRequestTimeoutCheck();
}
