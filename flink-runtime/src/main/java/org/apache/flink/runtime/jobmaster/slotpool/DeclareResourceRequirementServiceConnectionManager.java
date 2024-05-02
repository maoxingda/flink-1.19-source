/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.jobmaster.ServiceConnectionManager;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.slots.ResourceRequirements;

import java.util.concurrent.CompletableFuture;

/**
 * {@link ServiceConnectionManager} for declaring resource requirements.
 *
 * <p>In practice the backing service will be the ResourceManager.
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 用于声明资源需求的ServiceConnectionManager。
*/
public interface DeclareResourceRequirementServiceConnectionManager
        extends ServiceConnectionManager<
                DeclareResourceRequirementServiceConnectionManager
                        .DeclareResourceRequirementsService> {

    /**
     * Declares the given resource requirements at the connected service. If no connection is
     * established, then this call will be ignored.
     *
     * @param resourceRequirements resourceRequirements to declare at the connected service
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 在连接的服务中声明给定的资源需求。如果未建立连接，则此调用将被忽略。
    */
    void declareResourceRequirements(ResourceRequirements resourceRequirements);

    /** Service that accepts resource requirements. */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 接受资源需求的服务
    */
    interface DeclareResourceRequirementsService {
        CompletableFuture<Acknowledge> declareResourceRequirements(
                ResourceRequirements resourceRequirements);
    }
}
