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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceID;

import java.util.Collection;

/** Resource related actions which the {@link SlotManager} can perform. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 可以执行的与资源相关的操作
*/
public interface ResourceAllocator {

    /** Whether allocate/release resources are supported. */
    /** 是否支持分配/释放资源 */
    boolean isSupported();

    /**
     * Clean up the disconnected resource with the given resource id.
     *
     * @param resourceID identifying which resource to clean up
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 使用给定的资源id清理断开连接的资源。
    */
    void cleaningUpDisconnectedResource(ResourceID resourceID);

    /** declare resource need by slot manager. */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 通过SlotManager声明资源需求。
    */
    void declareResourceNeeded(Collection<ResourceDeclaration> resourceDeclarations);
}
