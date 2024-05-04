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

package org.apache.flink.runtime.slots;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.util.ResourceCounter;

import java.util.Optional;
import java.util.function.Function;

/** A matcher for resource profiles to requirements. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 资源配置文件与需求的匹配器。
*/
public interface RequirementMatcher {

    /**
     * Attempts to match the given resource profile with one of the given requirements.
     *
     * @param resourceProfile resource profile to match
     * @param totalRequirements the total requirements
     * @param numAssignedResourcesLookup a lookup for how many resources have already been assigned
     *     to a requirement
     * @return matching requirement profile, if one exists
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 尝试将给定的资源配置文件与给定的需求之一相匹配。
     * @param resourceProfile 要匹配的资源配置文件
     * @param totalRequirements 总要求
     * @param numAssignedResourcesLookup  查找已经分配了多少资源的需求
     * @return matching requirement profile, if one exists
    */
    Optional<ResourceProfile> match(
            ResourceProfile resourceProfile,
            ResourceCounter totalRequirements,
            Function<ResourceProfile, Integer> numAssignedResourcesLookup);
}
