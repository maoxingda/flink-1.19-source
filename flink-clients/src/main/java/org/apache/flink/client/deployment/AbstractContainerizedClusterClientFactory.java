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

package org.apache.flink.client.deployment;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.jobmanager.JobManagerProcessUtils;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An abstract {@link ClusterClientFactory} containing some common implementations for different
 * containerized deployment clusters.
 */
@Internal
public abstract class AbstractContainerizedClusterClientFactory<ClusterID>
        implements ClusterClientFactory<ClusterID> {

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 获取启动集群需要的配置信息
    */
    @Override
    public ClusterSpecification getClusterSpecification(Configuration configuration) {
        // 检查传入的配置信息是否为空
        checkNotNull(configuration);

        // 从配置中获取JobManager（作业管理器）的总进程内存（MB为单位）
        // JobManagerProcessUtils是一个工具类，用于从配置中解析JobManager的内存规格
        // 这里使用了一个新的选项来解释旧的堆内存配置（如果有的话）
        final int jobManagerMemoryMB =
                JobManagerProcessUtils.processSpecFromConfigWithNewOptionToInterpretLegacyHeap(
                                configuration, JobManagerOptions.TOTAL_PROCESS_MEMORY)
                        .getTotalProcessMemorySize()
                        .getMebiBytes();

        // 从配置中获取TaskManager（任务管理器）的总进程内存（MB为单位）
        // TaskExecutorProcessUtils是另一个工具类，用于从配置中解析TaskManager的内存规格
        // 这里还涉及到一个将旧的TaskManager堆内存大小配置映射到新的配置选项的逻辑
        final int taskManagerMemoryMB =
                TaskExecutorProcessUtils.processSpecFromConfig(
                                TaskExecutorProcessUtils
                                        .getConfigurationMapLegacyTaskManagerHeapSizeToConfigOption(
                                                configuration,
                                                TaskManagerOptions.TOTAL_PROCESS_MEMORY))
                        .getTotalProcessMemorySize()
                        .getMebiBytes();
        // 从配置中获取每个TaskManager的槽位数（即并发执行任务的数量）
        int slotsPerTaskManager = configuration.get(TaskManagerOptions.NUM_TASK_SLOTS);
        // 使用ClusterSpecification.ClusterSpecificationBuilder创建一个集群规格
        // 并设置JobManager和TaskManager的内存以及每个TaskManager的槽位数
        return new ClusterSpecification.ClusterSpecificationBuilder()
                .setMasterMemoryMB(jobManagerMemoryMB)
                .setTaskManagerMemoryMB(taskManagerMemoryMB)
                .setSlotsPerTaskManager(slotsPerTaskManager)
                .createClusterSpecification();
    }
}
