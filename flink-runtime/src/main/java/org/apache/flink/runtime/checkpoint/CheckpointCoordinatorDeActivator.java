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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.JobStatusListener;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This actor listens to changes in the JobStatus and activates or deactivates the periodic
 * checkpoint scheduler.
 */
public class CheckpointCoordinatorDeActivator implements JobStatusListener {

    private final CheckpointCoordinator coordinator;

    public CheckpointCoordinatorDeActivator(CheckpointCoordinator coordinator) {
        this.coordinator = checkNotNull(coordinator);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 当作业状态改变时调用，传入作业ID、新的作业状态和时间戳
    */
    @Override
    public void jobStatusChanges(JobID jobId, JobStatus newJobStatus, long timestamp) {
        if (newJobStatus == JobStatus.RUNNING) {
            // start the checkpoint scheduler
            // 如果是RUNNING状态，则启动检查点调度器
            // 检查点调度器可能用于定期保存作业的状态信息，以便在故障发生时能够恢复
            // start the checkpoint scheduler
            coordinator.startCheckpointScheduler();
        } else {
            // anything else should stop the trigger for now
            // 如果不是RUNNING状态（比如COMPLETED、FAILED、CANCELED等）
            // 则应该停止检查点调度器的触发
            // 这样可以避免在作业不需要时仍然进行不必要的检查点保存
            // anything else should stop the trigger for now
            coordinator.stopCheckpointScheduler();
        }
    }
}
