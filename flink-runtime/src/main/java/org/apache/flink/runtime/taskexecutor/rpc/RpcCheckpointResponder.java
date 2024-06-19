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

package org.apache.flink.runtime.taskexecutor.rpc;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorGateway;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.SubTaskInitializationMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.runtime.checkpoint.TaskStateSnapshot.serializeTaskStateSnapshot;

public class RpcCheckpointResponder implements CheckpointResponder {

    private final CheckpointCoordinatorGateway checkpointCoordinatorGateway;

    public RpcCheckpointResponder(CheckpointCoordinatorGateway checkpointCoordinatorGateway) {
        this.checkpointCoordinatorGateway =
                Preconditions.checkNotNull(checkpointCoordinatorGateway);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 确认检查点的方法，当子任务成功处理了一个检查点并准备将其状态持久化时调用。
     *
     * @param jobID              作业的唯一标识符
     * @param executionAttemptID 执行尝试的唯一标识符
     * @param checkpointId       当前检查点的ID
     * @param checkpointMetrics  关于检查点的度量指标
     * @param subtaskState       子任务的状态快照，表示在检查点时的状态
    */
    @Override
    public void acknowledgeCheckpoint(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics,
            TaskStateSnapshot subtaskState) {
        // 调用checkpointCoordinatorGateway的acknowledgeCheckpoint方法，以确认检查点
        // 这里可能需要先将子任务状态快照序列化，以便进行传输或存储
        checkpointCoordinatorGateway.acknowledgeCheckpoint(
                jobID,
                executionAttemptID,
                checkpointId,
                checkpointMetrics,
                serializeTaskStateSnapshot(subtaskState));
    }

    @Override
    public void reportCheckpointMetrics(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics) {
        checkpointCoordinatorGateway.reportCheckpointMetrics(
                jobID, executionAttemptID, checkpointId, checkpointMetrics);
    }

    @Override
    public void declineCheckpoint(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointException checkpointException) {

        checkpointCoordinatorGateway.declineCheckpoint(
                new DeclineCheckpoint(
                        jobID, executionAttemptID, checkpointId, checkpointException));
    }

    @Override
    public void reportInitializationMetrics(
            JobID jobId, SubTaskInitializationMetrics initializationMetrics) {
        checkpointCoordinatorGateway.reportInitializationMetrics(jobId, initializationMetrics);
    }
}
