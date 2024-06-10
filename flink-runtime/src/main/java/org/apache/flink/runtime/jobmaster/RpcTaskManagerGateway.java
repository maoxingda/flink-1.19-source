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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** Implementation of the {@link TaskManagerGateway} for Flink's RPC system. */
public class RpcTaskManagerGateway implements TaskManagerGateway {

    private final TaskExecutorGateway taskExecutorGateway;

    private final JobMasterId jobMasterId;

    public RpcTaskManagerGateway(TaskExecutorGateway taskExecutorGateway, JobMasterId jobMasterId) {
        this.taskExecutorGateway = Preconditions.checkNotNull(taskExecutorGateway);
        this.jobMasterId = Preconditions.checkNotNull(jobMasterId);
    }

    @Override
    public String getAddress() {
        return taskExecutorGateway.getAddress();
    }

    @Override
    public CompletableFuture<Acknowledge> submitTask(TaskDeploymentDescriptor tdd, Time timeout) {
        return taskExecutorGateway.submitTask(tdd, jobMasterId, timeout);
    }

    @Override
    public CompletableFuture<Acknowledge> cancelTask(
            ExecutionAttemptID executionAttemptID, Time timeout) {
        return taskExecutorGateway.cancelTask(executionAttemptID, timeout);
    }

    @Override
    public CompletableFuture<Acknowledge> updatePartitions(
            ExecutionAttemptID executionAttemptID,
            Iterable<PartitionInfo> partitionInfos,
            Time timeout) {
        return taskExecutorGateway.updatePartitions(executionAttemptID, partitionInfos, timeout);
    }

    @Override
    public void releasePartitions(JobID jobId, Set<ResultPartitionID> partitionIds) {
        taskExecutorGateway.releasePartitions(jobId, partitionIds);
    }

    @Override
    public void notifyCheckpointOnComplete(
            ExecutionAttemptID executionAttemptID,
            JobID jobId,
            long completedCheckpointId,
            long completedTimestamp,
            long lastSubsumedCheckpointId) {
        taskExecutorGateway.confirmCheckpoint(
                executionAttemptID,
                completedCheckpointId,
                completedTimestamp,
                lastSubsumedCheckpointId);
    }

    @Override
    public void notifyCheckpointAborted(
            ExecutionAttemptID executionAttemptID,
            JobID jobId,
            long checkpointId,
            long latestCompletedCheckpointId,
            long timestamp) {
        taskExecutorGateway.abortCheckpoint(
                executionAttemptID, checkpointId, latestCompletedCheckpointId, timestamp);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 触发一个指定执行尝试ID、作业ID和检查点选项的检查点。
     *
     * @param executionAttemptID 执行尝试的ID，用于标识要触发检查点的特定执行实例
     * @param jobId 作业的ID，用于标识要触发检查点的作业
     * @param checkpointId 检查点的ID，用于唯一标识要触发的检查点
     * @param timestamp 检查点的时间戳，通常用于日志记录和版本控制
     * @param checkpointOptions 检查点的选项配置，用于定制检查点的行为（如是否同步，是否强制等）
     * @return 返回一个Future对象，该对象在检查点被触发后将携带一个确认信息(Acknowledge)
    */
    @Override
    public CompletableFuture<Acknowledge> triggerCheckpoint(
            ExecutionAttemptID executionAttemptID,
            JobID jobId,
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions) {
        // 调用任务执行器网关的triggerCheckpoint方法来触发检查点
        // 传入执行尝试ID、检查点ID、时间戳和检查点选项
        return taskExecutorGateway.triggerCheckpoint(
                executionAttemptID, checkpointId, timestamp, checkpointOptions);
    }

    @Override
    public CompletableFuture<Acknowledge> freeSlot(
            AllocationID allocationId, Throwable cause, Time timeout) {
        return taskExecutorGateway.freeSlot(allocationId, cause, timeout);
    }

    @Override
    public CompletableFuture<Acknowledge> sendOperatorEventToTask(
            ExecutionAttemptID task, OperatorID operator, SerializedValue<OperatorEvent> evt) {
        return taskExecutorGateway.sendOperatorEventToTask(task, operator, evt);
    }
}
