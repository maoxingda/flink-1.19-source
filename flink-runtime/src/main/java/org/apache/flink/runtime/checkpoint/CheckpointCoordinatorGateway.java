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
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

/** RPC Gateway interface for messages to the CheckpointCoordinator. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * Checkpoint相关的网关接口
 * Task执行过程中向CheckpointCoordinator汇报当前Task的Checkpoint的执行情况
*/
public interface CheckpointCoordinatorGateway extends RpcGateway {

    void acknowledgeCheckpoint(
            final JobID jobID,
            final ExecutionAttemptID executionAttemptID,
            final long checkpointId,
            final CheckpointMetrics checkpointMetrics,
            @Nullable final SerializedValue<TaskStateSnapshot> subtaskState);

    void declineCheckpoint(DeclineCheckpoint declineCheckpoint);

    void reportCheckpointMetrics(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics);

    void reportInitializationMetrics(
            JobID jobId, SubTaskInitializationMetrics initializationMetrics);
}
