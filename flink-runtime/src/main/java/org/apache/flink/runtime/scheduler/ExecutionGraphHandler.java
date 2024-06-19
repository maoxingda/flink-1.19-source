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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.SubTaskInitializationMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Executor;

/** Handler for the {@link ExecutionGraph} which offers some common operations. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 提供一些常见操作的  ExecutionGraph 的处理程序。
*/
public class ExecutionGraphHandler {

    private final ExecutionGraph executionGraph;

    private final Logger log;

    private final Executor ioExecutor;

    private final ComponentMainThreadExecutor mainThreadExecutor;

    public ExecutionGraphHandler(
            ExecutionGraph executionGraph,
            Logger log,
            Executor ioExecutor,
            ComponentMainThreadExecutor mainThreadExecutor) {
        this.executionGraph = executionGraph;
        this.log = log;
        this.ioExecutor = ioExecutor;
        this.mainThreadExecutor = mainThreadExecutor;
    }

    public void reportCheckpointMetrics(
            ExecutionAttemptID attemptId, long id, CheckpointMetrics metrics) {
        processCheckpointCoordinatorMessage(
                "ReportCheckpointStats",
                coordinator -> coordinator.reportCheckpointMetrics(id, attemptId, metrics));
    }

    public void reportInitializationMetrics(SubTaskInitializationMetrics initializationMetrics) {
        if (executionGraph.getCheckpointStatsTracker() == null) {
            // TODO: Consider to support reporting initialization stats without checkpointing
            log.debug(
                    "Ignoring reportInitializationMetrics if checkpoint coordinator is not present");
            return;
        }
        ioExecutor.execute(
                () -> {
                    try {
                        executionGraph
                                .getCheckpointStatsTracker()
                                .reportInitializationMetrics(initializationMetrics);
                    } catch (Exception t) {
                        log.warn("Error while reportInitializationMetrics", t);
                    }
                });
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 确认检查点。
     *
     * @param jobID 作业ID
     * @param executionAttemptID 执行尝试ID
     * @param checkpointId 检查点ID
     * @param checkpointMetrics 检查点度量指标
     * @param checkpointState 检查点状态快照
    */
    public void acknowledgeCheckpoint(
            final JobID jobID,
            final ExecutionAttemptID executionAttemptID,
            final long checkpointId,
            final CheckpointMetrics checkpointMetrics,
            final TaskStateSnapshot checkpointState) {
        // 处理检查点协调器消息
        // 这里的"AcknowledgeCheckpoint"是一个日志或调试信息，用于标识正在进行的操作
        processCheckpointCoordinatorMessage(
                "AcknowledgeCheckpoint",
                coordinator ->
                        // 调用检查点协调器的receiveAcknowledgeMessage方法，通知检查点已被确认
                        coordinator.receiveAcknowledgeMessage(
                                new AcknowledgeCheckpoint(
                                        jobID,
                                        executionAttemptID,
                                        checkpointId,
                                        checkpointMetrics,
                                        checkpointState),
                                retrieveTaskManagerLocation(executionAttemptID)));
    }

    public void declineCheckpoint(final DeclineCheckpoint decline) {
        processCheckpointCoordinatorMessage(
                "DeclineCheckpoint",
                coordinator ->
                        coordinator.receiveDeclineMessage(
                                decline,
                                retrieveTaskManagerLocation(decline.getTaskExecutionId())));
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 处理检查点协调器的消息。
     *
     * @param messageType 消息类型
     * @param process     用于处理检查点协调器的ThrowingConsumer
    */
    private void processCheckpointCoordinatorMessage(
            String messageType, ThrowingConsumer<CheckpointCoordinator, Exception> process) {
        // 确保当前线程是主线程
        mainThreadExecutor.assertRunningInMainThread();

        // 从执行图中获取检查点协调器
        final CheckpointCoordinator checkpointCoordinator =
                executionGraph.getCheckpointCoordinator();
        // 如果检查点协调器不为空
        if (checkpointCoordinator != null) {
            // 在IO线程池中异步执行处理逻辑
            ioExecutor.execute(
                    () -> {
                        try {
                            // 调用传入的process来处理检查点协调器
                            process.accept(checkpointCoordinator);
                        } catch (Exception t) {
                            // 处理过程中出现异常，记录警告日志
                            log.warn("Error while processing " + messageType + " message", t);
                        }
                    });
            // 如果没有检查点协调器
        } else {
            // 构造错误消息的模板
            String errorMessage =
                    "Received " + messageType + " message for job {} with no CheckpointCoordinator";
            // 判断作业的执行状态
            if (executionGraph.getState() == JobStatus.RUNNING) {
                // 如果作业正在运行，记录错误日志
                log.error(errorMessage, executionGraph.getJobID());
            } else {
                // 如果作业未在运行，记录调试日志
                log.debug(errorMessage, executionGraph.getJobID());
            }
        }
    }

    private String retrieveTaskManagerLocation(ExecutionAttemptID executionAttemptID) {
        final Optional<Execution> currentExecution =
                Optional.ofNullable(
                        executionGraph.getRegisteredExecutions().get(executionAttemptID));

        return currentExecution
                .map(Execution::getAssignedResourceLocation)
                .map(TaskManagerLocation::toString)
                .orElse("Unknown location");
    }

    public ExecutionState requestPartitionState(
            final IntermediateDataSetID intermediateResultId,
            final ResultPartitionID resultPartitionId)
            throws PartitionProducerDisposedException {

        final Execution execution =
                executionGraph.getRegisteredExecutions().get(resultPartitionId.getProducerId());
        if (execution != null) {
            return execution.getState();
        } else {
            final IntermediateResult intermediateResult =
                    executionGraph.getAllIntermediateResults().get(intermediateResultId);

            if (intermediateResult != null) {
                // Try to find the producing execution
                Execution producerExecution =
                        intermediateResult
                                .getPartitionById(resultPartitionId.getPartitionId())
                                .getProducer()
                                .getCurrentExecutionAttempt();

                if (producerExecution.getAttemptId().equals(resultPartitionId.getProducerId())) {
                    return producerExecution.getState();
                } else {
                    throw new PartitionProducerDisposedException(resultPartitionId);
                }
            } else {
                throw new IllegalArgumentException(
                        "Intermediate data set with ID " + intermediateResultId + " not found.");
            }
        }
    }

    public SerializedInputSplit requestNextInputSplit(
            JobVertexID vertexID, ExecutionAttemptID executionAttempt) throws IOException {

        final Execution execution = executionGraph.getRegisteredExecutions().get(executionAttempt);
        if (execution == null) {
            // can happen when JobManager had already unregistered this execution upon on task
            // failure,
            // but TaskManager get some delay to aware of that situation
            if (log.isDebugEnabled()) {
                log.debug("Can not find Execution for attempt {}.", executionAttempt);
            }
            // but we should TaskManager be aware of this
            throw new IllegalArgumentException(
                    "Can not find Execution for attempt " + executionAttempt);
        }

        final ExecutionJobVertex vertex = executionGraph.getJobVertex(vertexID);
        if (vertex == null) {
            throw new IllegalArgumentException(
                    "Cannot find execution vertex for vertex ID " + vertexID);
        }

        if (vertex.getSplitAssigner() == null) {
            throw new IllegalStateException("No InputSplitAssigner for vertex ID " + vertexID);
        }

        final Optional<InputSplit> optionalNextInputSplit = execution.getNextInputSplit();

        final InputSplit nextInputSplit;
        if (optionalNextInputSplit.isPresent()) {
            nextInputSplit = optionalNextInputSplit.get();
            log.debug("Send next input split {}.", nextInputSplit);
        } else {
            nextInputSplit = null;
            log.debug("No more input splits available");
        }

        try {
            final byte[] serializedInputSplit = InstantiationUtil.serializeObject(nextInputSplit);
            return new SerializedInputSplit(serializedInputSplit);
        } catch (Exception ex) {
            IOException reason =
                    new IOException(
                            "Could not serialize the next input split of class "
                                    + nextInputSplit.getClass()
                                    + ".",
                            ex);
            vertex.fail(reason);
            throw reason;
        }
    }
}
