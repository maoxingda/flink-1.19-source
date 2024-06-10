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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.InternalExecutionGraphAccessor;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation for {@link CheckpointPlanCalculator}. If all tasks are running, it
 * directly marks all the sources as tasks to trigger, otherwise it would try to find the running
 * tasks without running processors as tasks to trigger.
 */
public class DefaultCheckpointPlanCalculator implements CheckpointPlanCalculator {

    private final JobID jobId;

    private final CheckpointPlanCalculatorContext context;

    private final List<ExecutionJobVertex> jobVerticesInTopologyOrder = new ArrayList<>();

    private final List<ExecutionVertex> allTasks = new ArrayList<>();

    private final List<ExecutionVertex> sourceTasks = new ArrayList<>();

    private final boolean allowCheckpointsAfterTasksFinished;

    public DefaultCheckpointPlanCalculator(
            JobID jobId,
            CheckpointPlanCalculatorContext context,
            Iterable<ExecutionJobVertex> jobVerticesInTopologyOrderIterable,
            boolean allowCheckpointsAfterTasksFinished) {

        this.jobId = checkNotNull(jobId);
        this.context = checkNotNull(context);
        this.allowCheckpointsAfterTasksFinished = allowCheckpointsAfterTasksFinished;

        checkNotNull(jobVerticesInTopologyOrderIterable);
        jobVerticesInTopologyOrderIterable.forEach(
                jobVertex -> {
                    jobVerticesInTopologyOrder.add(jobVertex);
                    allTasks.addAll(Arrays.asList(jobVertex.getTaskVertices()));

                    if (jobVertex.getJobVertex().isInputVertex()) {
                        sourceTasks.addAll(Arrays.asList(jobVertex.getTaskVertices()));
                    }
                });
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建CheckpointPlan检查点计划
    */
    @Override
    public CompletableFuture<CheckpointPlan> calculateCheckpointPlan() {
        // 使用CompletableFuture在上下文的主执行器上异步执行检查点计划的计算
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        // 如果有任务已经完成，并且配置不允许在任务完成后进行检查点操作，则抛出异常
                        if (context.hasFinishedTasks() && !allowCheckpointsAfterTasksFinished) {
                            throw new CheckpointException(
                                    "Some tasks of the job have already finished and checkpointing with finished tasks is not enabled.",
                                    CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
                        }
                        // 检查所有任务是否都已初始化
                        checkAllTasksInitiated();
                        // 根据是否有任务已完成，选择不同的计算检查点计划的方法
                        CheckpointPlan result =
                                context.hasFinishedTasks()
                                        ? calculateAfterTasksFinished()// 如果有任务已完成，则调用此方法计算检查点计划
                                        : calculateWithAllTasksRunning();// 如果所有任务都在运行，则调用此方法计算检查点计划
                        // 检查需要等待的任务是否都已启动
                        checkTasksStarted(result.getTasksToWaitFor());
                        // 返回计算得出的检查点计划
                        return result;
                    } catch (Throwable throwable) {
                        // 如果在计算过程中发生异常，将其包装为CompletionException并抛出
                        throw new CompletionException(throwable);
                    }
                },
                context.getMainExecutor());
    }

    /**
     * Checks if all tasks are attached with the current Execution already. This method should be
     * called from JobMaster main thread executor.
     *
     * @throws CheckpointException if some tasks do not have attached Execution.
     */
    private void checkAllTasksInitiated() throws CheckpointException {
        for (ExecutionVertex task : allTasks) {
            if (task.getCurrentExecutionAttempt() == null) {
                throw new CheckpointException(
                        String.format(
                                "task %s of job %s is not being executed at the moment. Aborting checkpoint.",
                                task.getTaskNameWithSubtaskIndex(), jobId),
                        CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
            }
        }
    }

    /**
     * Checks if all tasks to trigger have already been in RUNNING state. This method should be
     * called from JobMaster main thread executor.
     *
     * @throws CheckpointException if some tasks to trigger have not turned into RUNNING yet.
     */
    private void checkTasksStarted(List<Execution> toTrigger) throws CheckpointException {
        for (Execution execution : toTrigger) {
            if (execution.getState() != ExecutionState.RUNNING) {
                throw new CheckpointException(
                        String.format(
                                "Checkpoint triggering task %s of job %s is not being executed at the moment. "
                                        + "Aborting checkpoint.",
                                execution.getVertex().getTaskNameWithSubtaskIndex(), jobId),
                        CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
            }
        }
    }

    /**
     * Computes the checkpoint plan when all tasks are running. It would simply marks all the source
     * tasks as need to trigger and all the tasks as need to wait and commit.
     *
     * @return The plan of this checkpoint.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 当所有任务都在运行时，计算检查点计划。
     * 这个方法会简单地标记所有源任务为需要触发，并将所有任务标记为需要等待和提交。
     *
     * @return 当前检查点的计划。
    */
    private CheckpointPlan calculateWithAllTasksRunning() {
        // 获取所有源任务的当前执行尝试，并收集到一个列表中
        List<Execution> executionsToTrigger =
                sourceTasks.stream()
                        .map(ExecutionVertex::getCurrentExecutionAttempt)
                        .collect(Collectors.toList());
        // 创建需要等待的任务列表
        List<Execution> tasksToWaitFor = createTaskToWaitFor(allTasks);
        // 使用上述信息创建一个检查点计划实例
        return new DefaultCheckpointPlan(
                Collections.unmodifiableList(executionsToTrigger),// 源任务作为需要触发的执行
                Collections.unmodifiableList(tasksToWaitFor),// 所有任务作为需要等待和提交的任务
                Collections.unmodifiableList(allTasks),// 所有任务列表（可能包括非源任务）
                Collections.emptyList(),// 预留字段，当前为空列表
                Collections.emptyList(),// 预留字段，当前为空列表
                allowCheckpointsAfterTasksFinished);// 是否允许在任务完成后进行检查点
    }

    /**
     * Calculates the checkpoint plan after some tasks have finished. We iterate the job graph to
     * find the task that is still running, but do not has precedent running tasks.
     *
     * @return The plan of this checkpoint.
     */
    private CheckpointPlan calculateAfterTasksFinished() {
        // First collect the task running status into BitSet so that we could
        // do JobVertex level judgement for some vertices and avoid time-consuming
        // access to volatile isFinished flag of Execution.
        Map<JobVertexID, BitSet> taskRunningStatusByVertex = collectTaskRunningStatus();

        List<Execution> tasksToTrigger = new ArrayList<>();
        List<Execution> tasksToWaitFor = new ArrayList<>();
        List<ExecutionVertex> tasksToCommitTo = new ArrayList<>();
        List<Execution> finishedTasks = new ArrayList<>();
        List<ExecutionJobVertex> fullyFinishedJobVertex = new ArrayList<>();

        for (ExecutionJobVertex jobVertex : jobVerticesInTopologyOrder) {
            BitSet taskRunningStatus = taskRunningStatusByVertex.get(jobVertex.getJobVertexId());

            if (taskRunningStatus.cardinality() == 0) {
                fullyFinishedJobVertex.add(jobVertex);

                for (ExecutionVertex task : jobVertex.getTaskVertices()) {
                    finishedTasks.add(task.getCurrentExecutionAttempt());
                }

                continue;
            }

            List<JobEdge> prevJobEdges = jobVertex.getJobVertex().getInputs();

            // this is an optimization: we determine at the JobVertex level if some tasks can even
            // be eligible for being in the "triggerTo" set.
            boolean someTasksMustBeTriggered =
                    someTasksMustBeTriggered(taskRunningStatusByVertex, prevJobEdges);

            for (int i = 0; i < jobVertex.getTaskVertices().length; ++i) {
                ExecutionVertex task = jobVertex.getTaskVertices()[i];
                if (taskRunningStatus.get(task.getParallelSubtaskIndex())) {
                    tasksToWaitFor.add(task.getCurrentExecutionAttempt());
                    tasksToCommitTo.add(task);

                    if (someTasksMustBeTriggered) {
                        boolean hasRunningPrecedentTasks =
                                hasRunningPrecedentTasks(
                                        task, prevJobEdges, taskRunningStatusByVertex);

                        if (!hasRunningPrecedentTasks) {
                            tasksToTrigger.add(task.getCurrentExecutionAttempt());
                        }
                    }
                } else {
                    finishedTasks.add(task.getCurrentExecutionAttempt());
                }
            }
        }

        return new DefaultCheckpointPlan(
                Collections.unmodifiableList(tasksToTrigger),
                Collections.unmodifiableList(tasksToWaitFor),
                Collections.unmodifiableList(tasksToCommitTo),
                Collections.unmodifiableList(finishedTasks),
                Collections.unmodifiableList(fullyFinishedJobVertex),
                allowCheckpointsAfterTasksFinished);
    }

    private boolean someTasksMustBeTriggered(
            Map<JobVertexID, BitSet> runningTasksByVertex, List<JobEdge> prevJobEdges) {

        for (JobEdge jobEdge : prevJobEdges) {
            DistributionPattern distributionPattern = jobEdge.getDistributionPattern();
            BitSet upstreamRunningStatus =
                    runningTasksByVertex.get(jobEdge.getSource().getProducer().getID());

            if (hasActiveUpstreamVertex(distributionPattern, upstreamRunningStatus)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Every task must have active upstream tasks if
     *
     * <ol>
     *   <li>ALL_TO_ALL connection and some predecessors are still running.
     *   <li>POINTWISE connection and all predecessors are still running.
     * </ol>
     *
     * @param distribution The distribution pattern between the upstream vertex and the current
     *     vertex.
     * @param upstreamRunningTasks The running tasks of the upstream vertex.
     * @return Whether every task of the current vertex is connected to some active predecessors.
     */
    private boolean hasActiveUpstreamVertex(
            DistributionPattern distribution, BitSet upstreamRunningTasks) {
        return (distribution == DistributionPattern.ALL_TO_ALL
                        && upstreamRunningTasks.cardinality() > 0)
                || (distribution == DistributionPattern.POINTWISE
                        && upstreamRunningTasks.cardinality() == upstreamRunningTasks.size());
    }

    private boolean hasRunningPrecedentTasks(
            ExecutionVertex vertex,
            List<JobEdge> prevJobEdges,
            Map<JobVertexID, BitSet> taskRunningStatusByVertex) {

        InternalExecutionGraphAccessor executionGraphAccessor = vertex.getExecutionGraphAccessor();

        for (int i = 0; i < prevJobEdges.size(); ++i) {
            if (prevJobEdges.get(i).getDistributionPattern() == DistributionPattern.POINTWISE) {
                for (IntermediateResultPartitionID consumedPartitionId :
                        vertex.getConsumedPartitionGroup(i)) {
                    ExecutionVertex precedentTask =
                            executionGraphAccessor
                                    .getResultPartitionOrThrow(consumedPartitionId)
                                    .getProducer();
                    BitSet precedentVertexRunningStatus =
                            taskRunningStatusByVertex.get(precedentTask.getJobvertexId());

                    if (precedentVertexRunningStatus.get(precedentTask.getParallelSubtaskIndex())) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * Collects the task running status for each job vertex.
     *
     * @return The task running status for each job vertex.
     */
    @VisibleForTesting
    Map<JobVertexID, BitSet> collectTaskRunningStatus() {
        Map<JobVertexID, BitSet> runningStatusByVertex = new HashMap<>();

        for (ExecutionJobVertex vertex : jobVerticesInTopologyOrder) {
            BitSet runningTasks = new BitSet(vertex.getTaskVertices().length);

            for (int i = 0; i < vertex.getTaskVertices().length; ++i) {
                if (!vertex.getTaskVertices()[i].getCurrentExecutionAttempt().isFinished()) {
                    runningTasks.set(i);
                }
            }

            runningStatusByVertex.put(vertex.getJobVertexId(), runningTasks);
        }

        return runningStatusByVertex;
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 根据提供的任务顶点列表创建需要等待的任务执行列表。
     *
     * @param tasks 任务的执行顶点列表。
     * @return 需要等待确认的任务执行列表。
    */
    private List<Execution> createTaskToWaitFor(List<ExecutionVertex> tasks) {
        // 创建一个新的列表，用于存储需要等待的任务执行实例，初始容量设置为任务的数量
        List<Execution> tasksToAck = new ArrayList<>(tasks.size());
        // 遍历提供的每个任务顶点
        for (ExecutionVertex task : tasks) {
            // 将每个任务顶点的当前执行尝试添加到需要等待的任务列表中
            tasksToAck.add(task.getCurrentExecutionAttempt());
        }
        // 返回创建好的需要等待的任务执行列表
        return tasksToAck;
    }
}
