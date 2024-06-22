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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinatorHolder;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Default handler for the {@link OperatorCoordinator OperatorCoordinators}. */
public class DefaultOperatorCoordinatorHandler implements OperatorCoordinatorHandler {
    private final ExecutionGraph executionGraph;

    private final Map<OperatorID, OperatorCoordinatorHolder> coordinatorMap;

    private final GlobalFailureHandler globalFailureHandler;

    public DefaultOperatorCoordinatorHandler(
            ExecutionGraph executionGraph, GlobalFailureHandler globalFailureHandler) {
        this.executionGraph = executionGraph;

        this.coordinatorMap = createCoordinatorMap(executionGraph);
        this.globalFailureHandler = globalFailureHandler;
    }

    private static Map<OperatorID, OperatorCoordinatorHolder> createCoordinatorMap(
            ExecutionGraph executionGraph) {
        return executionGraph.getAllVertices().values().stream()
                .filter(ExecutionJobVertex::isInitialized)
                .flatMap(v -> v.getOperatorCoordinators().stream())
                .collect(
                        Collectors.toMap(
                                OperatorCoordinatorHolder::operatorId, Function.identity()));
    }

    @Override
    public void initializeOperatorCoordinators(ComponentMainThreadExecutor mainThreadExecutor) {
        for (OperatorCoordinatorHolder coordinatorHolder : coordinatorMap.values()) {
            coordinatorHolder.lazyInitialize(
                    globalFailureHandler,
                    mainThreadExecutor,
                    executionGraph.getCheckpointCoordinator());
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 启动所有Operator Coordinators
    */
    @Override
    public void startAllOperatorCoordinators() {
        startOperatorCoordinators(coordinatorMap.values());
    }

    @Override
    public void disposeAllOperatorCoordinators() {
        coordinatorMap.values().forEach(IOUtils::closeQuietly);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *
    */
    @Override
    public void deliverOperatorEventToCoordinator(
            final ExecutionAttemptID taskExecutionId,
            final OperatorID operatorId,
            final OperatorEvent evt)
            throws FlinkException {

        // Failure semantics (as per the javadocs of the method):
        // If the task manager sends an event for a non-running task or an non-existing operator
        // coordinator, then respond with an exception to the call. If task and coordinator exist,
        // then we assume that the call from the TaskManager was valid, and any bubbling exception
        // needs to cause a job failure.

        // 从 ExecutionGraph 中获取已注册的执行
        final Execution exec = executionGraph.getRegisteredExecutions().get(taskExecutionId);
        if (exec == null
                || exec.getState() != ExecutionState.RUNNING
                        && exec.getState() != ExecutionState.INITIALIZING) {
            // This situation is common when cancellation happens, or when the task failed while the
            // event was just being dispatched asynchronously on the TM side.
            // It should be fine in those expected situations to just ignore this event, but, to be
            // on the safe, we notify the TM that the event could not be delivered.

            // 这种情况在取消发生时很常见，或者在事件正在任务管理器端异步分派时任务失败。
            // 在这些预期情况下，只需忽略此事件就可以了，但为了安全起见，
            // 我们通知任务管理器该事件无法送达。
            throw new TaskNotRunningException(
                    "Task is not known or in state running on the JobManager.");
        }
        // 从协调器映射中获取 OperatorCoordinatorHolder
        final OperatorCoordinatorHolder coordinator = coordinatorMap.get(operatorId);
        if (coordinator == null) {
            // 如果没有为给定的操作符 ID 注册协调器，则抛出异常
            throw new FlinkException("No coordinator registered for operator " + operatorId);
        }

        try {
            // 将事件传递给协调器处理
            coordinator.handleEventFromOperator(
                    exec.getParallelSubtaskIndex(), exec.getAttemptNumber(), evt);
        } catch (Throwable t) {
            // 如果处理过程中发生异常，检查是否为致命错误或内存溢出
            ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
            globalFailureHandler.handleGlobalFailure(t);
        }
    }

    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operator, CoordinationRequest request) throws FlinkException {

        final OperatorCoordinatorHolder coordinatorHolder = coordinatorMap.get(operator);
        if (coordinatorHolder == null) {
            throw new FlinkException(
                    "Coordinator of operator "
                            + operator
                            + " does not exist or the job vertex this operator belongs to is not initialized.");
        }

        final OperatorCoordinator coordinator = coordinatorHolder.coordinator();
        if (coordinator instanceof CoordinationRequestHandler) {
            return ((CoordinationRequestHandler) coordinator).handleCoordinationRequest(request);
        } else {
            throw new FlinkException(
                    "Coordinator of operator " + operator + " cannot handle client event");
        }
    }

    @Override
    public void registerAndStartNewCoordinators(
            Collection<OperatorCoordinatorHolder> coordinators,
            ComponentMainThreadExecutor mainThreadExecutor,
            final int parallelism) {

        for (OperatorCoordinatorHolder coordinator : coordinators) {
            coordinatorMap.put(coordinator.operatorId(), coordinator);
            coordinator.lazyInitialize(
                    globalFailureHandler,
                    mainThreadExecutor,
                    executionGraph.getCheckpointCoordinator(),
                    parallelism);
        }
        startOperatorCoordinators(coordinators);
    }

    private void startOperatorCoordinators(Collection<OperatorCoordinatorHolder> coordinators) {
        try {
            for (OperatorCoordinatorHolder coordinator : coordinators) {
                coordinator.start();
            }
        } catch (Throwable t) {
            ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
            coordinators.forEach(IOUtils::closeQuietly);
            throw new FlinkRuntimeException("Failed to start the operator coordinators", t);
        }
    }

    @VisibleForTesting
    Map<OperatorID, OperatorCoordinatorHolder> getCoordinatorMap() {
        return coordinatorMap;
    }
}
