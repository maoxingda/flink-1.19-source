/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Default implementation of {@link ExecutionDeployer}. */
public class DefaultExecutionDeployer implements ExecutionDeployer {

    private final Logger log;

    private final ExecutionSlotAllocator executionSlotAllocator;

    private final ExecutionOperations executionOperations;

    private final ExecutionVertexVersioner executionVertexVersioner;

    private final Time partitionRegistrationTimeout;

    private final BiConsumer<ExecutionVertexID, AllocationID> allocationReservationFunc;

    private final ComponentMainThreadExecutor mainThreadExecutor;

    private DefaultExecutionDeployer(
            final Logger log,
            final ExecutionSlotAllocator executionSlotAllocator,
            final ExecutionOperations executionOperations,
            final ExecutionVertexVersioner executionVertexVersioner,
            final Time partitionRegistrationTimeout,
            final BiConsumer<ExecutionVertexID, AllocationID> allocationReservationFunc,
            final ComponentMainThreadExecutor mainThreadExecutor) {

        this.log = checkNotNull(log);
        this.executionSlotAllocator = checkNotNull(executionSlotAllocator);
        this.executionOperations = checkNotNull(executionOperations);
        this.executionVertexVersioner = checkNotNull(executionVertexVersioner);
        this.partitionRegistrationTimeout = checkNotNull(partitionRegistrationTimeout);
        this.allocationReservationFunc = checkNotNull(allocationReservationFunc);
        this.mainThreadExecutor = checkNotNull(mainThreadExecutor);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 申请资源并部署
    */
    @Override
    public void allocateSlotsAndDeploy(
            final List<Execution> executionsToDeploy,
            final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex) {
        /** 验证executionsToDeploy列表中的每个Execution对象的状态是否ExecutionState.CREATED */
        validateExecutionStates(executionsToDeploy);
        /** 状态转换为 ExecutionState.SCHEDULED*/
        transitionToScheduled(executionsToDeploy);
        /** todo 申请资源 */
        final Map<ExecutionAttemptID, ExecutionSlotAssignment> executionSlotAssignmentMap =
                allocateSlotsFor(executionsToDeploy);
        /** 构建 ExecutionDeploymentHandle */
        final List<ExecutionDeploymentHandle> deploymentHandles =
                createDeploymentHandles(
                        executionsToDeploy, requiredVersionByVertex, executionSlotAssignmentMap);
        /** 部署执行 */
        waitForAllSlotsAndDeploy(deploymentHandles);
    }

    private void validateExecutionStates(final Collection<Execution> executionsToDeploy) {
        executionsToDeploy.forEach(
                e ->
                        checkState(
                                e.getState() == ExecutionState.CREATED,
                                "Expected execution %s to be in CREATED state, was: %s",
                                e.getAttemptId(),
                                e.getState()));
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 状态转换 将状态转换为SCHEDULED
    */
    private void transitionToScheduled(final List<Execution> executionsToDeploy) {
        executionsToDeploy.forEach(e -> e.transitionState(ExecutionState.SCHEDULED));
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 通过ExecutionSlotAllocator对象实例的allocateSlotsFor方法申请资源
    */
    private Map<ExecutionAttemptID, ExecutionSlotAssignment> allocateSlotsFor(
            final List<Execution> executionsToDeploy) {
        /** 循环获取Execution 中的 ExecutionAttemptID*/
        final List<ExecutionAttemptID> executionAttemptIds =
                executionsToDeploy.stream()
                        .map(Execution::getAttemptId)
                        .collect(Collectors.toList());
        /**
         *通过ExecutionSlotAllocator对象实例的allocateSlotsFor方法申请资源
         */
        return executionSlotAllocator.allocateSlotsFor(executionAttemptIds);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 构建ExecutionDeploymentHandle
     * @param executionsToDeploy 待部署的执行任务列表
     * @param requiredVersionByVertex 映射执行顶点ID到其所需版本的信息
     * @param executionSlotAssignmentMap 映射执行尝试ID到其执行槽分配的信息
     * @return 包含ExecutionDeploymentHandle对象的列表，每个对象代表一个待部署的执行任务及其相关信息
     * @throws IllegalStateException 如果executionsToDeploy和executionSlotAssignmentMap的大小不匹配
     */
    private List<ExecutionDeploymentHandle> createDeploymentHandles(
            final List<Execution> executionsToDeploy,
            final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex,
            final Map<ExecutionAttemptID, ExecutionSlotAssignment> executionSlotAssignmentMap) {
        // 验证executionsToDeploy的大小是否与executionSlotAssignmentMap的大小相等
        checkState(executionsToDeploy.size() == executionSlotAssignmentMap.size());
        // 初始化一个与executionsToDeploy大小相同的ExecutionDeploymentHandle列表
        final List<ExecutionDeploymentHandle> deploymentHandles =
                new ArrayList<>(executionsToDeploy.size());
        // 遍历待部署的执行任务列表
        for (final Execution execution : executionsToDeploy) {
            // 从executionSlotAssignmentMap中获取当前执行任务的执行槽分配信息
            final ExecutionSlotAssignment assignment =
                    checkNotNull(executionSlotAssignmentMap.get(execution.getAttemptId()));
            // 获取当前执行任务的执行顶点ID
            final ExecutionVertexID executionVertexId = execution.getVertex().getID();
            // 创建一个新的ExecutionDeploymentHandle对象，包含当前执行任务、执行槽分配信息和所需版本信息
            final ExecutionDeploymentHandle deploymentHandle =
                    new ExecutionDeploymentHandle(
                            execution, assignment, requiredVersionByVertex.get(executionVertexId));
            // 将新创建的ExecutionDeploymentHandle对象添加到列表中
            deploymentHandles.add(deploymentHandle);
        }
        // 返回包含所有ExecutionDeploymentHandle对象的列表
        return deploymentHandles;
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 该方法用于等待所有插槽分配完毕并进行部署
     *
    */
    private void waitForAllSlotsAndDeploy(final List<ExecutionDeploymentHandle> deploymentHandles) {
        // 调用assignAllResourcesAndRegisterProducedPartitions方法分配所有资源并注册产生的分区
        // 该方法返回一个CompletableFuture，表示资源分配和分区注册的异步操作
        // 然后，通过handle方法连接一个部署操作（deployAll），该操作将在资源分配和分区注册完成后执行
        // handle方法接受一个Function作为参数，该函数将在异步操作完成时（无论是正常完成还是异常）被调用
        // 在这里，deployAll方法是一个异步操作，它将基于已分配的资源和注册的分区进行部署
        // 最后，使用FutureUtils.assertNoException确保在部署过程中没有发生异常
        // 如果在资源分配、分区注册或部署过程中发生异常，该方法将抛出异常
        FutureUtils.assertNoException(
                assignAllResourcesAndRegisterProducedPartitions(deploymentHandles)
                        .handle(deployAll(deploymentHandles)));
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 用于分配所有资源并注册产生的分区
     * // 返回值: CompletableFuture<Void> - 一个代表所有资源分配和分区注册完成情况的Future
    */
    private CompletableFuture<Void> assignAllResourcesAndRegisterProducedPartitions(
            final List<ExecutionDeploymentHandle> deploymentHandles) {
        // 创建一个列表，用于存储每个部署句柄对应的Future
        final List<CompletableFuture<Void>> resultFutures = new ArrayList<>();
        // 遍历每个部署句柄
        for (ExecutionDeploymentHandle deploymentHandle : deploymentHandles) {
            // 获取当前部署句柄的逻辑槽Future
            // 并开始一系列的异步操作：分配资源、注册产生的分区
            final CompletableFuture<Void> resultFuture =
                    deploymentHandle
                            .getLogicalSlotFuture()// 获取逻辑槽的Future
                            .handle(assignResource(deploymentHandle))// 分配资源，并处理逻辑槽的完成或失败
                            .thenCompose(registerProducedPartitions(deploymentHandle))// 注册分区，并等待其完成
                            .handle(// 处理注册分区的完成或失败
                                    (ignore, throwable) -> {
                                        if (throwable != null) {
                                            // 如果发生异常，则处理任务部署失败
                                            handleTaskDeploymentFailure(
                                                    deploymentHandle.getExecution(), throwable);
                                        }
                                        //
                                        return null;
                                    });

            resultFutures.add(resultFuture);
        }
        // 将当前部署句柄的Future添加到结果列表中
        return FutureUtils.waitForAll(resultFutures);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * // 定义一个私有方法deployAll，它接受一个ExecutionDeploymentHandle的列表作为参数，
     * // 并返回一个BiFunction对象，该对象处理Void类型和Throwable类型的参数，并返回Void。
     * // 这个BiFunction通常用于异步操作的回调处理，例如CompletableFuture的exceptionally方法。
    */
    private BiFunction<Void, Throwable, Void> deployAll(
            final List<ExecutionDeploymentHandle> deploymentHandles) {
        // 如果throwable不为空，则传播异常（可能是通过抛出异常或记录日志等方式）
        return (ignored, throwable) -> {
            propagateIfNonNull(throwable);
            // 遍历传入的deploymentHandles列表
            for (final ExecutionDeploymentHandle deploymentHandle : deploymentHandles) {
                // 获取deploymentHandle对应的逻辑槽位分配的CompletableFuture
                final CompletableFuture<LogicalSlot> slotAssigned =
                        deploymentHandle.getLogicalSlotFuture();
                checkState(slotAssigned.isDone());
                // 检查CompletableFuture是否已经完成（即是否已分配了逻辑槽位）
                // 如果未完成，则可能会抛出异常，因为后续代码依赖于已完成的Future  
                FutureUtils.assertNoException(
                        slotAssigned.handle(deployOrHandleError(deploymentHandle)));
            }
            return null;
        };
    }

    private static void propagateIfNonNull(final Throwable throwable) {
        if (throwable != null) {
            throw new CompletionException(throwable);
        }
    }

    private BiFunction<LogicalSlot, Throwable, LogicalSlot> assignResource(
            final ExecutionDeploymentHandle deploymentHandle) {
        // 返回一个BiFunction，它接收一个LogicalSlot和一个Throwable作为参数，并返回一个LogicalSlot
        return (logicalSlot, throwable) -> {
            // 获取所需的ExecutionVertexVersion
            final ExecutionVertexVersion requiredVertexVersion =
                    deploymentHandle.getRequiredVertexVersion();
            // 获取对应的Execution
            final Execution execution = deploymentHandle.getExecution();
            // 检查Execution的状态是否不是SCHEDULED或者所需的ExecutionVertexVersion是否被修改
            if (execution.getState() != ExecutionState.SCHEDULED
                    || executionVertexVersioner.isModified(requiredVertexVersion)) {
                // 如果Execution的状态不是SCHEDULED或Version被修改，且没有发生Throwable异常
                if (throwable == null) {
                    // 在日志中记录拒绝分配slot给该Execution的原因
                    log.debug(
                            "Refusing to assign slot to execution {} because this deployment was "
                                    + "superseded by another deployment",
                            deploymentHandle.getExecutionAttemptId());
                    // 释放已经存在的LogicalSlot
                    releaseSlotIfPresent(logicalSlot);
                }
                // 返回null，表示没有成功分配资源
                return null;
            }

            // throw exception only if the execution version is not outdated.
            // this ensures that canceling a pending slot request does not fail
            // a task which is about to cancel.
            // 如果发生了Throwable异常，且Execution的版本没有过时（即满足分配资源的条件）
            // 抛出CompletionException异常，并可能将原Throwable异常包装为NoResourceAvailableException
            if (throwable != null) {
                throw new CompletionException(maybeWrapWithNoResourceAvailableException(throwable));
            }
            // 尝试将LogicalSlot分配给Execution
            if (!execution.tryAssignResource(logicalSlot)) {
                // 如果分配失败，抛出IllegalStateException异常
                throw new IllegalStateException(
                        "Could not assign resource "
                                + logicalSlot
                                + " to execution "
                                + execution
                                + '.');
            }

            // We only reserve the latest execution of an execution vertex. Because it may cause
            // problems to reserve multiple slots for one execution vertex. Besides that, slot
            // reservation is for local recovery and therefore is only needed by streaming jobs, in
            // which case an execution vertex will have one only current execution.
            /**
             * 我们只保留执行顶点的最新执行。因为为一个执行顶点保留多个插槽可能会导致问题。
             * 除此之外，插槽预留用于本地恢复，因此仅流式作业需要，在这种情况下，执行顶点将只有一个当前执行。
             */
            allocationReservationFunc.accept(
                    execution.getAttemptId().getExecutionVertexId(), logicalSlot.getAllocationId());
            // 返回已经分配给Execution的LogicalSlot
            return logicalSlot;
        };
    }

    private static void releaseSlotIfPresent(@Nullable final LogicalSlot logicalSlot) {
        if (logicalSlot != null) {
            logicalSlot.releaseSlot(null);
        }
    }

    private static Throwable maybeWrapWithNoResourceAvailableException(final Throwable failure) {
        final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(failure);
        if (strippedThrowable instanceof TimeoutException) {
            return new NoResourceAvailableException(
                    "Could not allocate the required slot within slot request timeout. "
                            + "Please make sure that the cluster has enough resources.",
                    failure);
        } else {
            return failure;
        }
    }

    private Function<LogicalSlot, CompletableFuture<Void>> registerProducedPartitions(
            final ExecutionDeploymentHandle deploymentHandle) {

        return logicalSlot -> {
            // a null logicalSlot means the slot assignment is skipped, in which case
            // the produced partition registration process can be skipped as well
            if (logicalSlot != null) {
                final Execution execution = deploymentHandle.getExecution();
                final CompletableFuture<Void> partitionRegistrationFuture =
                        execution.registerProducedPartitions(logicalSlot.getTaskManagerLocation());

                return FutureUtils.orTimeout(
                        partitionRegistrationFuture,
                        partitionRegistrationTimeout.toMilliseconds(),
                        TimeUnit.MILLISECONDS,
                        mainThreadExecutor,
                        String.format(
                                "Registering produced partitions for execution %s timed out after %d ms.",
                                execution.getAttemptId(),
                                partitionRegistrationTimeout.toMilliseconds()));
            } else {
                return FutureUtils.completedVoidFuture();
            }
        };
    }
   /**
    * @授课老师(微信): yi_locus
    * email: 156184212@qq.com
    *部署或者处理操作
   */
    private BiFunction<Object, Throwable, Void> deployOrHandleError(
            final ExecutionDeploymentHandle deploymentHandle) {

        return (ignored, throwable) -> {
            // 获取当前部署所需的ExecutionVertexVersion
            final ExecutionVertexVersion requiredVertexVersion =
                    deploymentHandle.getRequiredVertexVersion();
            // 获取当前部署的Execution对象
            final Execution execution = deploymentHandle.getExecution();
            // 检查Execution的状态是否不是SCHEDULED，或者所需的ExecutionVertexVersion是否被修改
            if (execution.getState() != ExecutionState.SCHEDULED
                    || executionVertexVersioner.isModified(requiredVertexVersion)) {
                // 如果Execution状态不是SCHEDULED或者所需的版本被修改，并且没有抛出异常
                if (throwable == null) {
                    log.debug(
                            "Refusing to assign slot to execution {} because this deployment was "
                                    + "superseded by another deployment",
                            deploymentHandle.getExecutionAttemptId());
                }
                // 返回null，表示没有执行任何操作
                return null;
            }
            // 如果Execution状态是SCHEDULED且所需的版本未被修改
            if (throwable == null) {
                // 则安全地部署任务
                deployTaskSafe(execution);
            } else {
                handleTaskDeploymentFailure(execution, throwable);
            }
            return null;
        };
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 安全的部署
    */
    private void deployTaskSafe(final Execution execution) {
        try {
            // 尝试执行部署操作，这里假设executionOperations是一个已经定义好的对象，
            // 它包含了部署任务所需的方法
            // 调用其deploy方法，将execution对象作为参数传入，进行任务的部署
            executionOperations.deploy(execution);
        } catch (Throwable e) {
            handleTaskDeploymentFailure(execution, e);
        }
    }

    private void handleTaskDeploymentFailure(final Execution execution, final Throwable error) {
        executionOperations.markFailed(execution, error);
    }

    private static class ExecutionDeploymentHandle {

        private final Execution execution;

        private final ExecutionSlotAssignment executionSlotAssignment;

        private final ExecutionVertexVersion requiredVertexVersion;

        ExecutionDeploymentHandle(
                final Execution execution,
                final ExecutionSlotAssignment executionSlotAssignment,
                final ExecutionVertexVersion requiredVertexVersion) {
            this.execution = checkNotNull(execution);
            this.executionSlotAssignment = checkNotNull(executionSlotAssignment);
            this.requiredVertexVersion = checkNotNull(requiredVertexVersion);
        }

        Execution getExecution() {
            return execution;
        }

        ExecutionAttemptID getExecutionAttemptId() {
            return execution.getAttemptId();
        }

        CompletableFuture<LogicalSlot> getLogicalSlotFuture() {
            return executionSlotAssignment.getLogicalSlotFuture();
        }

        ExecutionVertexVersion getRequiredVertexVersion() {
            return requiredVertexVersion;
        }
    }

    /** Factory to instantiate the {@link DefaultExecutionDeployer}. */
    public static class Factory implements ExecutionDeployer.Factory {

        @Override
        public DefaultExecutionDeployer createInstance(
                Logger log,
                ExecutionSlotAllocator executionSlotAllocator,
                ExecutionOperations executionOperations,
                ExecutionVertexVersioner executionVertexVersioner,
                Time partitionRegistrationTimeout,
                BiConsumer<ExecutionVertexID, AllocationID> allocationReservationFunc,
                ComponentMainThreadExecutor mainThreadExecutor) {
            return new DefaultExecutionDeployer(
                    log,
                    executionSlotAllocator,
                    executionOperations,
                    executionVertexVersioner,
                    partitionRegistrationTimeout,
                    allocationReservationFunc,
                    mainThreadExecutor);
        }
    }
}
