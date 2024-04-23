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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.client.JobInitializationException;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceFactory;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Default {@link JobMasterServiceProcess} which is responsible for creating and running a {@link
 * JobMasterService}. The process is responsible for receiving the signals from the {@link
 * JobMasterService} and to create the respective {@link JobManagerRunnerResult} from it.
 *
 * <p>The {@link JobMasterService} can be created asynchronously and the creation can also fail.
 * That is why the process needs to observe the creation operation and complete the {@link
 * #resultFuture} with an initialization failure.
 *
 * <p>The {@link #resultFuture} can be completed with the following values:
 *
 * <ul>
 *   <li>{@link JobManagerRunnerResult} to signal an initialization failure of the {@link
 *       JobMasterService} or the completion of a job
 *   <li>{@link JobNotFinishedException} to signal that the job has not been completed by the {@link
 *       JobMasterService}
 *   <li>{@link Exception} to signal an unexpected failure
 * </ul>
 */
public class DefaultJobMasterServiceProcess
        implements JobMasterServiceProcess, OnCompletionActions {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultJobMasterServiceProcess.class);

    private final Object lock = new Object();

    private final JobID jobId;

    private final UUID leaderSessionId;

    private final CompletableFuture<JobMasterService> jobMasterServiceFuture;

    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

    private final CompletableFuture<JobManagerRunnerResult> resultFuture =
            new CompletableFuture<>();

    private final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture =
            new CompletableFuture<>();

    private final CompletableFuture<String> leaderAddressFuture = new CompletableFuture<>();

    @GuardedBy("lock")
    private boolean isRunning = true;
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 构造函数创建了一个新的DefaultJobMasterServiceProcess对象，
     * 该对象负责管理分布式作业中的作业主服务（JobMasterService）
     *  jobId: 作业的唯一标识符。
     *  leaderSessionId: 当前领导者的会话ID。
     *  jobMasterServiceFactory: 用于创建JobMasterService的工厂对象。
     *  failedArchivedExecutionGraphFactory: 一个函数式接口，它接受一个Throwable（异常）并返回一个ArchivedExecutionGraph。这个函数在作业失败时被用来创建归档的执行图。
     *
    */
    public DefaultJobMasterServiceProcess(
            JobID jobId,
            UUID leaderSessionId,
            JobMasterServiceFactory jobMasterServiceFactory,
            Function<Throwable, ArchivedExecutionGraph> failedArchivedExecutionGraphFactory) {
        /**
         * 构造函数将传入的jobId和leaderSessionId赋值给对象的成员变量。
         */
        this.jobId = jobId;
        this.leaderSessionId = leaderSessionId;
        /**
         * jobMasterServiceFactory工厂对象来创建一个JobMasterService的CompletableFuture。
         * 这个CompletableFuture代表一个异步计算的结果，这里指的是JobMasterService的创建过程。
         */
        this.jobMasterServiceFuture =
                jobMasterServiceFactory.createJobMasterService(leaderSessionId, this);
        /**
         * 当jobMasterServiceFuture完成时（即JobMasterService创建成功或失败），
         * whenComplete方法中的lambda表达式会被执行。
         */
        jobMasterServiceFuture.whenComplete(
                (jobMasterService, throwable) -> {
                    /** 如果throwable不为null（表示在创建JobMasterService时发生了异常）： */
                    if (throwable != null) {
                        /**
                         * 创建一个JobInitializationException异常，其中包含作业ID、错误消息和原始的throwable。
                         */
                        final JobInitializationException jobInitializationException =
                                new JobInitializationException(
                                        jobId, "Could not start the JobMaster.", throwable);
                        /**
                         * 记录日志
                          */
                        LOG.debug(
                                "Initialization of the JobMasterService for job {} under leader id {} failed.",
                                jobId,
                                leaderSessionId,
                                jobInitializationException);
                        /**
                         * 调用resultFuture.complete方法，将初始化失败的结果传递给resultFuture。
                         * 总结返回：JobManagerRunnerResult内部异常是初始化异常
                         */
                        resultFuture.complete(
                                JobManagerRunnerResult.forInitializationFailure(
                                        /**
                                         * 使用failedArchivedExecutionGraphFactory函数创建一个归档的执行图。
                                         */
                                        new ExecutionGraphInfo(
                                                failedArchivedExecutionGraphFactory.apply(
                                                        jobInitializationException)),
                                        jobInitializationException));
                    /**
                     * 如果throwable为null（表示JobMasterService创建成功）：
                     */
                    } else {
                        /**
                         * 调用registerJobMasterServiceFutures方法
                         * 来注册这个新创建的JobMasterService。
                         */
                        registerJobMasterServiceFutures(jobMasterService);
                    }
                });
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 主要作用是注册并处理JobMasterService相关的异步操作结果
    */
    private void registerJobMasterServiceFutures(JobMasterService jobMasterService) {
        LOG.debug(
                "Successfully created the JobMasterService for job {} under leader id {}.",
                jobId,
                leaderSessionId);
        /**
         * 获取JobMasterService的终止Future（getTerminationFuture()）。
         * 这个Future代表JobMasterService的终止状态，当JobMasterService终止时，这个Future会完成。
         *  将jobMasterService.getGateway()设置给jobMasterGatewayFuture
         *  将jobMasterService.getAddress()设置给leaderAddressFuture
         */
        jobMasterGatewayFuture.complete(jobMasterService.getGateway());

        leaderAddressFuture.complete(jobMasterService.getAddress());
        /**
         * 使用whenComplete方法来注册一个回调函数，当JobMasterService终止时（无论是正常还是异常），该回调函数都会被调用。
         */
        jobMasterService
                .getTerminationFuture()
                 /** 返回异常的CompletableFuture*/
                .whenComplete(
                        (unused, throwable) -> {
                            synchronized (lock) {
                                /**
                                 * 检查isRunning标志。如果isRunning为true（表示作业正在运行），
                                 * 则记录一条warn级别的日志，
                                 * 并调用jobMasterFailed方法来处理JobMasterService的意外终止。
                                 */
                                if (isRunning) {
                                    LOG.warn(
                                            "Unexpected termination of the JobMasterService for job {} under leader id {}.",
                                            jobId,
                                            leaderSessionId);
                                    jobMasterFailed(
                                            new FlinkException(
                                                    "Unexpected termination of the JobMasterService.",
                                                    throwable));
                                }
                            }
                        });
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (isRunning) {
                isRunning = false;

                LOG.debug(
                        "Terminating the JobMasterService process for job {} under leader id {}.",
                        jobId,
                        leaderSessionId);

                resultFuture.completeExceptionally(new JobNotFinishedException(jobId));
                jobMasterGatewayFuture.completeExceptionally(
                        new FlinkException("Process has been closed."));

                jobMasterServiceFuture.whenComplete(
                        (jobMasterService, throwable) -> {
                            if (throwable != null) {
                                // JobMasterService creation has failed. Nothing to stop then :-)
                                terminationFuture.complete(null);
                            } else {
                                FutureUtils.forward(
                                        jobMasterService.closeAsync(), terminationFuture);
                            }
                        });

                terminationFuture.whenComplete(
                        (unused, throwable) ->
                                LOG.debug(
                                        "JobMasterService process for job {} under leader id {} has been terminated.",
                                        jobId,
                                        leaderSessionId));
            }
        }
        return terminationFuture;
    }

    @Override
    public UUID getLeaderSessionId() {
        return leaderSessionId;
    }

    @Override
    public boolean isInitializedAndRunning() {
        synchronized (lock) {
            return jobMasterServiceFuture.isDone()
                    && !jobMasterServiceFuture.isCompletedExceptionally()
                    && isRunning;
        }
    }

    @Override
    public CompletableFuture<JobMasterGateway> getJobMasterGatewayFuture() {
        return jobMasterGatewayFuture;
    }

    @Override
    public CompletableFuture<JobManagerRunnerResult> getResultFuture() {
        return resultFuture;
    }

    @Override
    public CompletableFuture<String> getLeaderAddressFuture() {
        return leaderAddressFuture;
    }

    @Override
    public void jobReachedGloballyTerminalState(ExecutionGraphInfo executionGraphInfo) {
        LOG.debug(
                "Job {} under leader id {} reached a globally terminal state {}.",
                jobId,
                leaderSessionId,
                executionGraphInfo.getArchivedExecutionGraph().getState());
        resultFuture.complete(JobManagerRunnerResult.forSuccess(executionGraphInfo));
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 该类负责处理与作业管理（JobMaster）相关的逻辑，并且在作业管理失败时被调用。
    */
    @Override
    public void jobMasterFailed(Throwable cause) {
        /**
         * 记录日志
         */
        LOG.debug("Job {} under leader id {} failed.", jobId, leaderSessionId);
        /**
         * 调用了resultFuture对象的completeExceptionally方法，并传递了导致失败的cause。
         * 调用completeExceptionally方法意味着将这个未来对象标记为已完成，但是是以异常的方式完成，
         * 即表示操作失败，并将失败的原因（cause）传递给等待这个未来的任何代码。
         */
        resultFuture.completeExceptionally(cause);
    }
}
