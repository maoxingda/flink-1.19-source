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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.dispatcher.JobCancellationFailedException;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceProcessFactory;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.leaderelection.LeadershipLostException;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

/**
 * Leadership runner for the {@link JobMasterServiceProcess}.
 *
 * <p>The responsibility of this component is to manage the leadership of the {@link
 * JobMasterServiceProcess}. This means that the runner will create an instance of the process when
 * it obtains the leadership. The process is stopped once the leadership is revoked.
 *
 * <p>This component only accepts signals (job result completion, initialization failure) as long as
 * it is running and as long as the signals are coming from the current leader process. This ensures
 * that only the current leader can affect this component.
 *
 * <p>All leadership operations are serialized. This means that granting the leadership has to
 * complete before the leadership can be revoked and vice versa.
 *
 * <p>The {@link #resultFuture} can be completed with the following values: * *
 *
 * <ul>
 *   <li>{@link JobManagerRunnerResult} to signal an initialization failure of the {@link
 *       JobMasterService} or the completion of a job
 *   <li>{@link Exception} to signal an unexpected failure
 * </ul>
 */
public class JobMasterServiceLeadershipRunner implements JobManagerRunner, LeaderContender {

    private static final Logger LOG =
            LoggerFactory.getLogger(JobMasterServiceLeadershipRunner.class);

    private final Object lock = new Object();

    private final JobMasterServiceProcessFactory jobMasterServiceProcessFactory;

    private final LeaderElection leaderElection;

    private final JobResultStore jobResultStore;

    private final LibraryCacheManager.ClassLoaderLease classLoaderLease;

    private final FatalErrorHandler fatalErrorHandler;

    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

    private final CompletableFuture<JobManagerRunnerResult> resultFuture =
            new CompletableFuture<>();

    @GuardedBy("lock")
    private State state = State.RUNNING;

    @GuardedBy("lock")
    private CompletableFuture<Void> sequentialOperation = FutureUtils.completedVoidFuture();

    @GuardedBy("lock")
    private JobMasterServiceProcess jobMasterServiceProcess =
            JobMasterServiceProcess.waitingForLeadership();

    @GuardedBy("lock")
    private CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = new CompletableFuture<>();

    @GuardedBy("lock")
    private boolean hasCurrentLeaderBeenCancelled = false;

    public JobMasterServiceLeadershipRunner(
            JobMasterServiceProcessFactory jobMasterServiceProcessFactory,
            LeaderElection leaderElection,
            JobResultStore jobResultStore,
            LibraryCacheManager.ClassLoaderLease classLoaderLease,
            FatalErrorHandler fatalErrorHandler) {
        this.jobMasterServiceProcessFactory = jobMasterServiceProcessFactory;
        this.leaderElection = leaderElection;
        this.jobResultStore = jobResultStore;
        this.classLoaderLease = classLoaderLease;
        this.fatalErrorHandler = fatalErrorHandler;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        final CompletableFuture<Void> processTerminationFuture;
        synchronized (lock) {
            if (state == State.STOPPED) {
                return terminationFuture;
            }

            state = State.STOPPED;

            LOG.debug("Terminating the leadership runner for job {}.", getJobID());

            jobMasterGatewayFuture.completeExceptionally(
                    new FlinkException(
                            "JobMasterServiceLeadershipRunner is closed. Therefore, the corresponding JobMaster will never acquire the leadership."));

            resultFuture.complete(
                    JobManagerRunnerResult.forSuccess(
                            createExecutionGraphInfoWithJobStatus(JobStatus.SUSPENDED)));

            processTerminationFuture = jobMasterServiceProcess.closeAsync();
        }

        final CompletableFuture<Void> serviceTerminationFuture =
                FutureUtils.runAfterwards(
                        processTerminationFuture,
                        () -> {
                            classLoaderLease.release();
                            leaderElection.close();
                        });

        FutureUtils.forward(serviceTerminationFuture, terminationFuture);

        terminationFuture.whenComplete(
                (unused, throwable) ->
                        LOG.debug("Leadership runner for job {} has been terminated.", getJobID()));
        return terminationFuture;
    }

    @Override
    public void start() throws Exception {
        LOG.debug("Start leadership runner for job {}.", getJobID());
        leaderElection.startLeaderElection(this);
    }

    @Override
    public CompletableFuture<JobMasterGateway> getJobMasterGateway() {
        synchronized (lock) {
            return jobMasterGatewayFuture;
        }
    }

    @Override
    public CompletableFuture<JobManagerRunnerResult> getResultFuture() {
        return resultFuture;
    }

    @Override
    public JobID getJobID() {
        return jobMasterServiceProcessFactory.getJobId();
    }

    @Override
    public CompletableFuture<Acknowledge> cancel(Time timeout) {
        synchronized (lock) {
            hasCurrentLeaderBeenCancelled = true;
            return getJobMasterGateway()
                    .thenCompose(jobMasterGateway -> jobMasterGateway.cancel(timeout))
                    .exceptionally(
                            e -> {
                                throw new CompletionException(
                                        new JobCancellationFailedException(
                                                "Cancellation failed.",
                                                ExceptionUtils.stripCompletionException(e)));
                            });
        }
    }

    @Override
    public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
        return requestJob(timeout)
                .thenApply(
                        executionGraphInfo ->
                                executionGraphInfo.getArchivedExecutionGraph().getState());
    }

    @Override
    public CompletableFuture<JobDetails> requestJobDetails(Time timeout) {
        return requestJob(timeout)
                .thenApply(
                        executionGraphInfo ->
                                JobDetails.createDetailsForJob(
                                        executionGraphInfo.getArchivedExecutionGraph()));
    }

    @Override
    public CompletableFuture<ExecutionGraphInfo> requestJob(Time timeout) {
        synchronized (lock) {
            if (state == State.RUNNING) {
                if (jobMasterServiceProcess.isInitializedAndRunning()) {
                    return getJobMasterGateway()
                            .thenCompose(jobMasterGateway -> jobMasterGateway.requestJob(timeout));
                } else {
                    return CompletableFuture.completedFuture(
                            createExecutionGraphInfoWithJobStatus(
                                    hasCurrentLeaderBeenCancelled
                                            ? JobStatus.CANCELLING
                                            : JobStatus.INITIALIZING));
                }
            } else {
                return resultFuture.thenApply(JobManagerRunnerResult::getExecutionGraphInfo);
            }
        }
    }

    @Override
    public boolean isInitialized() {
        synchronized (lock) {
            return jobMasterServiceProcess.isInitializedAndRunning();
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 启动JobMasterService
    */
    @Override
    public void grantLeadership(UUID leaderSessionID) {
        runIfStateRunning(
                () -> startJobMasterServiceProcessAsync(leaderSessionID),
                "starting a new JobMasterServiceProcess");
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 用于异步启动作业主服务进程
    */
    @GuardedBy("lock")
    private void startJobMasterServiceProcessAsync(UUID leaderSessionId) {
        sequentialOperation =
                /**
                 * thenCompose 是一个方法，它接收一个函数作为参数，该函数返回另一个 CompletableFuture
                 * thenCompose 会等待第一个 CompletableFuture 完成，然后执行提供的函数，并返回新 CompletableFuture 的结果
                 */
                sequentialOperation.thenCompose(
                        unused ->
                                supplyAsyncIfValidLeader(
                                                leaderSessionId,
                                                () ->
                                                        /**
                                                         * 调用 jobResultStore.hasJobResultEntryAsync(getJobID())。
                                                         * 这个表达式似乎用于异步检查作业结果存储中是否存在特定作业的结果。
                                                         */
                                                        jobResultStore.hasJobResultEntryAsync(
                                                                getJobID()),
                                                () ->
                                                        /**
                                                         * 如果leaderSessionId无效，则创建一个异常完成的 Future，异常为 LeadershipLostException。
                                                         */
                                                        FutureUtils.completedExceptionally(
                                                                new LeadershipLostException(
                                                                        "The leadership is lost.")))
                                        .handle(
                                                (hasJobResult, throwable) -> {
                                                    /**
                                                     * 在 handle 方法中，代码检查返回的 throwable 是否为 LeadershipLostException 的实例：
                                                     */
                                                    if (throwable
                                                            instanceof LeadershipLostException) {
                                                        /**
                                                         * 如果是，则调用 printLogIfNotValidLeader 方法打印日志，并返回 null。
                                                         */
                                                        printLogIfNotValidLeader(
                                                                "verify job result entry",
                                                                leaderSessionId);
                                                        return null;
                                                        /**
                                                         * 如果不是 LeadershipLostException 但 throwable 不为 null，
                                                         * 则使用 ExceptionUtils.rethrow(throwable) 重新抛出异常。
                                                         */
                                                    } else if (throwable != null) {
                                                        ExceptionUtils.rethrow(throwable);
                                                    }
                                                    if (hasJobResult) {
                                                        /**
                                                         * 如果 hasJobResult 为 true，则调用 handleJobAlreadyDoneIfValidLeader 方法。
                                                         * 这个方法用于处理作业已经完成的情况，并且只在当前领导者有效时执行。
                                                         */
                                                        handleJobAlreadyDoneIfValidLeader(
                                                                leaderSessionId);
                                                    } else {
                                                        /**
                                                         * 如果 hasJobResult 为 false，则调用 createNewJobMasterServiceProcessIfValidLeader 方法。
                                                         * 这个方法用于启动新的作业主服务进程。
                                                         */
                                                        createNewJobMasterServiceProcessIfValidLeader(
                                                                leaderSessionId);
                                                    }
                                                    /**
                                                     * return null; 表示该 Lambda 表达式不返回任何有用的结果。
                                                     * 因为该方法的主要目的是执行一些操作（如启动服务进程或处理已完成的作业），而不是返回特定的结果。
                                                     */
                                                    return null;
                                                }));
        /**
         * 用于处理异步操作错误的实用方法
         */
        handleAsyncOperationError(sequentialOperation, "Could not start the job manager.");
    }

    private void handleJobAlreadyDoneIfValidLeader(UUID leaderSessionId) {
        runIfValidLeader(
                leaderSessionId, () -> jobAlreadyDone(leaderSessionId), "check completed job");
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *
    */
    private void createNewJobMasterServiceProcessIfValidLeader(UUID leaderSessionId) {
        /**
         * 检查当前leaderSessionId是否为有效
         */
        runIfValidLeader(
                leaderSessionId,
                /**如果 Leader有效则执行下面的方法*/
                () ->
                        ThrowingRunnable.unchecked(
                                        /**
                                         * 调用createNewJobMasterServiceProcess创建JobMasterService
                                         */
                                        () -> createNewJobMasterServiceProcess(leaderSessionId))
                                .run(),
                /** 如果leader无效则执行下面*/
                "create new job master service process");
    }

    private void printLogIfNotValidLeader(String actionDescription, UUID leaderSessionId) {
        LOG.debug(
                "Ignore leader action '{}' because the leadership runner is no longer the valid leader for {}.",
                actionDescription,
                leaderSessionId);
    }

    private ExecutionGraphInfo createExecutionGraphInfoWithJobStatus(JobStatus jobStatus) {
        return new ExecutionGraphInfo(
                jobMasterServiceProcessFactory.createArchivedExecutionGraph(jobStatus, null));
    }

    private void jobAlreadyDone(UUID leaderSessionId) {
        LOG.info(
                "{} for job {} was granted leadership with leader id {}, but job was already done.",
                getClass().getSimpleName(),
                getJobID(),
                leaderSessionId);
        resultFuture.complete(
                JobManagerRunnerResult.forSuccess(
                        new ExecutionGraphInfo(
                                jobMasterServiceProcessFactory.createArchivedExecutionGraph(
                                        JobStatus.FAILED,
                                        new JobAlreadyDoneException(getJobID())))));
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 用于创建一个新的 JobMasterServiceProcess 实例
    */
    @GuardedBy("lock")
    private void createNewJobMasterServiceProcess(UUID leaderSessionId) throws FlinkException {
        /**
         * 首先检查 jobMasterServiceProcess（如果存在）是否已异步关闭并且关闭操作已经完成
         */
        Preconditions.checkState(jobMasterServiceProcess.closeAsync().isDone());

        LOG.info(
                "{} for job {} was granted leadership with leader id {}. Creating new {}.",
                getClass().getSimpleName(),
                getJobID(),
                leaderSessionId,
                JobMasterServiceProcess.class.getSimpleName());
        /**
         * 通过调用 jobMasterServiceProcessFactory 的 create 方法，使用 leaderSessionId 创建一个新的 JobMasterServiceProcess 实例，
         * 并赋值给 jobMasterServiceProcess 变量。
         */
        jobMasterServiceProcess = jobMasterServiceProcessFactory.create(leaderSessionId);

        forwardIfValidLeader(
                leaderSessionId,
                jobMasterServiceProcess.getJobMasterGatewayFuture(),
                jobMasterGatewayFuture,
                "JobMasterGatewayFuture from JobMasterServiceProcess");
        /**
         * 校验创建是否成功，并封装异步结果进行返回
         */
        forwardResultFuture(leaderSessionId, jobMasterServiceProcess.getResultFuture());
        /**
         * 调用 confirmLeadership 方法，使用 leaderSessionId 和 jobMasterServiceProcess 中的 LeaderAddressFuture 来确认领导权
         * 确认LeaderContender已接受由给定的LeaderId标识的领导者
         * 内部调用leaderElection.confirmLeadership
         */
        confirmLeadership(leaderSessionId, jobMasterServiceProcess.getLeaderAddressFuture());
    }

    private void confirmLeadership(
            UUID leaderSessionId, CompletableFuture<String> leaderAddressFuture) {
        FutureUtils.assertNoException(
                leaderAddressFuture.thenAccept(
                        address ->
                                runIfValidLeader(
                                        leaderSessionId,
                                        () -> {
                                            LOG.debug("Confirm leadership {}.", leaderSessionId);
                                            leaderElection.confirmLeadership(
                                                    leaderSessionId, address);
                                        },
                                        "confirming leadership")));
    }

    private void forwardResultFuture(
            UUID leaderSessionId, CompletableFuture<JobManagerRunnerResult> resultFuture) {
        /**
         * resultFuture.whenComplete这是 CompletableFuture 的一个方法，
         * 它接受一个双参数函数作为参数。这个函数会在 resultFuture 完成时被调用，
         * 无论是正常完成还是异常完成。
         */
        resultFuture.whenComplete(
                (jobManagerRunnerResult, throwable) ->
                        runIfValidLeader(
                                leaderSessionId,
                                () -> onJobCompletion(jobManagerRunnerResult, throwable),
                                "result future forwarding"));
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 处理作业完成后的逻辑
    */
    @GuardedBy("lock")
    private void onJobCompletion(
            JobManagerRunnerResult jobManagerRunnerResult, Throwable throwable) {
        /** 将状态设置为 JOB_COMPLETED，表示作业已经完成。 */
        state = State.JOB_COMPLETED;

        LOG.debug("Completing the result for job {}.", getJobID());
        /**
         * 检查 throwable 是否为 null，以此判断作业是否异常完成。
         */
        if (throwable != null) {
            /**
             * 如果作业异常完成，使用 throwable 来异常地完成 resultFuture。
             */
            resultFuture.completeExceptionally(throwable);
            jobMasterGatewayFuture.completeExceptionally(
                    new FlinkException(
                            "Could not retrieve JobMasterGateway because the JobMaster failed.",
                            throwable));
        } else {
            /**
             * 检查 jobManagerRunnerResult 是否表示成功。
             */
            if (!jobManagerRunnerResult.isSuccess()) {
                /**
                 * 如果作业管理器初始化失败，则异常地完成 jobMasterGatewayFuture，
                 * 并传递一个包含初始化失败原因的新 FlinkException。
                  */
                jobMasterGatewayFuture.completeExceptionally(
                        new FlinkException(
                                "Could not retrieve JobMasterGateway because the JobMaster initialization failed.",
                                jobManagerRunnerResult.getInitializationFailure()));
            }
            /**
             * 如果作业成功完成，则使用 jobManagerRunnerResult 来正常地完成 resultFuture。
             */
            resultFuture.complete(jobManagerRunnerResult);
        }
    }

    @Override
    public void revokeLeadership() {
        runIfStateRunning(
                this::stopJobMasterServiceProcessAsync,
                "revoke leadership from JobMasterServiceProcess");
    }

    @GuardedBy("lock")
    private void stopJobMasterServiceProcessAsync() {
        sequentialOperation =
                sequentialOperation.thenCompose(
                        ignored ->
                                callIfRunning(
                                                this::stopJobMasterServiceProcess,
                                                "stop leading JobMasterServiceProcess")
                                        .orElse(FutureUtils.completedVoidFuture()));

        handleAsyncOperationError(sequentialOperation, "Could not suspend the job manager.");
    }

    @GuardedBy("lock")
    private CompletableFuture<Void> stopJobMasterServiceProcess() {
        LOG.info(
                "{} for job {} was revoked leadership with leader id {}. Stopping current {}.",
                getClass().getSimpleName(),
                getJobID(),
                jobMasterServiceProcess.getLeaderSessionId(),
                JobMasterServiceProcess.class.getSimpleName());

        jobMasterGatewayFuture.completeExceptionally(
                new FlinkException(
                        "Cannot obtain JobMasterGateway because the JobMaster lost leadership."));
        jobMasterGatewayFuture = new CompletableFuture<>();

        hasCurrentLeaderBeenCancelled = false;

        return jobMasterServiceProcess.closeAsync();
    }

    @Override
    public void handleError(Exception exception) {
        fatalErrorHandler.onFatalError(exception);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 处理异步操作可能产生的错误
    */
    private void handleAsyncOperationError(CompletableFuture<Void> operation, String message) {
        /**
         * 调用 CompletableFuture 的 whenComplete 方法，该方法会在异步操作完成（无论成功还是失败）时执行提供的回调函数。
         */
        operation.whenComplete(
                (unused, throwable) -> {
                    /**
                     * 检查 throwable 是否非空，即异步操作是否抛出了异常。
                     */
                    if (throwable != null) {
                        /**
                         * 检查当前组件或服务的状态，如果状态为 "running"，
                         */
                        runIfStateRunning(
                                () ->
                                        /**
                                         * 专门用于处理 JobMasterServiceLeadershipRunner 相关错误的函数。它接受一个 FlinkException 对象作为参数
                                         */
                                        handleJobMasterServiceLeadershipRunnerError(
                                                new FlinkException(message, throwable)),
                                "handle JobMasterServiceLeadershipRunner error");
                    }
                });
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 用于处理与 JobMasterServiceLeadershipRunner 相关的错误。
     * 该方法接收一个 Throwable 对象作为参数，这个 Throwable 对象表示错误或异常的原因。
    */
    private void handleJobMasterServiceLeadershipRunnerError(Throwable cause) {
        /**
         * 检查传入的 cause 是否是 JVM 致命错误
         */
        if (ExceptionUtils.isJvmFatalError(cause)) {
            /**
             * 如果 cause 是一个 JVM 致命错误，那么方法会调用 fatalErrorHandler.onFatalError(cause)。
             */
            fatalErrorHandler.onFatalError(cause);
        } else {
            /**
             * 如果 cause 不是 JVM 致命错误，
             * CompletableFuture 对象，它代表了一个异步操作的结果
             */
            resultFuture.completeExceptionally(cause);
        }
    }

    private void runIfStateRunning(Runnable action, String actionDescription) {
        synchronized (lock) {
            if (isRunning()) {
                action.run();
            } else {
                LOG.debug(
                        "Ignore '{}' because the leadership runner is no longer running.",
                        actionDescription);
            }
        }
    }

    private <T> Optional<T> callIfRunning(
            Supplier<? extends T> supplier, String supplierDescription) {
        synchronized (lock) {
            if (isRunning()) {
                return Optional.of(supplier.get());
            } else {
                LOG.debug(
                        "Ignore '{}' because the leadership runner is no longer running.",
                        supplierDescription);
                return Optional.empty();
            }
        }
    }

    @GuardedBy("lock")
    private boolean isRunning() {
        return state == State.RUNNING;
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 同步方法，用于在确认当前会话是有效领导者时执行某个操作，如果不是领导者，则执行备选操作。
     *  expectedLeaderId：期望的领导者会话的 UUID。
     *  action：如果当前会话是有效领导者，则执行的 Runnable 对象。
     *  noLeaderFallback：如果当前会话不是领导者，则执行的备选 Runnable 对象。
     *
    */
    private void runIfValidLeader(
            UUID expectedLeaderId, Runnable action, Runnable noLeaderFallback) {
        /**
         * @授课老师(微信): yi_locus
         * email: 156184212@qq.com
         * synchronized (lock) 语句确保同一时间只有一个线程可以执行此代码块
        */
        synchronized (lock) {
            /**
             * 如果当前leader有效
             */
            if (isValidLeader(expectedLeaderId)) {
                /**  action.run() 会被执行 */
                action.run();
            } else {
                /** noLeaderFallback.run() */
                noLeaderFallback.run();
            }
        }
    }

    private void runIfValidLeader(
            UUID expectedLeaderId, Runnable action, String noLeaderFallbackCommandDescription) {
        runIfValidLeader(
                expectedLeaderId,
                action,
                () ->
                        printLogIfNotValidLeader(
                                noLeaderFallbackCommandDescription, expectedLeaderId));
    }

    private <T> CompletableFuture<T> supplyAsyncIfValidLeader(
            UUID expectedLeaderId,
            Supplier<CompletableFuture<T>> supplier,
            Supplier<CompletableFuture<T>> noLeaderFallback) {
        final CompletableFuture<T> resultFuture = new CompletableFuture<>();
        runIfValidLeader(
                expectedLeaderId,
                () -> FutureUtils.forward(supplier.get(), resultFuture),
                () -> FutureUtils.forward(noLeaderFallback.get(), resultFuture));

        return resultFuture;
    }

    @GuardedBy("lock")
    private boolean isValidLeader(UUID expectedLeaderId) {
        return isRunning()
                && leaderElection != null
                && leaderElection.hasLeadership(expectedLeaderId);
    }

    private <T> void forwardIfValidLeader(
            UUID expectedLeaderId,
            CompletableFuture<? extends T> source,
            CompletableFuture<T> target,
            String forwardDescription) {
        source.whenComplete(
                (t, throwable) ->
                        runIfValidLeader(
                                expectedLeaderId,
                                () -> {
                                    if (throwable != null) {
                                        target.completeExceptionally(throwable);
                                    } else {
                                        target.complete(t);
                                    }
                                },
                                forwardDescription));
    }

    enum State {
        RUNNING,
        STOPPED,
        JOB_COMPLETED,
    }
}
