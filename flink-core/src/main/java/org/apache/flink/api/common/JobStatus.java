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

package org.apache.flink.api.common;

import org.apache.flink.annotation.PublicEvolving;

/** Possible states of a job once it has been accepted by the dispatcher. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 调度程序接受作业后作业的可能状态
*/
@PublicEvolving
public enum JobStatus {
    /**
     * The job has been received by the Dispatcher, and is waiting for the job manager to receive
     * leadership and to be created.
     */
    /**
     * 调度程序已收到作业，正在等待作业经理接收领导并创建该作业。
     */
    INITIALIZING(TerminalState.NON_TERMINAL),

    /** Job is newly created, no task has started to run. */
    /** 作业是新创建的，尚未开始运行任何任务 */
    CREATED(TerminalState.NON_TERMINAL),

    /** Some tasks are scheduled or running, some may be pending, some may be finished. */
    /** 有些任务已安排或正在运行，有些任务可能处于挂起状态，有些任务已完成。 */
    RUNNING(TerminalState.NON_TERMINAL),

    /** The job has failed and is currently waiting for the cleanup to complete. */
    /** 作业已失败，当前正在等待清理完成 */
    FAILING(TerminalState.NON_TERMINAL),

    /** The job has failed with a non-recoverable task failure. */
    /** 作业失败，出现不可恢复的任务失败。 */
    FAILED(TerminalState.GLOBALLY),

    /** Job is being cancelled. */
    /** 作业被取消 */
    CANCELLING(TerminalState.NON_TERMINAL),

    /** Job has been cancelled. */
    /** 作业已取消 */
    CANCELED(TerminalState.GLOBALLY),

    /** All of the job's tasks have successfully finished. */
    /** 作业的所有任务都已成功完成 */
    FINISHED(TerminalState.GLOBALLY),

    /** The job is currently undergoing a reset and total restart. */
    /** 作业当前正在重置并完全重新启动 */
    RESTARTING(TerminalState.NON_TERMINAL),

    /**
     * The job has been suspended which means that it has been stopped but not been removed from a
     * potential HA job store.
     */
    /** 作业已挂起，这意味着它已停止，但尚未从潜在的HA作业存储中删除。 */
    SUSPENDED(TerminalState.LOCALLY),

    /** The job is currently reconciling and waits for task execution report to recover state. */
    /** 作业当前正在进行协调，并等待任务执行报告恢复状态。 */
    RECONCILING(TerminalState.NON_TERMINAL);

    // --------------------------------------------------------------------------------------------

    private enum TerminalState {
        NON_TERMINAL,
        LOCALLY,//作业已挂起，这意味着它已停止，但尚未从潜在的HA作业存储中删除。
        GLOBALLY
    }

    private final TerminalState terminalState;

    JobStatus(TerminalState terminalState) {
        this.terminalState = terminalState;
    }

    /**
     * Checks whether this state is <i>globally terminal</i>. A globally terminal job is complete
     * and cannot fail any more and will not be restarted or recovered by another standby master
     * node.
     *
     * <p>When a globally terminal state has been reached, all recovery data for the job is dropped
     * from the high-availability services.
     *
     * @return True, if this job status is globally terminal, false otherwise.
     */
    public boolean isGloballyTerminalState() {
        return terminalState == TerminalState.GLOBALLY;
    }

    /**
     * Checks whether this state is <i>locally terminal</i>. Locally terminal refers to the state of
     * a job's execution graph within an executing JobManager. If the execution graph is locally
     * terminal, the JobManager will not continue executing or recovering the job.
     *
     * <p>The only state that is locally terminal, but not globally terminal is {@link #SUSPENDED},
     * which is typically entered when the executing JobManager loses its leader status.
     *
     * @return True, if this job status is terminal, false otherwise.
     */
    public boolean isTerminalState() {
        return terminalState != TerminalState.NON_TERMINAL;
    }
}
