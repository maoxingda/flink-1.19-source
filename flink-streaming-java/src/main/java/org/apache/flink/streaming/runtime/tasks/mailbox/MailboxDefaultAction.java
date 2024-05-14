/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks.mailbox;

import org.apache.flink.annotation.Internal;

/** Interface for the default action that is repeatedly invoked in the mailbox-loop. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 在邮箱循环中重复调用的默认操作的接口。
*/
@Internal
public interface MailboxDefaultAction {

    /**
     * This method implements the default action of the mailbox loop (e.g. processing one event from
     * the input). Implementations should (in general) be non-blocking.
     *
     * @param controller controller object for collaborative interaction between the default action
     *     and the mailbox loop.
     * @throws Exception on any problems in the action.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 方法实现了邮箱循环的默认操作（例如，处理来自输入的一个事件）。实现通常应该是非阻塞的
    */
    void runDefaultAction(Controller controller) throws Exception;

    /** Represents the suspended state of a {@link MailboxDefaultAction}, ready to resume. */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 代表一个准备恢复的MailboxDefaultAction的挂起状态。
    */
    @Internal
    interface Suspension {

        /** Resume execution of the default action. */
        /** 恢复默认操作的执行 */
        void resume();
    }

    /**
     * This controller is a feedback interface for the default action to interact with the mailbox
     * execution. In particular, it offers ways to signal that the execution of the default action
     * should be finished or temporarily suspended.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 这个控制器是默认操作与邮箱执行进行交互的反馈接口。
     * 它提供了方法来指示默认操作的执行应该完成或暂时挂起。
    */
    @Internal
    interface Controller {

        /**
         * This method must be called to end the stream task when all actions for the tasks have
         * been performed. This method can be invoked from any thread.
         */
        /**
         * @授课老师(微信): yi_locus
         * email: 156184212@qq.com
         * 当所有任务的操作都已完成时，必须调用此方法以结束流任务。此方法可以从任何线程中调用。
        */
        void allActionsCompleted();

        /**
         * Calling this method signals that the mailbox-thread should (temporarily) stop invoking
         * the default action, e.g. because there is currently no input available. This method must
         * be invoked from the mailbox-thread only!
         *
         * @param suspensionPeriodTimer started (ticking) {@link PeriodTimer} that measures how long
         *     the default action was suspended/idling. If mailbox loop is busy processing mails,
         *     this timer should be paused for the time required to process the mails.
         */
        /**
         * @授课老师(微信): yi_locus
         * email: 156184212@qq.com
         * 调用此方法表示邮箱线程应该（暂时）停止调用默认操作，例如，因为目前没有可用的输入。此方法只能从邮箱线程中调用！
        */
        Suspension suspendDefaultAction(PeriodTimer suspensionPeriodTimer);

        /**
         * Same as {@link #suspendDefaultAction(PeriodTimer)} but without any associated timer
         * measuring the idle time.
         */
        /**
         * @授课老师(微信): yi_locus
         * email: 156184212@qq.com
         * 与suspendDefaultAction(PeriodTimer)相同，但没有任何与空闲时间测量相关的计时器。
        */
        Suspension suspendDefaultAction();
    }
}
