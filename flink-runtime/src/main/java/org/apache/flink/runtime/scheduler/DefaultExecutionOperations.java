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

import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.executiongraph.Execution;

import java.util.concurrent.CompletableFuture;

/** Default implementation of {@link ExecutionOperations}. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 对Execution等各种操作、部署、取消、标记失败
*/
public class DefaultExecutionOperations implements ExecutionOperations {

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * Execution部署
    */
    @Override
    public void deploy(Execution execution) throws JobException {
        execution.deploy();
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * Execution取消
     */
    @Override
    public CompletableFuture<?> cancel(Execution execution) {
        execution.cancel();
        return execution.getReleaseFuture();
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将执行标记为失败。
    */
    @Override
    public void markFailed(Execution execution, Throwable cause) {
        execution.markFailed(cause);
    }
}
