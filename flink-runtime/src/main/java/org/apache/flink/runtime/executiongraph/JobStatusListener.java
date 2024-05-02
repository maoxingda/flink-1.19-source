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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;

/** Interface for observers that monitor the status of a job. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 用于监视作业状态的接口。
*/
public interface JobStatusListener {

    /**
     * This method is called whenever the status of the job changes.
     *
     * @param jobId The ID of the job.
     * @param newJobStatus The status the job switched to.
     * @param timestamp The timestamp when the status transition occurred.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 只要作业的状态发生变化，就会调用此方法。
    */
    void jobStatusChanges(JobID jobId, JobStatus newJobStatus, long timestamp);
}
