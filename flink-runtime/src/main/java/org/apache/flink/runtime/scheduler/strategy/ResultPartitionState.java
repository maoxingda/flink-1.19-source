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

package org.apache.flink.runtime.scheduler.strategy;

/** State of a {@link SchedulingResultPartition}. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * SchedulelingResultPartition 的状态。
*/
public enum ResultPartitionState {

    /** Partition is just created or is just reset. */
    /** 分区刚刚创建或刚刚重置 */
    CREATED,

    /** Partition has produced all data. */
    /** 分区已生成所有数据。 */
    ALL_DATA_PRODUCED
}
