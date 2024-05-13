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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;

/**
 * It is an internal equivalent of {@link org.apache.flink.core.io.InputStatus} that provides
 * additional non public statuses.
 *
 * <p>An {@code InputStatus} indicates the availability of data from an asynchronous input. When
 * asking an asynchronous input to produce data, it returns this status to indicate how to proceed.
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 一个InputStatus（输入状态）表示来自异步输入的数据可用性。当请求异步输入生成数据时，它会返回这个状态来指示如何继续。
 * 它是InputStatus的内部等价枚举，提供了额外的非公共状态。
 */
@Internal
public enum DataInputStatus {

    /**
     * Indicator that more data is available and the input can be called immediately again to
     * produce more data.
     */
    /** 指示有更多数据可用，并且可以立即再次调用输入以生成更多数据。 */
    MORE_AVAILABLE,

    /**
     * Indicator that no data is currently available, but more data will be available in the future
     * again.
     */
    /** 指示当前没有数据可用，但未来会有更多数据可用。 */
    NOTHING_AVAILABLE,

    /** Indicator that all persisted data of the data exchange has been successfully restored. */
    /** 指示数据交换的所有持久化数据已成功恢复。 */
    END_OF_RECOVERY,

    /** Indicator that the input was stopped because of stop-with-savepoint without drain. */
    /** 指示由于停止而停止输入的存储点没有耗尽。 */
    STOPPED,

    /** Indicator that the input has reached the end of data. */
    /** 指示输入已到达数据末尾的指示符。 */
    END_OF_DATA,

    /**
     * Indicator that the input has reached the end of data and control events. The input is about
     * to close.
     */
    /** 指示输入已达到数据的末尾。 */
    END_OF_INPUT
}
