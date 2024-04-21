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

package org.apache.flink.runtime.registration;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Configuration for the cluster components. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 群集组件的配置
*/
public class RetryingRegistrationConfiguration {

    private final long initialRegistrationTimeoutMillis;

    private final long maxRegistrationTimeoutMillis;

    private final long errorDelayMillis;

    private final long refusedDelayMillis;

    public RetryingRegistrationConfiguration(
            long initialRegistrationTimeoutMillis,
            long maxRegistrationTimeoutMillis,
            long errorDelayMillis,
            long refusedDelayMillis) {
        checkArgument(
                initialRegistrationTimeoutMillis > 0,
                "initial registration timeout must be greater than zero");
        checkArgument(
                maxRegistrationTimeoutMillis > 0,
                "maximum registration timeout must be greater than zero");
        checkArgument(errorDelayMillis >= 0, "delay on error must be non-negative");
        checkArgument(
                refusedDelayMillis >= 0, "delay on refused registration must be non-negative");

        this.initialRegistrationTimeoutMillis = initialRegistrationTimeoutMillis;
        this.maxRegistrationTimeoutMillis = maxRegistrationTimeoutMillis;
        this.errorDelayMillis = errorDelayMillis;
        this.refusedDelayMillis = refusedDelayMillis;
    }

    public long getInitialRegistrationTimeoutMillis() {
        return initialRegistrationTimeoutMillis;
    }

    public long getMaxRegistrationTimeoutMillis() {
        return maxRegistrationTimeoutMillis;
    }

    public long getErrorDelayMillis() {
        return errorDelayMillis;
    }

    public long getRefusedDelayMillis() {
        return refusedDelayMillis;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 注册重试相关的配置
    */
    public static RetryingRegistrationConfiguration fromConfiguration(
            final Configuration configuration) {
        /**
         *  cluster.registration.initial-timeout 群集组件之间的初始注册超时（以毫秒为单位）。
         */
        long initialRegistrationTimeoutMillis =
                configuration.get(ClusterOptions.INITIAL_REGISTRATION_TIMEOUT);
        /**
         * cluster.registration.max-timeout 群集组件之间的最大注册超时（以毫秒为单位）。
         */
        long maxRegistrationTimeoutMillis =
                configuration.get(ClusterOptions.MAX_REGISTRATION_TIMEOUT);
        /**
         * cluster.registration.error-delay 在注册尝试导致异常（而不是超时）后进行的暂停（以毫秒为单位）。
         */
        long errorDelayMillis = configuration.get(ClusterOptions.ERROR_REGISTRATION_DELAY);
        /**
         * cluster.registration.refused-registration-delay 注册尝试被拒绝后的暂停时间（以毫秒为单位）。
         */
        long refusedDelayMillis = configuration.get(ClusterOptions.REFUSED_REGISTRATION_DELAY);

        return new RetryingRegistrationConfiguration(
                initialRegistrationTimeoutMillis,
                maxRegistrationTimeoutMillis,
                errorDelayMillis,
                refusedDelayMillis);
    }

    public static RetryingRegistrationConfiguration defaultConfiguration() {
        return fromConfiguration(new Configuration());
    }
}
