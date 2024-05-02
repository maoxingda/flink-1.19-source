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

package org.apache.flink.runtime.scheduler.metrics;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.MetricGroup;

/** Utility to define metrics that capture the time that some component spends in a state. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 用于定义度量标准的实用程序，这些度量标准捕获某个组件在一个状态中花费的时间
*/
public interface StateTimeMetric {

    /**
     * Returns the time, in milliseconds, that have elapsed since we transitioned to the targeted
     * state. Returns 0 if we are not in the targeted state.
     */
    long getCurrentTime();

    /** Returns the total time, in milliseconds, that we have spent in the targeted state. */
    long getTotalTime();

    /** Returns 1 if we are in the targeted state, otherwise 0. */
    long getBinary();
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 向MetricGroup注册时间类的监控指标
    */
    static void register(
            MetricOptions.JobStatusMetricsSettings jobStatusMetricsSettings,
            MetricGroup metricGroup,
            StateTimeMetric stateTimeMetric,
            String baseName) {
        /**
         * 检查jobStatusMetricsSettings是否启用了状态度量（isStateMetricsEnabled()）。
         * 如果启用了，那么它会使用getStateMetricName(baseName)方法生成一个具体的度量名称，
         * 并进行注册
         */
        if (jobStatusMetricsSettings.isStateMetricsEnabled()) {
            metricGroup.gauge(getStateMetricName(baseName), stateTimeMetric::getBinary);
        }
        /**
         * 检查是否启用了当前时间度量（isCurrentTimeMetricsEnabled()））。如果启用了，
         * 生成getCurrentTimeMetricName(baseName)度量名称，并使用stateTimeMetric对象提供的相应方法来注册Gauge度量。
         */
        if (jobStatusMetricsSettings.isCurrentTimeMetricsEnabled()) {
            metricGroup.gauge(getCurrentTimeMetricName(baseName), stateTimeMetric::getCurrentTime);
        }
        /**
         * 检查是否启用了总时间度量（isTotalTimeMetricsEnabled()
         * 如果启用。生成getTotalTimeMetricName(baseName)来生成度量名称
         * 并使用stateTimeMetric对象提供的相应方法来注册Gauge度量
         */
        if (jobStatusMetricsSettings.isTotalTimeMetricsEnabled()) {
            metricGroup.gauge(getTotalTimeMetricName(baseName), stateTimeMetric::getTotalTime);
        }
    }

    @VisibleForTesting
    static String getStateMetricName(String baseName) {
        return baseName + "State";
    }

    @VisibleForTesting
    static String getCurrentTimeMetricName(String baseName) {
        return baseName + "Time";
    }

    @VisibleForTesting
    static String getTotalTimeMetricName(String baseName) {
        return baseName + "TimeTotal";
    }
}
