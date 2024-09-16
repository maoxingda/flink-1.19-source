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

package org.apache.flink.table.runtime.generated;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.runtime.dataview.StateDataViewStore;

/**
 * The base class for handling aggregate or table aggregate functions.
 *
 * <p>It is code generated to handle all {@link AggregateFunction}s and {@link
 * TableAggregateFunction}s together in an aggregation.
 *
 * <p>It is the entry point for aggregate operators to operate all {@link AggregateFunction}s and
 * {@link TableAggregateFunction}s.
 */
/**
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * 处理聚合或表聚合函数的基类。
 */
public interface AggsHandleFunctionBase extends Function {

    /** Initialization method for the function. It is called before the actual working methods. */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 函数的初始化方法。它被称为实际工作方法之前。
     */
    void open(StateDataViewStore store) throws Exception;

    /**
     * Accumulates the input values to the accumulators.
     *
     * @param input input values bundled in a row
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 将输入值累加到累加器。
     */
    void accumulate(RowData input) throws Exception;

    /**
     * Retracts the input values from the accumulators.
     *
     * @param input input values bundled in a row
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 从累加器中提取输入值。
     */
    void retract(RowData input) throws Exception;

    /**
     * Merges the other accumulators into current accumulators.
     *
     * @param accumulators The other row of accumulators
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 将其他累加器合并为当前累加器。
     */
    void merge(RowData accumulators) throws Exception;

    /**
     * Set the current accumulators (saved in a row) which contains the current aggregated results.
     * In streaming: accumulators are store in the state, we need to restore aggregate buffers from
     * state. In batch: accumulators are store in the hashMap, we need to restore aggregate buffers
     * from hashMap.
     *
     * @param accumulators current accumulators
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 设置包含当前聚合结果的当前累加器（保存在一行中）。
     * 在流中：累加器存储在状态中，我们需要从状态中恢复聚合缓冲区。
     * 批量：累加器存储在hashMap中，我们需要从hashMap恢复聚合缓冲区。
     */
    void setAccumulators(RowData accumulators) throws Exception;

    /** Resets all the accumulators. */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 重置所有累加器。
     */
    void resetAccumulators() throws Exception;

    /**
     * Gets the current accumulators (saved in a row) which contains the current aggregated results.
     *
     * @return the current accumulators
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 并获得累加器
     */
    RowData getAccumulators() throws Exception;

    /**
     * Initializes the accumulators and save them to a accumulators row.
     *
     * @return a row of accumulators which contains the aggregated results
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 初始化累加器并将其保存到累加器行。
     */
    RowData createAccumulators() throws Exception;

    /** Cleanup for the retired accumulators state. */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 清理累加器状态
     */
    void cleanup() throws Exception;

    /**
     * Tear-down method for this function. It can be used for clean up work. By default, this method
     * does nothing.
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 关闭累加器
     */
    void close() throws Exception;
}
