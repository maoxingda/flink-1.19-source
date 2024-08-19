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
package org.apache.flink.table.planner.plan.cost

import org.apache.calcite.plan.RelOptCost

/** A [[RelOptCost]] that extends network cost and memory cost. */
/**
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * FlinkCostBase 特质，它扩展了 RelOptCost，增加了网络成本和内存成本的考量。
 */
trait FlinkCostBase extends RelOptCost {

  /** @return usage of network resources */
  /**
   * 获取网络资源的使用情况。
   * 这个方法返回了一个双精度浮点数，表示执行该操作所需的网络资源量。
   * @return 网络资源的使用量
   */
  def getNetwork: Double

  /** @return usage of memory resources */
  /**
   * 获取内存资源的使用情况。
   * 这个方法返回了一个双精度浮点数，表示执行该操作所需的内存资源量。
   * @return 内存资源的使用量
   */
  def getMemory: Double
}
