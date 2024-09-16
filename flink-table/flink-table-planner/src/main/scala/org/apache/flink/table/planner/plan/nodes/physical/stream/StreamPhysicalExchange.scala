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
package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecExchange
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalExchange
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelDistribution, RelNode}

/** Stream physical RelNode for [[org.apache.calcite.rel.core.Exchange]]. */
class StreamPhysicalExchange(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relNode: RelNode,
    relDistribution: RelDistribution)
  extends CommonPhysicalExchange(cluster, traitSet, relNode, relDistribution)
  with StreamPhysicalRel {

  override def requireWatermark: Boolean = false

  override def copy(
      traitSet: RelTraitSet,
      newInput: RelNode,
      newDistribution: RelDistribution): StreamPhysicalExchange = {
    new StreamPhysicalExchange(cluster, traitSet, newInput, newDistribution)
  }

  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * StreamPhysicalExchange 转换为ExecNode为后面的Transformation做准备
   */
  override def translateToExecNode(): ExecNode[_] = {
    // 创建一个StreamExecExchange执行节点
    // 这个节点通常用于在Flink的执行计划中引入数据交换（shuffle），以便对数据进行重新分区或分发
    new StreamExecExchange(
      unwrapTableConfig(this),// 获取表的配置
      InputProperty.builder.requiredDistribution(getRequiredDistribution).build,
      FlinkTypeFactory.toLogicalRowType(getRowType),// 将表的行类型转换为逻辑行类型
      getRelDetailedDescription)// 获取关系的详细描述
  }
}
