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
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rex.RexProgram

import scala.collection.JavaConversions._

/** Stream physical RelNode for [[Calc]]. */
class StreamPhysicalCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    calcProgram: RexProgram,
    outputRowType: RelDataType)
  extends StreamPhysicalCalcBase(cluster, traitSet, inputRel, calcProgram, outputRowType) {

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new StreamPhysicalCalc(cluster, traitSet, child, program, outputRowType)
  }
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 将物理节点Calc转换为ExecNode为后面转换Transformation做准备
   */
  override def translateToExecNode(): ExecNode[_] = {
    // 从CalcProgram中获取投影列表，并将每个投影表达式中的本地引用扩展为完整的表达式
    val projection = calcProgram.getProjectList.map(calcProgram.expandLocalRef)
    // 如果CalcProgram中存在条件表达式，则将其中的本地引用扩展为完整的表达式；否则，条件表达式为null
    val condition = if (calcProgram.getCondition != null) {
      calcProgram.expandLocalRef(calcProgram.getCondition)
    } else {
      null
    }

    // 创建一个StreamExecCalc执行节点
    // 该节点用于在流处理中对输入数据进行计算，包括投影（选择列）和过滤（根据条件筛选）
    new StreamExecCalc(
      unwrapTableConfig(this),// 获取表的配置
      projection, // 投影列表，即要选择的列或表达式
      condition, // 过滤条件，如果不存在则为null
      InputProperty.DEFAULT,// 输入属性的默认设置
      FlinkTypeFactory.toLogicalRowType(getRowType),// 将表的行类型转换为逻辑行类型
      getRelDetailedDescription)// 获取关系的详细描述
  }
}
