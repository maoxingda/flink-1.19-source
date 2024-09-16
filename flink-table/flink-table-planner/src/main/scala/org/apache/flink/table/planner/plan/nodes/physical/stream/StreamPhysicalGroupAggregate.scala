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

import org.apache.flink.table.planner.JList
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.hint.StateTtlHint
import org.apache.flink.table.planner.plan.PartialFinalType
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupAggregate
import org.apache.flink.table.planner.plan.utils.{AggregateUtil, ChangelogPlanUtils, RelExplainUtil}
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.hint.RelHint

import java.util
import java.util.Collections

/**
 * Stream physical RelNode for unbounded group aggregate.
 *
 * This node does support un-splittable aggregate function (e.g. STDDEV_POP).
 *
 * @see
 *   [[StreamPhysicalGroupAggregateBase]] for more info.
 */
class StreamPhysicalGroupAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    grouping: Array[Int],
    aggCalls: Seq[AggregateCall],
    var partialFinalType: PartialFinalType = PartialFinalType.NONE,
    hints: JList[RelHint] = Collections.emptyList())
  extends StreamPhysicalGroupAggregateBase(cluster, traitSet, inputRel, grouping, aggCalls, hints) {

  private val aggInfoList =
    AggregateUtil.deriveAggregateInfoList(this, grouping.length, aggCalls)

  override def requireWatermark: Boolean = false

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamPhysicalGroupAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      outputRowType,
      grouping,
      aggCalls,
      partialFinalType,
      hints)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputRowType = getInput.getRowType
    super
      .explainTerms(pw)
      .itemIf("groupBy", RelExplainUtil.fieldToString(grouping, inputRowType), grouping.nonEmpty)
      .itemIf("partialFinalType", partialFinalType, partialFinalType != PartialFinalType.NONE)
      .item(
        "select",
        RelExplainUtil
          .streamGroupAggregationToString(inputRowType, getRowType, aggInfoList, grouping))
  }
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * StreamPhysicalGroupAggregate转换为StreamExecGroupAggregate执行节点 为后面转换为Transformation做准备
   */
  override def translateToExecNode(): ExecNode[_] = {
    // 判断聚合调用是否需要撤回操作。这通常与聚合函数是否支持增量更新有关，
    val aggCallNeedRetractions =
      AggregateUtil.deriveAggCallNeedRetractions(this, grouping.length, aggCalls)
    // 生成在更新操作之前的变更日志处理逻辑（如果有的话）。
    // 这通常用于在数据更新之前进行一些前置处理，比如记录变更前的状态。
    val generateUpdateBefore = ChangelogPlanUtils.generateUpdateBefore(this)
    // 判断是否需要处理撤回操作。这取决于输入流是否仅包含插入操作。
    // 如果输入流可能包含更新或删除操作，则需要处理撤回。
    val needRetraction = !ChangelogPlanUtils.inputInsertOnly(this)
    // 创建一个StreamExecGroupAggregate执行节点，用于执行分组聚合操作。
    new StreamExecGroupAggregate(
      unwrapTableConfig(this), // 获取表的配置
      grouping,// 分组键
      aggCalls.toArray,// 聚合函数调用的数组
      aggCallNeedRetractions,// 聚合调用是否需要撤回操作的标记数组
      generateUpdateBefore, // 是否在更新前生成变更日志处理逻辑
      needRetraction,// 是否需要处理撤回操作
      StateTtlHint.getStateTtlFromHintOnSingleRel(hints),
      InputProperty.DEFAULT,// 输入属性的默认设置
      FlinkTypeFactory.toLogicalRowType(getRowType),// 将表的行类型转换为逻辑行类型
      getRelDetailedDescription)// 获取关系的详细描述
  }
}
