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

import org.apache.flink.table.catalog.ContextResolvedTable
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.calcite.Sink
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSink
import org.apache.flink.table.planner.plan.utils.{ChangelogPlanUtils, UpsertKeyUtil}
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rel.hint.RelHint

import java.util

/**
 * Stream physical RelNode to write data into an external sink defined by a [[DynamicTableSink]].
 */
class StreamPhysicalSink(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    hints: util.List[RelHint],
    contextResolvedTable: ContextResolvedTable,
    tableSink: DynamicTableSink,
    targetColumns: Array[Array[Int]],
    abilitySpecs: Array[SinkAbilitySpec],
    val upsertMaterialize: Boolean = false)
  extends Sink(cluster, traitSet, inputRel, hints, targetColumns, contextResolvedTable, tableSink)
  with StreamPhysicalRel {

  override def requireWatermark: Boolean = false

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamPhysicalSink(
      cluster,
      traitSet,
      inputs.get(0),
      hints,
      contextResolvedTable,
      tableSink,
      targetColumns,
      abilitySpecs,
      upsertMaterialize)
  }

  def copy(newUpsertMaterialize: Boolean): StreamPhysicalSink = {
    new StreamPhysicalSink(
      cluster,
      traitSet,
      inputRel,
      hints,
      contextResolvedTable,
      tableSink,
      targetColumns,
      abilitySpecs,
      newUpsertMaterialize)
  }
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * StreamPhysicalSink转换为StreamExecSink执行节点为后面的Transformation做准备
   */
  override def translateToExecNode(): ExecNode[_] = {
    // 获取输入流的变更日志模式。变更日志模式决定了数据变更（如插入、更新、删除）的处理方式。
    val inputChangelogMode =
      ChangelogPlanUtils.getChangelogMode(getInput.asInstanceOf[StreamPhysicalRel]).get
    // 创建一个DynamicTableSinkSpec对象，该对象封装了与动态表汇（Sink）相关的配置和信息。
    val tableSinkSpec =
      new DynamicTableSinkSpec(
        contextResolvedTable,
        util.Arrays.asList(abilitySpecs: _*),
        targetColumns)
    // 将具体的表汇（Sink）对象设置到DynamicTableSinkSpec中。
    tableSinkSpec.setTableSink(tableSink)
    // no need to call getUpsertKeysInKeyGroupRange here because there's no exchange before sink,
    // and only add exchange in exec sink node.
    // 获取输入关系的更新键。更新键是用于在数据更新操作中唯一标识数据行的键。
    val inputUpsertKeys = FlinkRelMetadataQuery
      .reuseOrCreate(cluster.getMetadataQuery)
      .getUpsertKeys(inputRel)
    // 创建一个StreamExecSink执行节点，用于将数据写入到表汇（Sink）中。
    new StreamExecSink(
      unwrapTableConfig(this), // 获取表的配置
      tableSinkSpec, // 动态表汇规格
      inputChangelogMode, // 输入流的变更日志模式
      InputProperty.DEFAULT, // 输入属性的默认设置
      FlinkTypeFactory.toLogicalRowType(getRowType), // 将表的行类型转换为逻辑行类型
      upsertMaterialize,// 是否将更新操作物化（即实际写入到存储中）
      UpsertKeyUtil.getSmallestKey(inputUpsertKeys),// 获取输入更新键中的最小键（可能用于优化或排序）
      getRelDetailedDescription) // 获取关系的详细描述
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super
      .explainTerms(pw)
      .itemIf("upsertMaterialize", "true", upsertMaterialize)
  }
}
