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
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalTableSourceScan
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.metadata.RelMetadataQuery

import java.util

/**
 * Stream physical RelNode to read data from an external source defined by a
 * [[org.apache.flink.table.connector.source.ScanTableSource]].
 */
/**
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * 一个物理执行节点（physical RelNode），该节点被设计用于从外部数据源读取数据.
 * 外部数据源是由org.apache.flink.table.connector.source.ScanTableSource接口定义的。
 */
class StreamPhysicalTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    hints: util.List[RelHint],
    tableSourceTable: TableSourceTable)
  extends CommonPhysicalTableSourceScan(cluster, traitSet, hints, tableSourceTable)
  with StreamPhysicalRel {

  override def requireWatermark: Boolean = false

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamPhysicalTableSourceScan(cluster, traitSet, getHints, tableSourceTable)
  }

  override def copy(relOptTable: TableSourceTable): RelNode = {
    new StreamPhysicalTableSourceScan(cluster, traitSet, getHints, relOptTable)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this)
    val rowSize = mq.getAverageRowSize(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * rowSize)
  }

  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 将TableSource物理节点转为ExecNode为后面的Transformation做准备
   */
  override def translateToExecNode(): ExecNode[_] = {
    // 创建一个DynamicTableSourceSpec对象，传入解析后的表和表的能力规范
    val tableSourceSpec = new DynamicTableSourceSpec(
      tableSourceTable.contextResolvedTable,
      util.Arrays.asList(tableSourceTable.abilitySpecs: _*))
    // 设置表的源
    tableSourceSpec.setTableSource(tableSource)
    // 创建一个StreamExecTableSourceScan执行节点，传入配置、表源规范、行类型和详细描述
    new StreamExecTableSourceScan(
      unwrapTableConfig(this),// 获取表的配置
      tableSourceSpec,// 表源规范
      FlinkTypeFactory.toLogicalRowType(getRowType),// 将表的行类型转换为逻辑行类型
      getRelDetailedDescription)// 获取关系的详细描述
  }
}
