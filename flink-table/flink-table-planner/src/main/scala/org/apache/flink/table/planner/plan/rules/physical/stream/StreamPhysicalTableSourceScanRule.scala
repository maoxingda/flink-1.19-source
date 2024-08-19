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
package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.connector.source.ScanTableSource
import org.apache.flink.table.planner.connectors.DynamicSourceUtils.{isSourceChangeEventsDuplicate, isUpsertSource}
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalChangelogNormalize, StreamPhysicalTableSourceScan}
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.table.planner.plan.utils.ScanUtil
import org.apache.flink.table.planner.utils.ShortcutUtils

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.rel.core.TableScan

/**
 * Rule that converts [[FlinkLogicalTableSourceScan]] to [[StreamPhysicalTableSourceScan]].
 *
 * <p>Depends whether this is a scan source, this rule will also generate
 * [[StreamPhysicalChangelogNormalize]] to materialize the upsert stream.
 */
class StreamPhysicalTableSourceScanRule(config: Config) extends ConverterRule(config) {

  /** Rule must only match if TableScan targets a [[ScanTableSource]] */
  override def matches(call: RelOptRuleCall): Boolean = {
    val scan: TableScan = call.rel(0).asInstanceOf[TableScan]
    val tableSourceTable = scan.getTable.unwrap(classOf[TableSourceTable])
    tableSourceTable match {
      case tst: TableSourceTable =>
        tst.tableSource match {
          case _: ScanTableSource => true
          case _ => false
        }
      case _ => false
    }
  }
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 用于转换关系节点（RelNode）
   */
  def convert(rel: RelNode): RelNode = {
    // 将关系节点转换为FlinkLogicalTableSourceScan类型
    val scan = rel.asInstanceOf[FlinkLogicalTableSourceScan]
    // 替换关系节点的特性集，将其物理约定改为流物理约定
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    // 获取表配置
    val tableConfig = ShortcutUtils.unwrapContext(rel.getCluster).getTableConfig
    // 将扫描的表转换为TableSourceTable类型
    val table = scan.getTable.asInstanceOf[TableSourceTable]
    // 创建一个新的流物理表源扫描节点
    val newScan = new StreamPhysicalTableSourceScan(rel.getCluster, traitSet, scan.getHints, table)
    // 获取解析后的表结构
    val resolvedSchema = table.contextResolvedTable.getResolvedSchema
    // 判断是否需要生成变更日志规范化节点
    if (
      !scan.eventTimeSnapshotRequired && (isUpsertSource(resolvedSchema, table.tableSource) ||
        isSourceChangeEventsDuplicate(resolvedSchema, table.tableSource, tableConfig))
    ) {
      // generate changelog normalize node
      // primary key has been validated in CatalogSourceTable
      // 主键已在CatalogSourceTable中验证
      val primaryKey = resolvedSchema.getPrimaryKey.get()
      val keyFields = primaryKey.getColumns
      val inputFieldNames = newScan.getRowType.getFieldNames
      // 获取主键字段在输入字段中的索引
      val primaryKeyIndices = ScanUtil.getPrimaryKeyIndices(inputFieldNames, keyFields)
      // 设置所需的分布特性，基于主键的哈希分布
      val requiredDistribution = FlinkRelDistribution.hash(primaryKeyIndices, requireStrict = true)
      // 创建一个新的特性集，包含所需的分布特性和流物理约定
      val requiredTraitSet = rel.getCluster.getPlanner
        .emptyTraitSet()
        .replace(requiredDistribution)
        .replace(FlinkConventions.STREAM_PHYSICAL)
      // 将新的扫描节点转换为具有所需特性集的节点
      val newInput: RelNode = RelOptRule.convert(newScan, requiredTraitSet)
      // 创建一个新的流物理变更日志规范化节点
      new StreamPhysicalChangelogNormalize(
        scan.getCluster,
        traitSet,
        newInput,
        primaryKeyIndices,
        table.contextResolvedTable
      )
    } else {
      // 如果不需要变更日志规范化，则直接返回新的扫描节点
      newScan
    }
  }
}

object StreamPhysicalTableSourceScanRule {
  val INSTANCE = new StreamPhysicalTableSourceScanRule(
    Config.INSTANCE.withConversion(
      classOf[FlinkLogicalTableSourceScan],
      FlinkConventions.LOGICAL,
      FlinkConventions.STREAM_PHYSICAL,
      "StreamPhysicalTableSourceScanRule"))
}
