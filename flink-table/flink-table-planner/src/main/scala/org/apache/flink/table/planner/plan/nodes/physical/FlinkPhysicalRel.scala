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
package org.apache.flink.table.planner.plan.nodes.physical

import org.apache.flink.table.planner.plan.nodes.FlinkRelNode
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode

import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode

/** Base class for flink physical relational expression. */
/**
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * Flink物理关系表达式的基类。
 */
trait FlinkPhysicalRel extends FlinkRelNode {

  /**
   * Try to satisfy required traits by descendant of current node. If descendant can satisfy
   * required traits, and current node will not destroy it, then returns the new node with converted
   * inputs.
   *
   * @param requiredTraitSet
   *   required traits
   * @return
   *   A converted node which satisfy required traits by inputs node of current node. Returns None
   *   if required traits cannot be satisfied.
   */
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 通过当前节点的后代节点来满足所需的特性集。如果后代节点能够满足所需特性，
   * 并且当前节点不会破坏这些特性，则返回具有已转换输入的新节点。
   */
  def satisfyTraits(requiredTraitSet: RelTraitSet): Option[RelNode] = None

  /**
   * Translate this physical RelNode into an [[ExecNode]].
   *
   * NOTE: This method only needs to create the corresponding ExecNode, the connection to its
   * input/output nodes will be done by ExecGraphGenerator. Because some physical rels need not be
   * translated to a real ExecNode, such as Exchange will be translated to edge in the future.
   *
   * @param isCompiled
   *   Whether the translation happens as part of a plan compilation.
   */
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 将此物理RelNode转换为[[ExecNode]]。
   * 注意：此方法仅需要创建对应的ExecNode，其与输入/输出节点的连接将由ExecGraphGenerator完成。
   * 因为一些物理关系表达式不需要转换为实际的ExecNode，例如未来的Exchange可能会被转换为边。
   */
  def translateToExecNode(isCompiled: Boolean): ExecNode[_] = {
    val execNode = translateToExecNode()
    execNode.setCompiled(isCompiled)
    execNode
  }

  /**
   * Translate this physical RelNode into an [[ExecNode]].
   *
   * NOTE: This method only needs to create the corresponding ExecNode, the connection to its
   * input/output nodes will be done by ExecGraphGenerator. Because some physical rels need not be
   * translated to a real ExecNode, such as Exchange will be translated to edge in the future.
   */
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 将此物理RelNode转换为[[ExecNode]]。
   * 注意：此方法仅需要创建对应的ExecNode，其与输入/输出节点的连接将由ExecGraphGenerator完成。
   * 因为一些物理关系表达式不需要转换为实际的ExecNode，例如未来的Exchange可能会被转换为边。
   */
  def translateToExecNode(): ExecNode[_]
}
