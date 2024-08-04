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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.planner.catalog.QueryOperationCatalogViewTable

import com.google.common.collect.Sets
import org.apache.calcite.plan.ViewExpanders
import org.apache.calcite.rel.{RelHomogeneousShuttle, RelNode, RelShuttleImpl}
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.logical._
import org.apache.calcite.rex.{RexNode, RexShuttle, RexSubQuery}

import scala.collection.JavaConversions._

class DefaultRelShuttle extends RelHomogeneousShuttle {

  override def visit(rel: RelNode): RelNode = {
    var change = false
    val newInputs = rel.getInputs.map {
      input =>
        val newInput = input.accept(this)
        change = change || (input ne newInput)
        newInput
    }
    if (change) {
      rel.copy(rel.getTraitSet, newInputs)
    } else {
      rel
    }
  }
}

/**
 * Convert all [[QueryOperationCatalogViewTable]]s (including tables in [[RexSubQuery]]) to a
 * relational expression.
 */
class ExpandTableScanShuttle extends RelShuttleImpl {

  /**
   * Override this method to use `replaceInput` method instead of `copy` method if any children
   * change. This will not change any output of LogicalTableScan when LogicalTableScan is replaced
   * with RelNode tree in its RelTable.
   */
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 重写此方法以在子节点发生变化时使用 `replaceInput` 方法而不是 `copy` 方法。
   * 当在 RelTable 中的 LogicalTableScan 被替换为 RelNode 树时，这不会改变 LogicalTableScan 的任何输出。
   *
   * @param parent 当前正在访问的父节点。
   * @param i 当前正在访问的子节点在父节点输入列表中的索引。
   * @param child 当前正在访问的父节点的子节点。
   */
  override def visitChild(parent: RelNode, i: Int, child: RelNode): RelNode = {
    // 将父节点推入栈中，可能用于调试或跟踪递归深度。
    stack.push(parent)
    try {
      // 递归地访问子节点，子节点可能根据实现进行转换或保持不变。
      val child2 = child.accept(this)
      // 如果子节点在访问过程中发生了变化（即 child2 不等于 child），
      // 则使用 replaceInput 方法将父节点的第 i 个输入替换为新的子节点 child2。
      if (child2 ne child) {
        parent.replaceInput(i, child2)
      }
      // 返回父节点，无论其子节点是否发生变化。
      parent
    } finally {
      // 无论是否发生异常，都将父节点从栈中弹出。
      stack.pop
    }
  }

  override def visit(filter: LogicalFilter): RelNode = {
    val newCondition = filter.getCondition.accept(new ExpandTableScanInSubQueryShuttle)
    if (newCondition ne filter.getCondition) {
      val newFilter = filter.copy(filter.getTraitSet, filter.getInput, newCondition)
      super.visit(newFilter)
    } else {
      super.visit(filter)
    }
  }

  override def visit(project: LogicalProject): RelNode = {
    val shuttle = new ExpandTableScanInSubQueryShuttle
    var changed = false
    val newProjects = project.getProjects.map {
      project =>
        val newProject = project.accept(shuttle)
        if (newProject ne project) {
          changed = true
        }
        newProject
    }
    if (changed) {
      val newProject =
        project.copy(project.getTraitSet, project.getInput, newProjects, project.getRowType)
      super.visit(newProject)
    } else {
      super.visit(project)
    }
  }

  override def visit(join: LogicalJoin): RelNode = {
    val newCondition = join.getCondition.accept(new ExpandTableScanInSubQueryShuttle)
    if (newCondition ne join.getCondition) {
      val newJoin = join.copy(
        join.getTraitSet,
        newCondition,
        join.getLeft,
        join.getRight,
        join.getJoinType,
        join.isSemiJoinDone)
      super.visit(newJoin)
    } else {
      super.visit(join)
    }
  }

  class ExpandTableScanInSubQueryShuttle extends RexShuttle {
    override def visitSubQuery(subQuery: RexSubQuery): RexNode = {
      val newRel = subQuery.rel.accept(ExpandTableScanShuttle.this)
      var changed = false
      val newOperands = subQuery.getOperands.map {
        op =>
          val newOp = op.accept(ExpandTableScanInSubQueryShuttle.this)
          if (op ne newOp) {
            changed = true
          }
          newOp
      }

      var newSubQuery = subQuery
      if (newRel ne newSubQuery.rel) {
        newSubQuery = newSubQuery.clone(newRel)
      }
      if (changed) {
        newSubQuery = newSubQuery.clone(newSubQuery.getType, newOperands)
      }
      newSubQuery
    }
  }

  /**
   * Converts [[LogicalTableScan]] the result [[RelNode]] tree by calling
   * [[QueryOperationCatalogViewTable]]#toRel
   */
  override def visit(scan: TableScan): RelNode = {
    scan match {
      case tableScan: LogicalTableScan =>
        val viewTable = tableScan.getTable.unwrap(classOf[QueryOperationCatalogViewTable])
        if (viewTable != null) {
          val rel = viewTable.toRel(ViewExpanders.simpleContext(tableScan.getCluster))
          rel.accept(this)
        } else {
          tableScan
        }
      case otherScan => otherScan
    }
  }
}

/**
 * Rewrite same rel object to different rel objects.
 *
 * <p>e.g.
 * {{{
 *      Join                       Join
 *     /    \                     /    \
 * Filter1 Filter2     =>     Filter1 Filter2
 *     \   /                     |      |
 *      Scan                  Scan1    Scan2
 * }}}
 * After rewrote, Scan1 and Scan2 are different object but have same digest.
 */
class SameRelObjectShuttle extends DefaultRelShuttle {
  private val visitedNodes = Sets.newIdentityHashSet[RelNode]()

  override def visit(node: RelNode): RelNode = {
    val visited = !visitedNodes.add(node)
    var change = false
    val newInputs = node.getInputs.map {
      input =>
        val newInput = input.accept(this)
        change = change || (input ne newInput)
        newInput
    }
    if (change || visited) {
      node.copy(node.getTraitSet, newInputs)
    } else {
      node
    }
  }
}
