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
package org.apache.flink.table.planner.plan.optimize.program

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.metadata.FlinkRelMdNonCumulativeCost
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil
import org.apache.flink.util.Preconditions

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan.RelOptPlanner.CannotPlanException
import org.apache.calcite.plan.RelTrait
import org.apache.calcite.plan.volcano.VolcanoPlanner
import org.apache.calcite.rel.RelNode
import org.apache.calcite.tools.{Programs, RuleSet}

/**
 * A FlinkRuleSetProgram that runs with [[org.apache.calcite.plan.volcano.VolcanoPlanner]].
 *
 * @tparam OC
 *   OptimizeContext
 */
/**
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * 一个FlinkRuleSetProgram，与[[org.apache.clexical.plan.volacano.VolcanoPlanner]]一起运行。
 */
class FlinkVolcanoProgram[OC <: FlinkOptimizeContext] extends FlinkRuleSetProgram[OC] {

  /** Required output traits, this must not be None when doing optimize. */
  protected var requiredOutputTraits: Option[Array[RelTrait]] = None

  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 定义一个优化方法，用于优化给定的逻辑计划节点（RelNode）
   */
  override def optimize(root: RelNode, context: OC): RelNode = {
    // 如果没有优化规则，则直接返回原始的根节点
    if (rules.isEmpty) {
      return root
    }
    // 如果所需的输出特性为空，则抛出异常，因为在Flink的Volcano优化器中，输出特性不应为空
    if (requiredOutputTraits.isEmpty) {
      throw new TableException("Required output traits should not be None in FlinkVolcanoProgram")
    }
    /**
     * RelTraitSet是一组RelTrait的集合，用于描述一个RelNode（即关系表达式节点）的物理属性。
     * 这些物理属性包括但不限于数据的排序方式（Collation）、数据的分布方式（Distribution）
     * 以及数据访问的规约（Convention）等。
     * 常见的RelTrait包括排序（Collation）、分布（Distribution）和规约（Convention）等。
     */
    val targetTraits = root.getTraitSet.plusAll(requiredOutputTraits.get).simplify()
    // VolcanoPlanner limits that the planer a RelNode tree belongs to and
    // the VolcanoPlanner used to optimize the RelNode tree should be same instance.
    // see: VolcanoPlanner#registerImpl
    // here, use the planner in cluster directly
    // 由于VolcanoPlanner的限制，同一个RelNode树必须属于同一个VolcanoPlanner实例
    // 这里直接使用集群中的planner实例
    val planner = root.getCluster.getPlanner.asInstanceOf[VolcanoPlanner]
    // 创建包含所有优化规则的程序
    val optProgram = Programs.ofRules(rules)

    try {
      // 设置当前线程的优化器为planner，以便在优化过程中使用
      FlinkRelMdNonCumulativeCost.THREAD_PLANNER.set(planner)
      // 执行优化程序，传入planner、根节点、目标特性集等参数
      optProgram.run(planner, root, targetTraits, ImmutableList.of(), ImmutableList.of())
    } catch {
      case e: CannotPlanException =>
        throw new TableException(
          s"Cannot generate a valid execution plan for the given query: \n\n" +
            s"${FlinkRelOptUtil.toString(root)}\n" +
            s"This exception indicates that the query uses an unsupported SQL feature.\n" +
            s"Please check the documentation for the set of currently supported SQL features.",
          e)
      case t: TableException =>
        throw new TableException(
          s"Cannot generate a valid execution plan for the given query: \n\n" +
            s"${FlinkRelOptUtil.toString(root)}\n" +
            s"${t.getMessage}\n" +
            s"Please check the documentation for the set of currently supported SQL features.",
          t)
      case a: AssertionError =>
        throw new AssertionError(s"Sql optimization: Assertion error: ${a.getMessage}", a)
      case r: RuntimeException if r.getCause.isInstanceOf[TableException] =>
        throw new TableException(
          s"Sql optimization: Cannot generate a valid execution plan for the given query: \n\n" +
            s"${FlinkRelOptUtil.toString(root)}\n" +
            s"${r.getCause.getMessage}\n" +
            s"Please check the documentation for the set of currently supported SQL features.",
          r.getCause)
    } finally {
      FlinkRelMdNonCumulativeCost.THREAD_PLANNER.remove()
    }
  }

  /** Sets required output traits. */
  def setRequiredOutputTraits(relTraits: Array[RelTrait]): Unit = {
    Preconditions.checkNotNull(relTraits)
    requiredOutputTraits = Some(relTraits)
  }

}

class FlinkVolcanoProgramBuilder[OC <: FlinkOptimizeContext] {
  private val volcanoProgram = new FlinkVolcanoProgram[OC]

  /** Adds rules for this program. */
  def add(ruleSet: RuleSet): FlinkVolcanoProgramBuilder[OC] = {
    volcanoProgram.add(ruleSet)
    this
  }

  /** Sets required output traits. */
  def setRequiredOutputTraits(relTraits: Array[RelTrait]): FlinkVolcanoProgramBuilder[OC] = {
    volcanoProgram.setRequiredOutputTraits(relTraits)
    this
  }

  def build(): FlinkVolcanoProgram[OC] = volcanoProgram

}

object FlinkVolcanoProgramBuilder {
  def newBuilder[OC <: FlinkOptimizeContext] = new FlinkVolcanoProgramBuilder[OC]
}
