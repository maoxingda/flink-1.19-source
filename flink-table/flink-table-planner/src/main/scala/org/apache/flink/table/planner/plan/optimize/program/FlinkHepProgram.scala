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
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.RelTrait
import org.apache.calcite.plan.hep.{HepPlanner, HepProgram}
import org.apache.calcite.rel.RelNode

/**
 * A FlinkOptimizeProgram that runs with [[HepPlanner]].
 *
 * <p>In most case, [[FlinkHepRuleSetProgram]] could meet our requirements. Otherwise we could
 * choose this program for some advanced features, and use
 * [[org.apache.calcite.plan.hep.HepProgramBuilder]] to create [[HepProgram]].
 *
 * @tparam OC
 *   OptimizeContext
 */
class FlinkHepProgram[OC <: FlinkOptimizeContext] extends FlinkOptimizeProgram[OC] {

  /** [[HepProgram]] instance for [[HepPlanner]], this must not be None when doing optimize. */
  private var hepProgram: Option[HepProgram] = None

  /** Requested root traits, it's an optional item. */
  private var requestedRootTraits: Option[Array[RelTrait]] = None
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 优化RelNode
   */
  override def optimize(root: RelNode, context: OC): RelNode = {
    // 如果hepProgram为空，则抛出异常，因为hepProgram在FlinkHepProgram中不应为空
    if (hepProgram.isEmpty) {
      throw new TableException("hepProgram should not be None in FlinkHepProgram")
    }

    try {
      // 使用hepProgram和上下文信息创建一个新的HepPlanner实例
      val planner = new HepPlanner(hepProgram.get, context)
      // 设置当前线程的FlinkRelMdNonCumulativeCost的planner，以便在元数据计算时使用
      FlinkRelMdNonCumulativeCost.THREAD_PLANNER.set(planner)
      // 设置优化器的根节点
      planner.setRoot(root)
      // 如果存在请求的根节点特性（traits），则尝试修改根节点的特性
      if (requestedRootTraits.isDefined) {
        // 计算目标特性集，即将请求的特性添加到根节点的当前特性集中
        val targetTraitSet = root.getTraitSet.plusAll(requestedRootTraits.get)
        // 如果根节点的当前特性集与目标特性集不同，则修改根节点的特性集
        if (!root.getTraitSet.equals(targetTraitSet)) {
          planner.changeTraits(root, targetTraitSet.simplify)
        }
      }
      // 执行优化，找到最佳的表达式树,并返回
      planner.findBestExp
    } finally {
      // 无论优化是否成功，都清除当前线程的FlinkRelMdNonCumulativeCost的planner设置
      FlinkRelMdNonCumulativeCost.THREAD_PLANNER.remove()
    }
  }

  /** Sets hep program instance. */
  def setHepProgram(hepProgram: HepProgram): Unit = {
    Preconditions.checkNotNull(hepProgram)
    this.hepProgram = Some(hepProgram)
  }

  /** Sets requested root traits. */
  def setRequestedRootTraits(relTraits: Array[RelTrait]): Unit = {
    requestedRootTraits = Option.apply(relTraits)
  }

}

object FlinkHepProgram {

  def apply[OC <: FlinkOptimizeContext](
      hepProgram: HepProgram,
      requestedRootTraits: Option[Array[RelTrait]] = None): FlinkHepProgram[OC] = {

    val flinkHepProgram = new FlinkHepProgram[OC]()
    flinkHepProgram.setHepProgram(hepProgram)
    if (requestedRootTraits.isDefined) {
      flinkHepProgram.setRequestedRootTraits(requestedRootTraits.get)
    }
    flinkHepProgram
  }
}
