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

import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil
import org.apache.flink.table.planner.utils.Logging
import org.apache.flink.util.Preconditions

import org.apache.calcite.rel.RelNode

import java.util

import scala.collection.JavaConversions._

/**
 * A FlinkOptimizeProgram that contains a sequence of sub-[[FlinkOptimizeProgram]]s as a group.
 * Programs in the group will be executed in sequence, and the group will be executed `iterations`
 * times.
 *
 * @tparam OC
 *   OptimizeContext
 */
/**
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * FlinkOptimizeProgram包含一系列 sub [[FlinkOptimizeProgram]]作为一个组。组中的程序将按顺序执行，组将执行“迭代”次。
 */
class FlinkGroupProgram[OC <: FlinkOptimizeContext] extends FlinkOptimizeProgram[OC] with Logging {

  /** Sub-programs in this program. */
  /** 本程序中的子程序 */
  private val programs = new util.ArrayList[(FlinkOptimizeProgram[OC], String)]()

  /** Repeat execution times for sub-programs as a group. */
  /** 将子程序作为一个组重复执行时间。 */
  private var iterations = 1

  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 该方法接受一个关系节点（RelNode）和一个上下文（OC）作为输入，并返回一个优化后的关系节点
   */
  override def optimize(root: RelNode, context: OC): RelNode = {
    // 如果没有优化程序，则直接返回原始的关系节点
    if (programs.isEmpty) {
      return root
    }
    // 使用foldLeft来迭代执行优化程序，迭代次数由iterations指定
    // 初始值为原始的关系节点root
    (0 until iterations).foldLeft(root) {
      // 对于每一次迭代
      case (input, i) =>
        // 如果日志级别设置为DEBUG，则打印当前迭代次数
      if (LOG.isDebugEnabled) {
          LOG.debug(s"iteration: ${i + 1}")
        }

        // 再次使用foldLeft来遍历所有的优化程序，对输入的关系节点进行优化
        // 当前迭代的输入关系节点作为初始值
        programs.foldLeft(input) {
          // 对于每一对优化程序及其描述
          case (currentInput, (program, description)) =>
            // 记录开始时间
          val start = System.currentTimeMillis()
            // 执行优化程序，传入当前的关系节点和上下文
            val result = program.optimize(currentInput, context)
            // 记录结束时间，并计算执行时间
            val end = System.currentTimeMillis()

            // 如果日志级别设置为DEBUG，则打印优化程序的描述、执行时间和优化结果
            if (LOG.isDebugEnabled) {
              LOG.debug(
                s"optimize $description cost ${end - start} ms.\n" +
                  s"optimize result:\n ${FlinkRelOptUtil.toString(result)}")
            }
            // 返回优化后的关系节点，作为下一次迭代的输入
            result
        }
    }
  }

  def addProgram(program: FlinkOptimizeProgram[OC], description: String = ""): Unit = {
    Preconditions.checkNotNull(program)
    val desc = if (description != null) description else ""
    programs.add((program, desc))
  }

  def setIterations(iterations: Int): Unit = {
    Preconditions.checkArgument(iterations > 0)
    this.iterations = iterations
  }
}

/**
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * 添加FlinkOptimizeProgram
 */
class FlinkGroupProgramBuilder[OC <: FlinkOptimizeContext] {
  private val groupProgram = new FlinkGroupProgram[OC]
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 添加FlinkOptimizeProgram方法
   */
  def addProgram(
      program: FlinkOptimizeProgram[OC],
      description: String = ""): FlinkGroupProgramBuilder[OC] = {
    groupProgram.addProgram(program, description)
    this
  }
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 设置groupProgram的iterations
   */
  def setIterations(iterations: Int): FlinkGroupProgramBuilder[OC] = {
    groupProgram.setIterations(iterations)
    this
  }

  def build(): FlinkGroupProgram[OC] = groupProgram

}

object FlinkGroupProgramBuilder {
  def newBuilder[OC <: FlinkOptimizeContext] = new FlinkGroupProgramBuilder[OC]
}
