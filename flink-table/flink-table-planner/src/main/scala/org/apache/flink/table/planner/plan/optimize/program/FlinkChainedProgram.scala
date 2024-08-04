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
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil
import org.apache.flink.table.planner.utils.Logging
import org.apache.flink.util.Preconditions

import org.apache.calcite.rel.RelNode

import java.util

import scala.collection.JavaConversions._

/**
 * A FlinkOptimizeProgram contains a sequence of [[FlinkOptimizeProgram]]s which are chained
 * together.
 *
 * The chained-order of programs can be adjusted by [[addFirst]], [[addLast]], [[addBefore]] and
 * [[remove]] methods.
 *
 * When [[optimize]] method called, each program's optimize method will be called in sequence.
 *
 * @tparam OC
 *   OptimizeContext
 */
/**
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * FlinkOptimizeProgram包含一系列链接在一起的[[FlinkOptimizeProgram]]。
 */
class FlinkChainedProgram[OC <: FlinkOptimizeContext]
  extends FlinkOptimizeProgram[OC]
  with Logging {

  // keep program as ordered
  private val programNames = new util.ArrayList[String]()
  // map program name to program instance
  private val programMap = new util.HashMap[String, FlinkOptimizeProgram[OC]]()

  /** Calling each program's optimize method in sequence. */
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 顺序调用每个程序的优化方法。
   *
   * @param root 需要被优化的根节点，类型为RelNode（关系逻辑节点）。
   * @param context 优化的上下文信息，类型为OC（可能是一个自定义的优化上下文类型）。
   * @return 返回优化后的根节点，类型为RelNode。
   */
  def optimize(root: RelNode, context: OC): RelNode = {
    // 使用foldLeft方法遍历programNames列表，从root节点开始，依次应用每个程序的优化方法。
    programNames.foldLeft(root) {
      // (input, name)是foldLeft的累加器和当前元素，这里input是上一次优化后的结果，name是当前要调用的程序名称。
      (input, name) =>
        // 根据名称获取对应的优化程序，如果不存在则抛出TableException异常。
      val program = get(name).getOrElse(throw new TableException(s"This should not happen."))
        // 记录开始时间
        val start = System.currentTimeMillis()
        // 调用当前程序的优化方法，传入当前输入节点和上下文信息。
        val result = program.optimize(input, context)
        // 记录结束时间
        val end = System.currentTimeMillis()

        // 如果日志级别为DEBUG，则打印优化耗时和结果。
        if (LOG.isDebugEnabled) {
          LOG.debug(
            s"optimize $name cost ${end - start} ms.\n" +
              s"optimize result: \n${FlinkRelOptUtil.toString(result)}")
        }
        // 返回优化后的结果，作为下一次迭代的输入。
        result
    }
  }

  /** Gets program associated with the given name. If not found, return [[None]]. */
  def get(name: String): Option[FlinkOptimizeProgram[OC]] = Option.apply(programMap.get(name))

  /**
   * Gets FlinkRuleSetProgram associated with the given name. If the program is not found or is not
   * a [[FlinkRuleSetProgram]], return [[None]]. This method is mainly used for updating rules in
   * FlinkRuleSetProgram for existed FlinkChainedPrograms instance.
   */
  def getFlinkRuleSetProgram(name: String): Option[FlinkRuleSetProgram[OC]] = {
    get(name).getOrElse(None) match {
      case p: FlinkRuleSetProgram[OC] => Some(p)
      case _ => None
    }
  }

  /**
   * Appends the specified program to the end of program collection.
   *
   * @return
   *   false if program collection contains the specified program; otherwise true.
   */
  def addLast(name: String, program: FlinkOptimizeProgram[OC]): Boolean = {
    Preconditions.checkNotNull(name)
    Preconditions.checkNotNull(program)

    if (programNames.contains(name)) {
      false
    } else {
      programNames.add(name)
      programMap.put(name, program)
      true
    }
  }

  /**
   * Inserts the specified program to the beginning of program collection.
   *
   * @return
   *   false if program collection contains the specified program; otherwise true.
   */
  def addFirst(name: String, program: FlinkOptimizeProgram[OC]): Boolean = {
    Preconditions.checkNotNull(name)
    Preconditions.checkNotNull(program)

    if (programNames.contains(name)) {
      false
    } else {
      programNames.add(0, name)
      programMap.put(name, program)
      true
    }
  }

  /**
   * Inserts the specified program before `nameOfBefore`.
   *
   * @return
   *   false if program collection contains the specified program or does not contain
   *   `nameOfBefore`; otherwise true.
   */
  def addBefore(nameOfBefore: String, name: String, program: FlinkOptimizeProgram[OC]): Boolean = {
    Preconditions.checkNotNull(nameOfBefore)
    Preconditions.checkNotNull(name)
    Preconditions.checkNotNull(program)

    if (programNames.contains(name) || !programNames.contains(nameOfBefore)) {
      false
    } else if (programNames.isEmpty) {
      addLast(name, program)
    } else {
      val index = programNames.indexOf(nameOfBefore)
      programNames.add(index, name)
      programMap.put(name, program)
      true
    }
  }

  /**
   * Removes program associated with the given name from program collection.
   *
   * @return
   *   The removed program associated with the given name. If not found, return [[None]].
   */
  def remove(name: String): Option[FlinkOptimizeProgram[OC]] = {
    programNames.remove(name)
    Option.apply(programMap.remove(name))
  }

  /** Returns program names with chained order. */
  def getProgramNames: util.List[String] = new util.ArrayList[String](programNames)

}
