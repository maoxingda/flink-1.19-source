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

import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE.HEP_RULES_EXECUTION_TYPE
import org.apache.flink.util.Preconditions

import org.apache.calcite.plan.RelTrait
import org.apache.calcite.plan.hep.{HepMatchOrder, HepPlanner, HepProgramBuilder}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.tools.RuleSet

import scala.collection.JavaConversions._

/**
 * A FlinkRuleSetProgram that runs with [[HepPlanner]].
 *
 * <p>In most case this program could meet our requirements, otherwise we could choose
 * [[FlinkHepProgram]] for some advanced features.
 *
 * <p>Currently, default hep execution type is [[HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE]]. (Please
 * refer to [[HEP_RULES_EXECUTION_TYPE]] for more info about execution types)
 *
 * @tparam OC
 *   OptimizeContext
 */
/**
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * 与[[HepPlanner]]一起运行的FlinkRuleSetProgram。
 */
class FlinkHepRuleSetProgram[OC <: FlinkOptimizeContext] extends FlinkRuleSetProgram[OC] {

  /**
   * The order of graph traversal when looking for rule matches, default match order is ARBITRARY.
   */
  /** 查找规则匹配时的图遍历顺序，默认匹配顺序为ARBITRARY。 */
  private var matchOrder: HepMatchOrder = HepMatchOrder.ARBITRARY

  /** The limit of pattern matches for this program, default match limit is Integer.MAX_VALUE. */
  /** 此程序的模式匹配限制，默认匹配限制为整数。最大值 */
  private var matchLimit: Int = Integer.MAX_VALUE

  /** Hep rule execution type. This is a required item, default execution type is RULE_SEQUENCE. */
  /** Hep规则执行类型。这是必填项，默认执行类型为RULE_SEQUENCE */
  private var executionType: HEP_RULES_EXECUTION_TYPE = HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE

  /** Requested root traits, this is an optional item. */
  private var requestedRootTraits: Option[Array[RelTrait]] = None
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * FlinkHepProgram进行规则优化
   */
  override def optimize(input: RelNode, context: OC): RelNode = {
    // 如果规则集为空，则直接返回输入的RelNode，不进行任何优化
    if (rules.isEmpty) {
      return input
    }

    // build HepProgram
    // 构建HepProgram
    val builder = new HepProgramBuilder// 创建一个HepProgram的构建器
    builder.addMatchOrder(matchOrder) // 添加规则匹配的顺序
    builder.addMatchLimit(matchLimit)// 添加规则匹配的限制（如最大匹配次数）
    // 根据executionType决定如何添加规则
    executionType match {
      case HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE =>// 规则序列执行
        rules.foreach(builder.addRuleInstance) // 逐个添加规则实例
      case HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION =>// 规则集合执行
        builder.addRuleCollection(rules)// 将所有规则作为一个集合添加
      case _ =>
        throw new RuntimeException(s"Unsupported HEP_RULES_EXECUTION_TYPE: $executionType")
    }

    // optimize with HepProgram
    // 使用构建好的规则集和请求的根节点特性集创建Flink专用的HepProgram
    val flinkHepProgram = FlinkHepProgram[OC](builder.build(), requestedRootTraits)
    // 使用FlinkHepProgram对输入的RelNode进行优化，并返回优化后的RelNode
    flinkHepProgram.optimize(input, context)
  }

  /** Sets rules match order. */
  def setHepMatchOrder(matchOrder: HepMatchOrder): Unit = {
    this.matchOrder = Preconditions.checkNotNull(matchOrder)
  }

  /** Sets the limit of pattern matches. */
  def setMatchLimit(matchLimit: Int): Unit = {
    Preconditions.checkArgument(matchLimit > 0)
    this.matchLimit = matchLimit
  }

  /** Sets hep rule execution type. */
  def setHepRulesExecutionType(executionType: HEP_RULES_EXECUTION_TYPE): Unit = {
    this.executionType = Preconditions.checkNotNull(executionType)
  }

  /** Sets requested root traits. */
  def setRequestedRootTraits(relTraits: Array[RelTrait]): Unit = {
    requestedRootTraits = Option.apply(relTraits)
  }
}

/**
 * An enumeration of hep rule execution type, to tell the [[HepPlanner]] how exactly execute the
 * rules.
 */
/**
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * 一个枚举类型，用于指定 HepPlanner 如何精确执行 Hep 规则。
 */
object HEP_RULES_EXECUTION_TYPE extends Enumeration {
  type HEP_RULES_EXECUTION_TYPE = Value

  /**
   * Rules in RULE_SEQUENCE type are executed with RuleInstance. RuleInstance is an instruction that
   * matches a specific rule, each rule in the rule collection is associated with one RuleInstance.
   * Each RuleInstance will be executed only once according to the order defined by the rule
   * collection, but a rule may be applied more than once. If arbitrary order is needed, use
   * RULE_COLLECTION instead.
   *
   * Please refer to [[HepProgramBuilder#addRuleInstance]] for more info about RuleInstance.
   */
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 规则执行类型：规则序列。
   */
  val RULE_SEQUENCE: HEP_RULES_EXECUTION_TYPE.Value = Value

  /**
   * Rules in RULE_COLLECTION type are executed with RuleCollection. RuleCollection is an
   * instruction that matches any rules in a given collection. The order in which the rules within a
   * collection will be attempted is arbitrary, so if more control is needed, use RULE_SEQUENCE
   * instead.
   *
   * Please refer to [[HepProgramBuilder#addRuleCollection]] for more info about RuleCollection.
   */
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 规则执行类型：规则集合。
   */
  val RULE_COLLECTION: HEP_RULES_EXECUTION_TYPE.Value = Value
}
/**
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * 定义一个 FlinkHepRuleSetProgramBuilder 类，用于构建 FlinkHepRuleSetProgram 实例
 * 该类是一个构建器模式的应用，允许链式调用方法来配置和优化规则集程序
 *
 */
class FlinkHepRuleSetProgramBuilder[OC <: FlinkOptimizeContext] {
  private val hepRuleSetProgram = new FlinkHepRuleSetProgram[OC]

  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 设置 Hep 规则的执行类型
   * HEP_RULES_EXECUTION_TYPE 是一个枚举或类型别名，表示规则的执行方式（如顺序执行、并行执行等）
   */
  def setHepRulesExecutionType(
      executionType: HEP_RULES_EXECUTION_TYPE): FlinkHepRuleSetProgramBuilder[OC] = {
    // 将执行类型设置到 FlinkHepRuleSetProgram 实例中
    hepRuleSetProgram.setHepRulesExecutionType(executionType)
    // 返回构建器自身，支持链式调用
    this
  }

  /** Sets rules match order. */
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 设置规则匹配的顺序。
   * HepMatchOrder 是一个枚举或类型，定义了匹配规则时的顺序策略。
   */
  def setHepMatchOrder(matchOrder: HepMatchOrder): FlinkHepRuleSetProgramBuilder[OC] = {
    // 设置匹配顺序到 FlinkHepRuleSetProgram 实例中
    hepRuleSetProgram.setHepMatchOrder(matchOrder)
    // 返回构建器自身，支持链式调用
    this
  }

  /** Sets the limit of pattern matches. */
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 设置模式匹配的限制数量。
   * 这有助于控制优化过程中匹配规则的次数，以避免过多的计算开销。
   */
  def setMatchLimit(matchLimit: Int): FlinkHepRuleSetProgramBuilder[OC] = {
    // 设置匹配限制到 FlinkHepRuleSetProgram 实例中
    hepRuleSetProgram.setMatchLimit(matchLimit)
    // 返回构建器自身，支持链式调用
    this
  }

  /** Adds rules for this program. */
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 向程序添加规则集。
   * 规则集包含了一系列要应用于查询计划的优化规则。
   */
  def add(ruleSet: RuleSet): FlinkHepRuleSetProgramBuilder[OC] = {
    // 将规则集添加到 FlinkHepRuleSetProgram 实例中
    hepRuleSetProgram.add(ruleSet)
    // 返回构建器自身，支持链式调用
    this
  }

  /** Sets requested root traits. */
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 设置请求的根特征。
   * RelTrait 定义了关系表达式（如查询计划中的节点）的特性。
   * 通过设置根特征，可以控制哪些特性的关系表达式会被优化规则处理。
   */
  def setRequestedRootTraits(relTraits: Array[RelTrait]): FlinkHepRuleSetProgramBuilder[OC] = {
    // 设置请求的根特征到 FlinkHepRuleSetProgram 实例中
    hepRuleSetProgram.setRequestedRootTraits(relTraits)
    // 返回构建器自身，支持链式调用
    this
  }
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 构建并返回配置好的 FlinkHepRuleSetProgram 实例。
   */
  def build(): FlinkHepRuleSetProgram[OC] = hepRuleSetProgram

}

object FlinkHepRuleSetProgramBuilder {
  def newBuilder[OC <: FlinkOptimizeContext] = new FlinkHepRuleSetProgramBuilder[OC]
}
