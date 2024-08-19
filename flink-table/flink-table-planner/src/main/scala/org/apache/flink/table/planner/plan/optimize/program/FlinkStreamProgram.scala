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

import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.rules.FlinkStreamRuleSets
import org.apache.flink.table.planner.plan.rules.logical.EventTimeTemporalJoinRewriteRule

import org.apache.calcite.plan.hep.HepMatchOrder

/** Defines a sequence of programs to optimize for stream table plan. */
/**
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * 定义了一系列用于优化流表计划的程序序列。
 */
object FlinkStreamProgram {
  // 子查询重写规则，用于优化包含子查询的查询计划。
  val SUBQUERY_REWRITE = "subquery_rewrite"
  // 时态连接重写规则，用于优化涉及时间属性的连接操作。
  val TEMPORAL_JOIN_REWRITE = "temporal_join_rewrite"
  // 去相关化规则，用于消除查询中的相关性，以便更好地并行处理。
  val DECORRELATE = "decorrelate"
  // 默认重写规则，包含了一系列基本的优化重写步骤。
  val DEFAULT_REWRITE = "default_rewrite"
  // 谓词下推规则，用于将过滤条件尽可能地下推到查询计划的低层，以减少数据处理量。
  val PREDICATE_PUSHDOWN = "predicate_pushdown"
  // 连接重排序规则，用于优化连接操作的执行顺序，以提高查询效率。
  val JOIN_REORDER = "join_reorder"
  // 投影重写规则，用于优化查询中的投影操作，减少不必要的数据列处理。
  val PROJECT_REWRITE = "project_rewrite"
  // 逻辑阶段，标志着优化过程中的逻辑优化阶段的开始。
  val LOGICAL = "logical"
  // 逻辑重写规则，在逻辑优化阶段应用的一系列优化规则。
  val LOGICAL_REWRITE = "logical_rewrite"
  // 时间指示符转换规则，用于在需要时转换时间相关的表达式或字段。
  val TIME_INDICATOR = "time_indicator"
  // 物理阶段，标志着优化过程中的物理优化阶段的开始。
  val PHYSICAL = "physical"
  // 物理重写规则，在物理优化阶段应用的一系列优化规则，主要关注于生成高效的执行计划。
  val PHYSICAL_REWRITE = "physical_rewrite"

  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 构建优化规则程序
   */
  def buildProgram(tableConfig: ReadableConfig): FlinkChainedProgram[StreamOptimizeContext] = {
    val chainedProgram = new FlinkChainedProgram[StreamOptimizeContext]()

    // rewrite sub-queries to joins
    // 将子查询重写为连接
    chainedProgram.addLast(
      SUBQUERY_REWRITE,
      FlinkGroupProgramBuilder // 使用FlinkGroupProgramBuilder构建一个包含多个优化步骤的组程序
        .newBuilder[StreamOptimizeContext]
        // rewrite QueryOperationCatalogViewTable before rewriting sub-queries
        // 在重写子查询之前，首先转换表引用
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)// 设置规则的执行类型为顺序执行
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)// 设置规则的匹配顺序为自下而上
            .add(FlinkStreamRuleSets.TABLE_REF_RULES)// 添加处理表引用的规则集
            .build(),
          "convert table references before rewriting sub-queries to semi-join" //在将子查询重写为半连接之前转换表引用
        )
        // 第二个优化程序，将子查询重写为半连接
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)// 设置规则的执行类型为集合执行
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)// 设置规则的匹配顺序为自下而上
            .add(FlinkStreamRuleSets.SEMI_JOIN_RULES) // 添加处理半连接相关的规则集
            .build(),
          "rewrite sub-queries to semi-join" //将子查询重写为半连接
        )
        // 第三个优化程序，进一步处理子查询
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)// 设置规则的执行类型为集合执行
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP) //设置规则的匹配顺序为自下而上
            .add(FlinkStreamRuleSets.TABLE_SUBQUERY_RULES)// 添加处理子查询的规则集
            .build(),
          "sub-queries remove"
        )
        // convert RelOptTableImpl (which exists in SubQuery before) to FlinkRelOptTable
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)// 设置规则的执行类型为集合执行
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP) //设置规则的匹配顺序为自下而上
            .add(FlinkStreamRuleSets.TABLE_REF_RULES)
            .build(),
          "convert table references after sub-queries removed"
        )
        .build()
    )

    // rewrite special temporal join plan
    chainedProgram.addLast(
      TEMPORAL_JOIN_REWRITE,
      FlinkGroupProgramBuilder
        .newBuilder[StreamOptimizeContext]
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamRuleSets.EXPAND_PLAN_RULES)
            .build(),
          "convert correlate to temporal table join"
        )
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamRuleSets.POST_EXPAND_CLEAN_UP_RULES)
            .build(),
          "convert enumerable table scan"
        )
        .build()
    )

    // query decorrelation
    chainedProgram.addLast(
      DECORRELATE,
      FlinkGroupProgramBuilder
        .newBuilder[StreamOptimizeContext]
        // rewrite before decorrelation
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamRuleSets.PRE_DECORRELATION_RULES)
            .build(),
          "pre-rewrite before decorrelation"
        )
        .addProgram(new FlinkDecorrelateProgram)
        .build()
    )

    // default rewrite, includes: predicate simplification, expression reduction, window
    // properties rewrite, etc.
    chainedProgram.addLast(
      DEFAULT_REWRITE,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamRuleSets.DEFAULT_REWRITE_RULES)
        .build()
    )

    // rule based optimization: push down predicate(s) in where clause, so it only needs to read
    // the required data
    chainedProgram.addLast(
      PREDICATE_PUSHDOWN,
      FlinkGroupProgramBuilder
        .newBuilder[StreamOptimizeContext]
        .addProgram(
          FlinkGroupProgramBuilder
            .newBuilder[StreamOptimizeContext]
            .addProgram(
              FlinkHepRuleSetProgramBuilder
                .newBuilder[StreamOptimizeContext]
                .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
                .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                .add(FlinkStreamRuleSets.JOIN_PREDICATE_REWRITE_RULES)
                .build(),
              "join predicate rewrite"
            )
            .addProgram(
              FlinkHepRuleSetProgramBuilder.newBuilder
                .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
                .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                .add(FlinkStreamRuleSets.FILTER_PREPARE_RULES)
                .build(),
              "filter rules"
            )
            .setIterations(5)
            .build(),
          "predicate rewrite"
        )
        .addProgram(
          // PUSH_PARTITION_DOWN_RULES should always be in front of PUSH_FILTER_DOWN_RULES
          // to prevent PUSH_FILTER_DOWN_RULES from consuming the predicates in partitions
          FlinkGroupProgramBuilder
            .newBuilder[StreamOptimizeContext]
            .addProgram(
              FlinkHepRuleSetProgramBuilder.newBuilder
                .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
                .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                .add(FlinkStreamRuleSets.PUSH_PARTITION_DOWN_RULES)
                .build(),
              "push down partitions into table scan"
            )
            .addProgram(
              FlinkHepRuleSetProgramBuilder.newBuilder
                .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
                .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                .add(FlinkStreamRuleSets.PUSH_FILTER_DOWN_RULES)
                .build(),
              "push down filters into table scan"
            )
            .build(),
          "push predicate into table scan"
        )
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamRuleSets.PRUNE_EMPTY_RULES)
            .build(),
          "prune empty after predicate push down"
        )
        .build()
    )

    // join reorder
    if (tableConfig.get(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED)) {
      chainedProgram.addLast(
        JOIN_REORDER,
        FlinkGroupProgramBuilder
          .newBuilder[StreamOptimizeContext]
          .addProgram(
            FlinkHepRuleSetProgramBuilder.newBuilder
              .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
              .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
              .add(FlinkStreamRuleSets.JOIN_REORDER_PREPARE_RULES)
              .build(),
            "merge join into MultiJoin"
          )
          .addProgram(
            FlinkHepRuleSetProgramBuilder.newBuilder
              .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
              .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
              .add(FlinkStreamRuleSets.JOIN_REORDER_RULES)
              .build(),
            "do join reorder"
          )
          .build()
      )
    }

    // project rewrite
    chainedProgram.addLast(
      PROJECT_REWRITE,
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamRuleSets.PROJECT_RULES)
        .build()
    )

    // optimize the logical plan
    chainedProgram.addLast(
      LOGICAL,
      FlinkVolcanoProgramBuilder.newBuilder
        .add(FlinkStreamRuleSets.LOGICAL_OPT_RULES)
        .setRequiredOutputTraits(Array(FlinkConventions.LOGICAL))
        .build()
    )

    // logical rewrite
    chainedProgram.addLast(
      LOGICAL_REWRITE,
      FlinkGroupProgramBuilder
        .newBuilder[StreamOptimizeContext]
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamRuleSets.LOGICAL_REWRITE)
            .build())
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(EventTimeTemporalJoinRewriteRule.EVENT_TIME_TEMPORAL_JOIN_REWRITE_RULES)
            .build())
        .build()
    )

    // convert time indicators
    chainedProgram.addLast(TIME_INDICATOR, new FlinkRelTimeIndicatorProgram)

    // optimize the physical plan
    chainedProgram.addLast(
      PHYSICAL,
      FlinkVolcanoProgramBuilder.newBuilder
        .add(FlinkStreamRuleSets.PHYSICAL_OPT_RULES)
        .setRequiredOutputTraits(Array(FlinkConventions.STREAM_PHYSICAL))
        .build()
    )

    // physical rewrite
    chainedProgram.addLast(
      PHYSICAL_REWRITE,
      FlinkGroupProgramBuilder
        .newBuilder[StreamOptimizeContext]
        // add a HEP program for watermark transpose rules to make this optimization deterministic
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamRuleSets.WATERMARK_TRANSPOSE_RULES)
            .build(),
          "watermark transpose"
        )
        .addProgram(new FlinkChangelogModeInferenceProgram, "Changelog mode inference")
        .addProgram(
          new FlinkMiniBatchIntervalTraitInitProgram,
          "Initialization for mini-batch interval inference")
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
            .setHepMatchOrder(HepMatchOrder.TOP_DOWN)
            .add(FlinkStreamRuleSets.MINI_BATCH_RULES)
            .build(),
          "mini-batch interval rules"
        )
        .addProgram(
          FlinkHepRuleSetProgramBuilder.newBuilder
            .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
            .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
            .add(FlinkStreamRuleSets.PHYSICAL_REWRITE)
            .build(),
          "physical rewrite"
        )
        .build()
    )

    chainedProgram
  }
}
