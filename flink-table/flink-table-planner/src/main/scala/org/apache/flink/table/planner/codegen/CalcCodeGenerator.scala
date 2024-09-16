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
package org.apache.flink.table.planner.codegen

import org.apache.flink.api.common.functions.{FlatMapFunction, Function}
import org.apache.flink.api.dag.Transformation
import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.data.{BoxedWrapperRowData, RowData}
import org.apache.flink.table.functions.FunctionKind
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.runtime.generated.GeneratedFunction
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.RowType

import org.apache.calcite.rex._

object CalcCodeGenerator {

  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 生成一个计算操作器的工厂类。
   *
   * @param ctx 代码生成上下文，包含代码生成所需的各种配置和类加载器。
   * @param inputTransform 输入的Flink Transformation，表示数据的来源。
   * @param outputType 输出数据的类型，这里假设为RowType。
   * @param projection 投影表达式列表，用于指定输出数据中应包含哪些字段。
   * @param condition 可选的条件表达式，用于过滤数据。
   * @param retainHeader 是否保留输入数据中的头部信息（如时间戳等）。
   * @param opName 操作器的名称，用于调试和日志记录。
   * @return 一个能够创建计算操作器的工厂类。
   */
  def generateCalcOperator(
      ctx: CodeGeneratorContext,
      inputTransform: Transformation[RowData],
      outputType: RowType,
      projection: Seq[RexNode],
      condition: Option[RexNode],
      retainHeader: Boolean = false,
      opName: String): CodeGenOperatorFactory[RowData] = {
    // 获取输入Transformation的输出类型，并将其转换为RowType
    val inputType = inputTransform.getOutputType
      .asInstanceOf[InternalTypeInfo[RowData]]
      .toRowType
    // filter out time attributes
    // 输入项的默认名称，用于代码生成
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM
    // 生成处理数据的代码，包括投影和条件过滤
    val processCode = generateProcessCode(
      ctx,// 代码生成上下文
      inputType,// 输入数据的类型
      outputType,// 输出数据的类型
      classOf[BoxedWrapperRowData],//使用BoxedWrapperRowData作为中间处理的数据类型
      projection, // 投影表达式
      condition,// 条件表达式
      eagerInputUnboxingCode = true,// 是否在输入时立即解箱（即转换为Java对象）
      retainHeader = retainHeader)// 是否保留头部信息
    // 使用OperatorCodeGenerator生成一个单输入流操作器的代码
    val genOperator =
      OperatorCodeGenerator.generateOneInputStreamOperator[RowData, RowData](
        ctx,// 代码生成上下文
        opName,// 操作器名称
        processCode,// 处理数据的代码
        inputType,// 输入数据的类型
        inputTerm = inputTerm,// 输入项的名称
        lazyInputUnboxingCode = true)// 是否在需要时延迟解箱输入数据
    // 创建一个能够创建计算操作器的工厂类
    new CodeGenOperatorFactory(genOperator)
  }

  private[flink] def generateFunction[T <: Function](
      inputType: RowType,
      name: String,
      returnType: RowType,
      outRowClass: Class[_ <: RowData],
      calcProjection: Seq[RexNode],
      calcCondition: Option[RexNode],
      tableConfig: ReadableConfig,
      classLoader: ClassLoader): GeneratedFunction[FlatMapFunction[RowData, RowData]] = {
    val ctx = new CodeGeneratorContext(tableConfig, classLoader)
    val inputTerm = CodeGenUtils.DEFAULT_INPUT1_TERM
    val collectorTerm = CodeGenUtils.DEFAULT_COLLECTOR_TERM
    val processCode = generateProcessCode(
      ctx,
      inputType,
      returnType,
      outRowClass,
      calcProjection,
      calcCondition,
      collectorTerm = collectorTerm,
      eagerInputUnboxingCode = false,
      outputDirectly = true
    )

    FunctionCodeGenerator.generateFunction(
      ctx,
      name,
      classOf[FlatMapFunction[RowData, RowData]],
      processCode,
      returnType,
      inputType,
      input1Term = inputTerm,
      collectorTerm = collectorTerm)
  }
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 动态代码生成
   */
  private[flink] def generateProcessCode(
      ctx: CodeGeneratorContext,// 代码生成上下文
      inputType: RowType,// 输入行类型
      outRowType: RowType,// 输出行类型
      outRowClass: Class[_ <: RowData],// 输出行类的Class
      projection: Seq[RexNode],// 投影表达式
      condition: Option[RexNode],// 条件表达式
      inputTerm: String = CodeGenUtils.DEFAULT_INPUT1_TERM,// 输入项名称
      collectorTerm: String = CodeGenUtils.DEFAULT_OPERATOR_COLLECTOR_TERM,// 收集器项名称
      eagerInputUnboxingCode: Boolean,// 是否急切地生成输入解包代码
      retainHeader: Boolean = false,// 是否保留头信息
      outputDirectly: Boolean = false): String = {// 是否直接输出

    // according to the SQL standard, every table function should also be a scalar function
    // but we don't allow that for now
    // 对投影中的每个表达式应用ScalarFunctionsValidator验证器
    projection.foreach(_.accept(ScalarFunctionsValidator))
    // 对条件中的每个表达式应用ScalarFunctionsValidator验证器
    condition.foreach(_.accept(ScalarFunctionsValidator))

    // 创建一个表达式代码生成器，绑定输入类型和输入项
    val exprGenerator = new ExprCodeGenerator(ctx, false)
      .bindInput(inputType, inputTerm = inputTerm)


    // 判断是否只有过滤操作，即投影中的表达式数量与输入类型的字段数量相同，
    // 并且每个投影表达式都是对输入字段的直接引用，且引用索引与字段索引相同
    val onlyFilter = projection.lengthCompare(inputType.getFieldCount) == 0 &&
      projection.zipWithIndex.forall {
        case (rexNode, index) =>
          rexNode.isInstanceOf[RexInputRef] && rexNode.asInstanceOf[RexInputRef].getIndex == index
      }
    // 定义生成输出代码的方法，根据是否直接输出到收集器来生成不同的代码
    def produceOutputCode(resultTerm: String): String = if (outputDirectly) {
      s"$collectorTerm.collect($resultTerm);"
    } else {
      s"${OperatorCodeGenerator.generateCollect(resultTerm)}"
    }

    // 定义生成投影代码的方法
    def produceProjectionCode: String = {
      // 生成投影表达式
      val projectionExprs = projection.map(exprGenerator.generateExpression)
      // 生成结果表达式
      val projectionExpression =
        exprGenerator.generateResultExpression(projectionExprs, outRowType, outRowClass)

      // 获取结果表达式的代码
      val projectionExpressionCode = projectionExpression.code
      // 根据是否保留头信息生成不同的代码
      val header = if (retainHeader) {
        s"${projectionExpression.resultTerm}.setRowKind($inputTerm.getRowKind());"
      } else {
        ""
      }
      // 拼接生成投影代码
      s"""
         |$header
         |$projectionExpressionCode
         |${produceOutputCode(projectionExpression.resultTerm)}
         |""".stripMargin
    }
    // 如果条件为空且只有过滤操作，则抛出异常，因为此计算既没有有用的投影也没有过滤，应该被CalcRemoveRule移除
    if (condition.isEmpty && onlyFilter) {
      throw new TableException(
        "This calc has no useful projection and no filter. " +
          "It should be removed by CalcRemoveRule.")
    } else if (condition.isEmpty) { // only projection // 如果只有投影
      val projectionCode = produceProjectionCode// 生成投影代码
      s"""
         |${if (eagerInputUnboxingCode) ctx.reuseInputUnboxingCode() else ""}
         |$projectionCode
         |""".stripMargin
    } else {// 如果有条件（过滤）
      val filterCondition = exprGenerator.generateExpression(condition.get) // 生成过滤条件表达式
      // only filter // 如果只有过滤
      if (onlyFilter) {
        s"""
           |${if (eagerInputUnboxingCode) ctx.reuseInputUnboxingCode() else ""}
           |${filterCondition.code}
           |if (${filterCondition.resultTerm}) {
           |  ${produceOutputCode(inputTerm)}
           |}
           |""".stripMargin
      } else { // both filter and projection // 既有过滤也有投影
        val filterInputCode = ctx.reuseInputUnboxingCode()// 重用输入解包代码
        val filterInputSet = Set(ctx.reusableInputUnboxingExprs.keySet.toSeq: _*)// 获取已重用的输入解包表达式集合

        // if any filter conditions, projection code will enter an new scope
        // 如果有任何过滤条件，投影代码将进入一个新的作用域
        val projectionCode = produceProjectionCode// 生成投影代码
        // 过滤掉已用于过滤的输入解包表达式
        val projectionInputCode = ctx.reusableInputUnboxingExprs
          .filter(entry => !filterInputSet.contains(entry._1))
          .values
          .map(_.code)
          .mkString("\n")// 将剩余的输入解包表达式代码拼接成一个字符串
        // 如果需要急切地解包输入，则使用过滤输入解包代码
        // 过滤条件代码
        // 如果过滤条件为真
        // 如果需要急切地解包输入，则使用投影输入解包代码
        // 投影代码
        s"""
           |${if (eagerInputUnboxingCode) filterInputCode else ""}
           |${filterCondition.code}
           |if (${filterCondition.resultTerm}) {
           |  ${if (eagerInputUnboxingCode) projectionInputCode else ""}
           |  $projectionCode
           |}
           |""".stripMargin
      }
    }
  }

  private object ScalarFunctionsValidator extends RexVisitorImpl[Unit](true) {
    override def visitCall(call: RexCall): Unit = {
      super.visitCall(call)
      call.getOperator match {
        case bsf: BridgingSqlFunction if bsf.getDefinition.getKind != FunctionKind.SCALAR =>
          throw new ValidationException(
            s"Invalid use of function '$bsf'. " +
              s"Currently, only scalar functions can be used in a projection or filter operation.")
        case _ => // ok
      }
    }
  }
}
