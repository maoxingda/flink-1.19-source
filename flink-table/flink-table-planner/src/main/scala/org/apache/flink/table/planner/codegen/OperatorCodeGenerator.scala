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

import org.apache.flink.streaming.api.graph.StreamConfig
import org.apache.flink.streaming.api.operators.{AbstractStreamOperator, BoundedMultiInput, BoundedOneInput, InputSelectable, InputSelection, OneInputStreamOperator, Output, StreamOperator, TwoInputStreamOperator}
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.runtime.tasks.{ProcessingTimeService, StreamTask}
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.Indenter.toISC
import org.apache.flink.table.planner.utils.Logging
import org.apache.flink.table.runtime.generated.GeneratedOperator
import org.apache.flink.table.types.logical.LogicalType

/** A code generator for generating Flink [[StreamOperator]]s. */
object OperatorCodeGenerator extends Logging {

  val ELEMENT = "element"
  val OUT_ELEMENT = "outElement"

  val STREAM_RECORD: String = className[StreamRecord[_]]
  val INPUT_SELECTION: String = className[InputSelection]

  def addReuseOutElement(ctx: CodeGeneratorContext): Unit = {
    ctx.addReusableMember(s"private final $STREAM_RECORD $OUT_ELEMENT = new $STREAM_RECORD(null);")
  }
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 使用OperatorCodeGenerator生成一个单输入流操作器的代码
   */
  def generateOneInputStreamOperator[IN <: Any, OUT <: Any](
      ctx: CodeGeneratorContext,// 代码生成上下文
      name: String,// 生成的操作符名称
      processCode: String,// 处理逻辑的代码
      inputType: LogicalType,// 输入数据的逻辑类型
      inputTerm: String = CodeGenUtils.DEFAULT_INPUT1_TERM,// 输入数据项的默认名称
      endInputCode: Option[String] = None,// 结束输入时的自定义代码，可选
      lazyInputUnboxingCode: Boolean = false,// 是否延迟输入解包代码，默认为否
      converter: String => String = a => a): GeneratedOperator[OneInputStreamOperator[IN, OUT]] = {// 字符串转换器，默认为恒等转换
    // 添加重用输出元素的逻辑
    addReuseOutElement(ctx)
    // 生成新的操作符名称，避免命名冲突
    val operatorName = newName(ctx, name)
    // 获取操作符的抽象基类名称
    val abstractBaseClass = ctx.getOperatorBaseClass
    // 指定操作符的基类为OneInputStreamOperator[IN, OUT]
    val baseClass = classOf[OneInputStreamOperator[IN, OUT]]
    // 将输入数据的逻辑类型转换为对应的Java类型术语
    val inputTypeTerm = boxedTypeTermForType(inputType)
    // 处理结束输入时的代码，如果endInputCode为Some，则生成重写endInput方法的代码
    val (endInput, endInputImpl) = endInputCode match {
      case None => ("", "")// 如果没有自定义的结束输入代码，则留空
      case Some(code) =>
        (
          s"""
             |@Override
             |public void endInput() throws Exception {
             |  ${ctx.reuseLocalVariableCode()}
             |  $code
             |}
         """.stripMargin,
          s", ${className[BoundedOneInput]}")// 如果需要，添加BoundedOneInput接口的实现
    }
    // 构造操作符类的Java代码

    val operatorCode =
      j"""
      public class $operatorName extends ${abstractBaseClass.getCanonicalName}
          implements ${baseClass.getCanonicalName}$endInputImpl {
         // 引用数组，用于存储可能需要的外部引用
        private final Object[] references;
        ${ctx.reuseMemberCode()}
        // 构造函数
        public $operatorName(
            Object[] references,
            ${className[StreamTask[_, _]]} task,
            ${className[StreamConfig]} config,
            ${className[Output[_]]} output,
            ${className[ProcessingTimeService]} processingTimeService) throws Exception {
          this.references = references;
          ${ctx.reuseInitCode()}
          this.setup(task, config, output);
          if (this instanceof ${className[AbstractStreamOperator[_]]}) {
            ((${className[AbstractStreamOperator[_]]}) this)
              .setProcessingTimeService(processingTimeService);
          }
        }
        // open方法，用于初始化
        @Override
        public void open() throws Exception {
          super.open();
          ${ctx.reuseOpenCode()}
        }
        // processElement方法，处理输入元素
        @Override
        public void processElement($STREAM_RECORD $ELEMENT) throws Exception {
          $inputTypeTerm $inputTerm = ($inputTypeTerm) ${converter(s"$ELEMENT.getValue()")};
          ${ctx.reusePerRecordCode()}
          ${ctx.reuseLocalVariableCode()}
          ${if (lazyInputUnboxingCode) "" else ctx.reuseInputUnboxingCode()}
          $processCode
        }

        $endInput
        // finish方法，在任务结束时调用
        @Override
        public void finish() throws Exception {
            ${ctx.reuseFinishCode()}
            super.finish();
        }
       // close方法，用于清理资源
        @Override
        public void close() throws Exception {
           super.close();
           ${ctx.reuseCloseCode()}
        }

        ${ctx.reuseInnerClassDefinitionCode()}
      }
    """.stripMargin
    // 日志输出，调试信息
    LOG.debug(s"Compiling OneInputStreamOperator Code:\n$name")
    LOG.trace(s"Code: \n$operatorCode")
    // 构造GeneratedOperator实例并返回
    new GeneratedOperator(operatorName, operatorCode, ctx.references.toArray, ctx.tableConfig)
  }

  def generateTwoInputStreamOperator[IN1 <: Any, IN2 <: Any, OUT <: Any](
      ctx: CodeGeneratorContext,
      name: String,
      processCode1: String,
      processCode2: String,
      input1Type: LogicalType,
      input2Type: LogicalType,
      input1Term: String = CodeGenUtils.DEFAULT_INPUT1_TERM,
      input2Term: String = CodeGenUtils.DEFAULT_INPUT2_TERM,
      nextSelectionCode: Option[String] = None,
      endInputCode1: Option[String] = None,
      endInputCode2: Option[String] = None,
      useTimeCollect: Boolean = false): GeneratedOperator[TwoInputStreamOperator[IN1, IN2, OUT]] = {
    addReuseOutElement(ctx)
    val operatorName = newName(ctx, name)
    val abstractBaseClass = ctx.getOperatorBaseClass
    val baseClass = classOf[TwoInputStreamOperator[IN1, IN2, OUT]]
    val inputTypeTerm1 = boxedTypeTermForType(input1Type)
    val inputTypeTerm2 = boxedTypeTermForType(input2Type)

    val (nextSel, nextSelImpl) = nextSelectionCode match {
      case None => ("", "")
      case Some(code) =>
        val end1 = endInputCode1.getOrElse("")
        val end2 = endInputCode2.getOrElse("")
        (
          s"""
             |@Override
             |public $INPUT_SELECTION nextSelection() {
             |  $code
             |}
         """.stripMargin,
          s", ${className[InputSelectable]}")
    }

    val (endInput, endInputImpl) = (endInputCode1, endInputCode2) match {
      case (None, None) => ("", "")
      case (_, _) =>
        val end1 = endInputCode1.getOrElse("")
        val end2 = endInputCode2.getOrElse("")
        (
          s"""
             |private void endInput1() throws Exception {
             |  $end1
             |}
             |
             |private void endInput2() throws Exception {
             |  $end2
             |}
             |
             |@Override
             |public void endInput(int inputId) throws Exception {
             |  switch (inputId) {
             |    case 1:
             |      endInput1();
             |      break;
             |    case 2:
             |      endInput2();
             |      break;
             |  }
             |}
         """.stripMargin,
          s", ${className[BoundedMultiInput]}")
    }

    val operatorCode =
      j"""
      public class $operatorName extends ${abstractBaseClass.getCanonicalName}
          implements ${baseClass.getCanonicalName}$nextSelImpl$endInputImpl {

        public static org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger("$operatorName");

        private final Object[] references;
        ${ctx.reuseMemberCode()}

        public $operatorName(
            Object[] references,
            ${className[StreamTask[_, _]]} task,
            ${className[StreamConfig]} config,
            ${className[Output[_]]} output,
            ${className[ProcessingTimeService]} processingTimeService) throws Exception {
          this.references = references;
          ${ctx.reuseInitCode()}
          this.setup(task, config, output);
          if (this instanceof ${className[AbstractStreamOperator[_]]}) {
            ((${className[AbstractStreamOperator[_]]}) this)
              .setProcessingTimeService(processingTimeService);
          }
        }

        @Override
        public void open() throws Exception {
          super.open();
          ${ctx.reuseOpenCode()}
        }

        @Override
        public void processElement1($STREAM_RECORD $ELEMENT)
         throws Exception {
          ${ctx.reuseLocalVariableCode()}
          $inputTypeTerm1 $input1Term = ${generateInputTerm(inputTypeTerm1)}
          $processCode1
        }

        @Override
        public void processElement2($STREAM_RECORD $ELEMENT)
         throws Exception {
          ${ctx.reuseLocalVariableCode()}
          $inputTypeTerm2 $input2Term = ${generateInputTerm(inputTypeTerm2)}
          $processCode2
        }

        $nextSel

        $endInput

        @Override
        public void finish() throws Exception {
          super.finish();
          ${ctx.reuseFinishCode()}
        }


        @Override
        public void close() throws Exception {
          super.close();
          ${ctx.reuseCloseCode()}
        }

        ${ctx.reuseInnerClassDefinitionCode()}
      }
    """.stripMargin

    LOG.debug(s"Compiling TwoInputStreamOperator Code:\n$name")
    LOG.trace(s"Code: \n$operatorCode")
    new GeneratedOperator(operatorName, operatorCode, ctx.references.toArray, ctx.tableConfig)
  }

  private def generateInputTerm(inputTypeTerm: String): String = {
    s"($inputTypeTerm) $ELEMENT.getValue();"
  }

  def generateCollect(emit: String): String =
    s"$DEFAULT_OPERATOR_COLLECTOR_TERM.collect($OUT_ELEMENT.replace($emit));"

  def generateCollectWithTimestamp(emit: String, timestampTerm: String): String =
    s"$DEFAULT_OPERATOR_COLLECTOR_TERM.collect($OUT_ELEMENT.replace($emit, $timestampTerm));"
}
