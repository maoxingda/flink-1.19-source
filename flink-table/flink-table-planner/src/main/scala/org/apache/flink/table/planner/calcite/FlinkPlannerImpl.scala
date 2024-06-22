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
package org.apache.flink.table.planner.calcite

import org.apache.flink.sql.parser.ExtendedSqlNode
import org.apache.flink.sql.parser.ddl.{SqlCompilePlan, SqlReset, SqlSet, SqlUseModules}
import org.apache.flink.sql.parser.dml.{RichSqlInsert, SqlBeginStatementSet, SqlCompileAndExecutePlan, SqlEndStatementSet, SqlExecute, SqlExecutePlan, SqlStatementSet, SqlTruncateTable}
import org.apache.flink.sql.parser.dql._
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.planner.hint.FlinkHints
import org.apache.flink.table.planner.parse.CalciteParser
import org.apache.flink.table.planner.plan.FlinkCalciteCatalogReader
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil

import com.google.common.collect.ImmutableList
import org.apache.calcite.config.NullCollation
import org.apache.calcite.plan._
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelFieldCollation, RelRoot}
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rex.{RexInputRef, RexNode}
import org.apache.calcite.sql.{SqlBasicCall, SqlCall, SqlHint, SqlKind, SqlNode, SqlNodeList, SqlOperatorTable, SqlSelect, SqlTableRef}
import org.apache.calcite.sql.advise.SqlAdvisorValidator
import org.apache.calcite.sql.util.SqlShuttle
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.sql2rel.{SqlRexConvertletTable, SqlToRelConverter}
import org.apache.calcite.tools.{FrameworkConfig, RelConversionException}

import javax.annotation.Nullable

import java.lang.{Boolean => JBoolean}
import java.util
import java.util.Locale
import java.util.function.{Function => JFunction}

import scala.collection.JavaConverters._

/**
 * NOTE: this is heavily inspired by Calcite's PlannerImpl. We need it in order to share the planner
 * between the Table API relational plans and the SQL relation plans that are created by the Calcite
 * parser. The main difference is that we do not create a new RelOptPlanner in the ready() method.
 */
class FlinkPlannerImpl(
    val config: FrameworkConfig,
    catalogReaderSupplier: JFunction[JBoolean, CalciteCatalogReader],
    typeFactory: FlinkTypeFactory,
    val cluster: RelOptCluster) {

  val operatorTable: SqlOperatorTable = config.getOperatorTable
  val parser: CalciteParser = new CalciteParser(config.getParserConfig)
  val convertletTable: SqlRexConvertletTable = config.getConvertletTable
  val sqlToRelConverterConfig: SqlToRelConverter.Config =
    config.getSqlToRelConverterConfig.withAddJsonTypeOperatorEnabled(false)

  var validator: FlinkCalciteSqlValidator = _

  def getSqlAdvisorValidator(): SqlAdvisorValidator = {
    new SqlAdvisorValidator(
      operatorTable,
      catalogReaderSupplier.apply(true), // ignore cases for lenient completion
      typeFactory,
      SqlValidator.Config.DEFAULT
        .withConformance(config.getParserConfig.conformance()))
  }

  /**
   * Get the [[FlinkCalciteSqlValidator]] instance from this planner, create a new instance if
   * current validator has not been initialized, or returns the validator instance directly.
   *
   * <p>The validator instance creation is not thread safe.
   *
   * @return
   *   a new validator instance or current existed one
   */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 从这个规划器中获取 [[FlinkCalciteSqlValidator]] 实例，
     */
  def getOrCreateSqlValidator(): FlinkCalciteSqlValidator = {
    // 如果 validator 为空（即尚未初始化）
    if (validator == null) {
      // 调用 catalogReaderSupplier 的 apply 方法，并传入 false 作为参数，获取 catalogReader
      val catalogReader = catalogReaderSupplier.apply(false)
      // 使用 catalogReader 创建一个新的 FlinkCalciteSqlValidator 实例，并赋值给 validator
      validator = createSqlValidator(catalogReader)
    }
    // 返回 validator 实例
    validator
  }

  private def createSqlValidator(catalogReader: CalciteCatalogReader) = {
    val validator = new FlinkCalciteSqlValidator(
      operatorTable,
      catalogReader,
      typeFactory,
      SqlValidator.Config.DEFAULT
        .withIdentifierExpansion(true)
        .withDefaultNullCollation(FlinkPlannerImpl.defaultNullCollation)
        .withTypeCoercionEnabled(false),
      createToRelContext(),
      cluster,
      config
    ) // Disable implicit type coercion for now.
    validator
  }

  /**
   * @授课老师(微信): yi_locus
   * email: 156184212@qq.com
   * @param aaaa
  */
  def validate(sqlNode: SqlNode): SqlNode = {
    // 获取或创建一个 SQL 验证器实例
    val validator = getOrCreateSqlValidator()
    // 使用 SQL 验证器来验证 sqlNode
    validate(sqlNode, validator)
  }

  /**
   * @授课老师(微信): yi_locus
   * email: 156184212@qq.com
   *
   *
  */
  private def validate(sqlNode: SqlNode, validator: FlinkCalciteSqlValidator): SqlNode = {
    try {
      // 使用 PreValidateReWriter 对 sqlNode 进行预验证和可能的重写
      sqlNode.accept(new PreValidateReWriter(validator, typeFactory))
      // do extended validation.
      // 使用模式匹配进行验证
      sqlNode match {
        // 根据 sqlNode 的类型，如果是 ExtendedSqlNode，则调用其 validate 方法
        case node: ExtendedSqlNode =>
          node.validate()
        // 如果不是 ExtendedSqlNode，则不进行扩展验证
        case _ =>
      }
      // no need to validate row type for DDL and insert nodes.
      // 对于 DDL 和某些特定类型的节点，不需要验证行类型
      // 以下判断中，包含了这些特定类型的 SQL 语句
      if (
        sqlNode.getKind.belongsTo(SqlKind.DDL)// 属于 DDL 类型的节点
        || sqlNode.getKind == SqlKind.CREATE_FUNCTION // 创建函数、删除函数
        || sqlNode.getKind == SqlKind.DROP_FUNCTION
        || sqlNode.getKind == SqlKind.OTHER_DDL  // 其他 DDL 类型的节点
        || sqlNode.isInstanceOf[SqlLoadModule] // 加载模块
        || sqlNode.isInstanceOf[SqlShowCatalogs]  // 显示目录、当前目录、数据库、当前数据库等查询
          || sqlNode.isInstanceOf[SqlShowCurrentCatalog]
        || sqlNode.isInstanceOf[SqlShowDatabases]
        || sqlNode.isInstanceOf[SqlShowCurrentDatabase]
          || sqlNode.isInstanceOf[SqlShowTables] // 显示表、函数、JAR 包、模块、视图、列、分区、过程、作业等查询
        || sqlNode.isInstanceOf[SqlShowFunctions]
        || sqlNode.isInstanceOf[SqlShowJars]
        || sqlNode.isInstanceOf[SqlShowModules]
        || sqlNode.isInstanceOf[SqlShowViews]
        || sqlNode.isInstanceOf[SqlShowColumns]
        || sqlNode.isInstanceOf[SqlShowPartitions]
        || sqlNode.isInstanceOf[SqlShowProcedures]
        || sqlNode.isInstanceOf[SqlShowJobs]
        || sqlNode.isInstanceOf[SqlRichDescribeTable]
        || sqlNode.isInstanceOf[SqlUnloadModule]
        || sqlNode.isInstanceOf[SqlUseModules]
        || sqlNode.isInstanceOf[SqlBeginStatementSet]
        || sqlNode.isInstanceOf[SqlEndStatementSet]
        || sqlNode.isInstanceOf[SqlSet]
        || sqlNode.isInstanceOf[SqlReset]
        || sqlNode.isInstanceOf[SqlExecutePlan]
        || sqlNode.isInstanceOf[SqlTruncateTable]
      ) {
        return sqlNode
      }
      // 对给定的 SQL 节点进行验证，并可能对其进行修改
      sqlNode match {
        // 如果 sqlNode 是 SqlRichExplain 类型（可能是一个复杂的查询解释）
        case richExplain: SqlRichExplain =>
          // 匹配 richExplain 中的语句，并进行验证
        val validatedStatement = richExplain.getStatement match {
            // only validate source here
          // 只验证源语句（可能是 INSERT 语句）
          case insert: RichSqlInsert =>
            // 调用 validateRichSqlInsert 方法验证 INSERT 语句
          validateRichSqlInsert(insert)
          // 对于其他类型的语句，调用通用的 validate 方法进行验证
          case others =>
              validate(others)
          }
          // 将验证后的语句设置回 richExplain 中
          richExplain.setOperand(0, validatedStatement)
          // 返回修改后的 richExplain
          richExplain
        // 如果 sqlNode 是 SqlStatementSet 类型（可能包含多个 SQL 语句）
        case statementSet: SqlStatementSet =>
          // 遍历 statementSet 中的 INSERT 语句，并逐个进行验证
        statementSet.getInserts.asScala.zipWithIndex.foreach {
          // 调用 validate 方法验证 INSERT 语句，并将验证后的语句设置回 statementSet 中
          case (insert, idx) => statementSet.setOperand(idx, validate(insert))
          }
          // 返回修改后的 statementSet
          statementSet
        // 如果 sqlNode 是 SqlExecute 类型（可能是一个执行计划）
        case execute: SqlExecute =>
          // 验证 execute 中的语句，并将验证后的语句设置回 execute 中
        execute.setOperand(0, validate(execute.getStatement))
          // 返回修改后的 execute
          execute
        // 如果 sqlNode 是 RichSqlInsert 类型（可能是 INSERT 语句）
        case insert: RichSqlInsert =>
          // 调用 validateRichSqlInsert 方法验证 INSERT 语句
        validateRichSqlInsert(insert)
        // 如果 sqlNode 是 SqlCompilePlan 类型（可能是编译后的 SQL 计划）
        case compile: SqlCompilePlan =>
          // 验证 compile 中的第一个操作数，并设置回 compile 中
        compile.setOperand(0, validate(compile.getOperandList.get(0)))
          // 返回修改后的 compile
          compile

        // 如果 sqlNode 是 SqlCompileAndExecutePlan 类型（可能是编译并执行的 SQL 计划）
        case compileAndExecute: SqlCompileAndExecutePlan =>
          // 验证 compileAndExecute 中的第一个操作数，并设置回 compileAndExecute 中
        compileAndExecute.setOperand(0, validate(compileAndExecute.getOperandList.get(0)))
          // 返回修改后的 compileAndExecute
          compileAndExecute
        // for call procedure statement
        // 如果 sqlNode 是调用存储过程的语句
        case sqlCallNode if sqlCallNode.getKind == SqlKind.PROCEDURE_CALL =>
          // 将 sqlCallNode 转换为 SqlBasicCall 类型
        val callNode = sqlCallNode.asInstanceOf[SqlBasicCall]
          // 遍历 callNode 中的操作数，并逐个进行验证
          callNode.getOperandList.asScala.zipWithIndex.foreach {
            // 调用 validate 方法验证操作数，并设置回 callNode 中
            case (operand, idx) => callNode.setOperand(idx, validate(operand))
          }
          // 返回修改后的 callNode
          callNode
        // 如果 sqlNode 不是上述任何类型，则使用 validator 进行默认验证
        case _ =>
          validator.validate(sqlNode)
      }
    } catch {
      // 捕获可能出现的运行时异常，并转换为 ValidationException 异常抛出
      case e: RuntimeException =>
        throw new ValidationException(s"SQL validation failed. ${e.getMessage}", e)
    }
  }

  def rel(validatedSqlNode: SqlNode): RelRoot = {
    rel(validatedSqlNode, getOrCreateSqlValidator())
  }

  private def rel(validatedSqlNode: SqlNode, sqlValidator: FlinkCalciteSqlValidator) = {
    try {
      assert(validatedSqlNode != null)
      // check whether this SqlNode tree contains query hints
      val checkContainQueryHintsShuttle = new CheckContainQueryHintsShuttle
      validatedSqlNode.accept(checkContainQueryHintsShuttle)
      val sqlToRelConverter: SqlToRelConverter =
        if (checkContainQueryHintsShuttle.containsQueryHints) {
          val converter = createSqlToRelConverter(
            sqlValidator,
            // disable project merge during sql to rel phase to prevent
            // incorrect propagation of query hints into child query block
            sqlToRelConverterConfig.addRelBuilderConfigTransform(c => c.withBloat(-1))
          )
          // TODO currently, it is a relatively hacked way to tell converter
          // that this SqlNode tree contains query hints
          converter.containsQueryHints()
          converter
        } else {
          createSqlToRelConverter(sqlValidator, sqlToRelConverterConfig)
        }

      sqlToRelConverter.convertQuery(validatedSqlNode, false, true)
      // we disable automatic flattening in order to let composite types pass without modification
      // we might enable it again once Calcite has better support for structured types
      // root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true))

      // TableEnvironment.optimize will execute the following
      // root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel))
      // convert time indicators
      // root = root.withRel(RelTimeIndicatorConverter.convert(root.rel, rexBuilder))
    } catch {
      case e: RelConversionException => throw new TableException(e.getMessage)
    }
  }

  private class CheckContainQueryHintsShuttle extends SqlShuttle {
    var containsQueryHints: Boolean = false

    override def visit(call: SqlCall): SqlNode = {
      call match {
        case select: SqlSelect =>
          if (select.hasHints && hasQueryHints(select.getHints.getList)) {
            containsQueryHints = true
            return call
          }
        case table: SqlTableRef =>
          val hintList = table.getOperandList.get(1).asInstanceOf[SqlNodeList]
          if (hasQueryHints(hintList.getList)) {
            containsQueryHints = true
            return call
          }
        case _ => // ignore
      }
      super.visit(call)
    }

    private def hasQueryHints(hints: util.List[SqlNode]): Boolean = {
      JavaScalaConversionUtil.toScala(hints).foreach {
        case hint: SqlHint =>
          val hintName = hint.getName
          if (FlinkHints.isQueryHint(hintName.toUpperCase(Locale.ROOT))) {
            return true
          }
      }
      false
    }
  }

  def validateExpression(
      sqlNode: SqlNode,
      inputRowType: RelDataType,
      @Nullable outputType: RelDataType): SqlNode = {
    validateExpression(sqlNode, getOrCreateSqlValidator(), inputRowType, outputType)
  }

  private def validateExpression(
      sqlNode: SqlNode,
      sqlValidator: FlinkCalciteSqlValidator,
      inputRowType: RelDataType,
      @Nullable outputType: RelDataType): SqlNode = {
    val nameToTypeMap = new util.HashMap[String, RelDataType]()
    inputRowType.getFieldList.asScala
      .foreach(f => nameToTypeMap.put(f.getName, f.getType))
    if (outputType != null) {
      sqlValidator.setExpectedOutputType(sqlNode, outputType)
    }
    sqlValidator.validateParameterizedExpression(sqlNode, nameToTypeMap)
  }

  private def validateRichSqlInsert(insert: RichSqlInsert): SqlNode = {
    // We don't support UPSERT INTO semantics (see FLINK-24225).
    if (insert.isUpsert) {
      throw new ValidationException(
        "UPSERT INTO statement is not supported. Please use INSERT INTO instead.")
    }
    // only validate source here.
    // ignore row type which will be verified in table environment.
    val validatedSource = validate(insert.getSource)
    insert.setOperand(2, validatedSource)
    insert
  }

  def rex(
      sqlNode: SqlNode,
      inputRowType: RelDataType,
      @Nullable outputType: RelDataType): RexNode = {
    rex(sqlNode, getOrCreateSqlValidator(), inputRowType, outputType)
  }

  private def rex(
      sqlNode: SqlNode,
      sqlValidator: FlinkCalciteSqlValidator,
      inputRowType: RelDataType,
      @Nullable outputType: RelDataType) = {
    try {
      val validatedSqlNode = validateExpression(sqlNode, sqlValidator, inputRowType, outputType)
      val sqlToRelConverter = createSqlToRelConverter(sqlValidator, sqlToRelConverterConfig)
      val nameToNodeMap = inputRowType.getFieldList.asScala
        .map(field => (field.getName, RexInputRef.of(field.getIndex, inputRowType)))
        .toMap[String, RexNode]
        .asJava
      sqlToRelConverter.convertExpression(validatedSqlNode, nameToNodeMap)
    } catch {
      case e: RelConversionException => throw new TableException(e.getMessage)
    }
  }

  private def createSqlToRelConverter(
      sqlValidator: SqlValidator,
      config: SqlToRelConverter.Config): SqlToRelConverter = {
    new SqlToRelConverter(
      createToRelContext(),
      sqlValidator,
      sqlValidator.getCatalogReader.unwrap(classOf[CalciteCatalogReader]),
      cluster,
      convertletTable,
      config)
  }

  /** Creates a new instance of [[RelOptTable.ToRelContext]] for [[RelOptTable]]. */
  def createToRelContext(): RelOptTable.ToRelContext = new ToRelContextImpl

  /**
   * Implements [[RelOptTable.ToRelContext]] interface for [[RelOptTable]] and
   * [[org.apache.calcite.tools.Planner]].
   */
  class ToRelContextImpl extends RelOptTable.ToRelContext {

    override def expandView(
        rowType: RelDataType,
        queryString: String,
        schemaPath: util.List[String],
        viewPath: util.List[String]): RelRoot = {
      val parsed = parser.parse(queryString)
      val originalReader = catalogReaderSupplier.apply(false)
      val readerWithPathAdjusted = new FlinkCalciteCatalogReader(
        originalReader.getRootSchema,
        List(schemaPath, schemaPath.subList(0, 1)).asJava,
        originalReader.getTypeFactory,
        originalReader.getConfig
      )
      val validator = createSqlValidator(readerWithPathAdjusted)
      val validated = validate(parsed, validator)
      rel(validated, validator)
    }

    override def getCluster: RelOptCluster = cluster

    override def getTableHints: util.List[RelHint] = ImmutableList.of()
  }
}

object FlinkPlannerImpl {

  /**
   * the null default direction if not specified. Consistent with HIVE/SPARK/MYSQL/FLINK-RUNTIME. So
   * the default value only is set [[NullCollation.LOW]] for keeping consistent with FLINK-RUNTIME.
   * [[NullCollation.LOW]] means null values appear first when the order is ASC (ascending), and
   * ordered last when the order is DESC (descending).
   */
  val defaultNullCollation: NullCollation = NullCollation.LOW

  /** the default field collation if not specified, Consistent with CALCITE. */
  val defaultCollationDirection: RelFieldCollation.Direction = RelFieldCollation.Direction.ASCENDING
}
