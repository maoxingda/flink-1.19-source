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
package org.apache.flink.table.planner.delegation

import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.graph.StreamGraph
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.catalog._
import org.apache.flink.table.catalog.ManagedTableListener.isManagedTable
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.delegation.{Executor, Parser, ParserFactory, Planner}
import org.apache.flink.table.factories.{DynamicTableSinkFactory, FactoryUtil, TableFactoryUtil}
import org.apache.flink.table.module.{Module, ModuleManager}
import org.apache.flink.table.operations._
import org.apache.flink.table.operations.OutputConversionModifyOperation.UpdateMode
import org.apache.flink.table.planner.JMap
import org.apache.flink.table.planner.calcite._
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema
import org.apache.flink.table.planner.connectors.DynamicSinkUtils
import org.apache.flink.table.planner.connectors.DynamicSinkUtils.validateSchemaAndApplyImplicitCast
import org.apache.flink.table.planner.hint.FlinkHints
import org.apache.flink.table.planner.operations.PlannerQueryOperation
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalLegacySink
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNodeGraph, ExecNodeGraphGenerator}
import org.apache.flink.table.planner.plan.nodes.exec.processor.{ExecNodeGraphProcessor, ProcessorContext}
import org.apache.flink.table.planner.plan.nodes.exec.serde.SerdeContext
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.planner.plan.optimize.Optimizer
import org.apache.flink.table.planner.sinks.DataStreamTableSink
import org.apache.flink.table.planner.sinks.TableSinkUtils.{inferSinkPhysicalSchema, validateLogicalPhysicalTypesCompatible, validateTableSink}
import org.apache.flink.table.planner.utils.InternalConfigOptions.{TABLE_QUERY_CURRENT_DATABASE, TABLE_QUERY_START_EPOCH_TIME, TABLE_QUERY_START_LOCAL_TIME}
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.{toJava, toScala}
import org.apache.flink.table.planner.utils.TableConfigUtils
import org.apache.flink.table.runtime.generated.CompileUtils
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter

import _root_.scala.collection.JavaConversions._
import org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema
import org.apache.calcite.plan.{RelTrait, RelTraitDef}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.logical.LogicalTableModify

import java.lang.{Long => JLong}
import java.util
import java.util.{Collections, TimeZone}

import scala.collection.mutable

/**
 * Implementation of a [[Planner]]. It supports only streaming use cases. (The new
 * [[org.apache.flink.table.sources.InputFormatTableSource]] should work, but will be handled as
 * streaming sources, and no batch specific optimizations will be applied).
 *
 * @param executor
 *   instance of [[Executor]], needed to extract [[StreamExecutionEnvironment]] for
 *   [[org.apache.flink.table.sources.StreamTableSource.getDataStream]]
 * @param tableConfig
 *   mutable configuration passed from corresponding [[TableEnvironment]]
 * @param moduleManager
 *   manager for modules
 * @param functionCatalog
 *   catalog of functions
 * @param catalogManager
 *   manager of catalog meta objects such as tables, views, databases etc.
 * @param isStreamingMode
 *   Determines if the planner should work in a batch (false}) or streaming (true) mode.
 */
abstract class PlannerBase(
    executor: Executor,
    tableConfig: TableConfig,
    val moduleManager: ModuleManager,
    val functionCatalog: FunctionCatalog,
    val catalogManager: CatalogManager,
    isStreamingMode: Boolean,
    classLoader: ClassLoader)
  extends Planner {

  private var parserFactory: ParserFactory = _
  private var parser: Parser = _
  private var currentDialect: SqlDialect = getTableConfig.getSqlDialect
  // the transformations generated in translateToPlan method, they are not connected
  // with sink transformations but also are needed in the final graph.
  private[flink] val extraTransformations = new util.ArrayList[Transformation[_]]()

  @VisibleForTesting
  private[flink] val plannerContext: PlannerContext =
    new PlannerContext(
      !isStreamingMode,
      tableConfig,
      moduleManager,
      functionCatalog,
      catalogManager,
      asRootSchema(new CatalogManagerCalciteSchema(catalogManager, isStreamingMode)),
      getTraitDefs.toList,
      classLoader
    )

  private[flink] def createRelBuilder: FlinkRelBuilder = {
    plannerContext.createRelBuilder()
  }

  @VisibleForTesting
  private[flink] def createFlinkPlanner: FlinkPlannerImpl = {
    plannerContext.createFlinkPlanner()
  }

  private[flink] def getTypeFactory: FlinkTypeFactory = plannerContext.getTypeFactory

  protected def getTraitDefs: Array[RelTraitDef[_ <: RelTrait]]

  protected def getOptimizer: Optimizer

  def getTableConfig: TableConfig = tableConfig

  def getFlinkContext: FlinkContext = plannerContext.getFlinkContext

  /**
   * @deprecated
   *   Do not use this method anymore. Use [[getTableConfig]] to access options. Create
   *   transformations without it. A [[StreamExecutionEnvironment]] is a mixture of executor and
   *   stream graph generator/builder. In the long term, we would like to avoid the need for it in
   *   the planner module.
   */
  @deprecated
  private[flink] def getExecEnv: StreamExecutionEnvironment = {
    executor.asInstanceOf[DefaultExecutor].getExecutionEnvironment
  }

  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 构建ParserFactory
   */
  def getParserFactory: ParserFactory = {
    // 如果parserFactory为空或者当前表的配置中的SQL方言与当前方言不一致
    if (parserFactory == null || getTableConfig.getSqlDialect != currentDialect) {
      // 获取当前SQL方言的名称，并将其转换为小写
      val factoryIdentifier = getTableConfig.getSqlDialect.name().toLowerCase
      // 使用FactoryUtil工具类发现（或加载）与指定方言标识符相匹配的ParserFactory
      // 参数包括：类加载器（用于加载类），ParserFactory的类类型，以及方言标识符
      parserFactory = FactoryUtil.discoverFactory(
        getClass.getClassLoader,
        classOf[ParserFactory],
        factoryIdentifier)
      // 更新当前方言为表的配置中的SQL方言
      currentDialect = getTableConfig.getSqlDialect
      // 方言已经改变，重置parser为null，以便下次调用getParser时重新创建
      parser = null
    }
    // 返回parserFactory对象
    parserFactory
  }

  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 定义一个覆盖（override）的def方法，返回一个Parser对象
   */
  override def getParser: Parser = {
    if (parser == null || getTableConfig.getSqlDialect != currentDialect) {
      // 获取Parser的工厂（Factory）
      parserFactory = getParserFactory
      // 使用Parser工厂创建一个新的Parser对象，传入一个DefaultCalciteContext对象，
      // 该对象基于catalogManager和plannerContext构建
      parser = parserFactory.create(new DefaultCalciteContext(catalogManager, plannerContext))
    }
    // 返回parser对象
    parser
  }

  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 将Operation转换为Flink Transformation
   */
  override def translate(
      modifyOperations: util.List[ModifyOperation]): util.List[Transformation[_]] = {
    // 在翻译操作之前执行的前置处理,配置时间，获取数据库名
    beforeTranslation()
    // 检查是否有修改操作需要转换，如果没有则直接返回一个空的转换列表
    if (modifyOperations.isEmpty) {
      return List.empty[Transformation[_]]
    }
    // 将每个修改操作转换为关系节点（RelNode），这些节点是逻辑计划的一部分
    val relNodes = modifyOperations.map(translateToRel)
    // 对关系节点进行优化，生成优化后的关系节点列表
    val optimizedRelNodes = optimize(relNodes)
    // 将优化后的关系节点列表翻译为执行节点图（ExecNodeGraph），这里指定不直接编译为物理计划
    val execGraph = translateToExecNodeGraph(optimizedRelNodes, isCompiled = false)
    // 将执行节点图翻译为物理执行计划，即一系列的转换操作（Transformation）
    val transformations = translateToPlan(execGraph)
    // 在翻译操作之后执行的后置处理
    afterTranslation()
    // 返回最终的转换操作列表
    transformations
  }

  /** Converts a relational tree of [[ModifyOperation]] into a Calcite relational expression. */
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 将一个由[[ModifyOperation]]组成的关系树转换为Calcite的关系表达式。
   */
  @VisibleForTesting
  private[flink] def translateToRel(modifyOperation: ModifyOperation): RelNode = {
    // 获取数据类型工厂
    val dataTypeFactory = catalogManager.getDataTypeFactory
    modifyOperation match {
      // 处理未注册的Sink修改操作
      case s: UnregisteredSinkModifyOperation[_] =>
        // 构建子查询的Calcite关系表达式
        val input = createRelBuilder.queryOperation(s.getChild).build()
        // 获取Sink的表结构
        val sinkSchema = s.getSink.getTableSchema
        // validate query schema and sink schema, and apply cast if possible
        // 验证查询的schema和Sink的schema，并在可能的情况下应用隐式类型转换
        val query = validateSchemaAndApplyImplicitCast(
          input,
          catalogManager.getSchemaResolver.resolve(sinkSchema.toSchema),
          null,
          dataTypeFactory,
          getTypeFactory)
        // 创建表示未注册Sink的逻辑节点
        LogicalLegacySink.create(
          query,
          s.getSink,
          "UnregisteredSink",
          ConnectorCatalogTable.sink(s.getSink, !isStreamingMode))

      case collectModifyOperation: CollectModifyOperation =>// 处理收集（Collect）修改操作
        val input = createRelBuilder.queryOperation(modifyOperation.getChild).build()// 构建子查询的Calcite关系表达式
        // 将收集操作转换为Calcite的关系表达式
        DynamicSinkUtils.convertCollectToRel(
          createRelBuilder,// 创建关系构建器
          input,// 输入的Calcite关系表达式
          collectModifyOperation,// 收集操作实例
          getTableConfig,// 获取表配置
          getFlinkContext.getClassLoader// 获取类加载器
        )

      case stagedSink: StagedSinkModifyOperation =>// 处理分阶段Sink修改操作
        val input = createRelBuilder.queryOperation(modifyOperation.getChild).build()
        // 将分阶段Sink转换为Calcite的关系表达式
        DynamicSinkUtils.convertSinkToRel(
          createRelBuilder,
          input,
          stagedSink,
          stagedSink.getDynamicTableSink)

      case catalogSink: SinkModifyOperation =>// 处理基于Catalog的Sink修改操作
        // 构建子查询的Calcite关系表达式
      val input = createRelBuilder.queryOperation(modifyOperation.getChild).build()
        // 获取动态选项，这些选项可能用于配置Sink
        val dynamicOptions = catalogSink.getDynamicOptions
        // 尝试从表上下文中获取TableSink
        getTableSink(catalogSink.getContextResolvedTable, dynamicOptions).map {
          case (table, sink: TableSink[_]) =>
            // Legacy tables can't be anonymous
            // 获取表的标识符（用于错误消息和日志）
          val identifier = catalogSink.getContextResolvedTable.getIdentifier

            // check it's not for UPDATE/DELETE because they're not supported for Legacy table
            // 检查是否是DELETE或UPDATE操作，因为这些操作在Legacy TableSink中不受支持
            if (catalogSink.isDelete || catalogSink.isUpdate) {
              throw new TableException(
                String.format(
                  "Can't perform %s operation of the table %s " +
                    " because the corresponding table sink is the legacy TableSink," +
                    " Please implement %s for it.",
                  if (catalogSink.isDelete) "delete" else "update",
                  identifier,
                  classOf[DynamicTableSink].getName
                ))
            }

            // check the logical field type and physical field type are compatible
            // 检查逻辑字段类型和物理字段类型是否兼容
            val queryLogicalType = FlinkTypeFactory.toLogicalRowType(input.getRowType)
            // validate logical schema and physical schema are compatible
            // 验证逻辑模式和物理模式是否兼容
            validateLogicalPhysicalTypesCompatible(table, sink, queryLogicalType)
            // validate TableSink
            // 验证TableSink
            validateTableSink(catalogSink, identifier, sink, table.getPartitionKeys)
            // validate query schema and sink schema, and apply cast if possible
            // 验证查询的schema和Sink的schema，并在可能的情况下应用隐式类型转换
            val query = validateSchemaAndApplyImplicitCast(
              input,// 输入的Calcite关系表达式
              table.getResolvedSchema,// 表的解析后的schema
              identifier.asSummaryString, // 表的标识符，用于日志和错误消息
              dataTypeFactory, // 数据类型工厂
              getTypeFactory)  // Calcite的类型工厂
            // 准备额外的执行提示（如动态选项）
            val hints = new util.ArrayList[RelHint]
            if (!dynamicOptions.isEmpty) {
              // 如果存在动态选项，将它们作为提示添加到列表中
              hints.add(RelHint.builder("OPTIONS").hintOptions(dynamicOptions).build)
            }
            //构建Sink
            LogicalLegacySink.create(
              query,
              hints,
              sink,
              identifier.toString,
              table,
              catalogSink.getStaticPartitions.toMap)

          case (table, sink: DynamicTableSink) =>// 处理DynamicTableSink的情况
            // 使用DynamicSinkUtils工具类将DynamicTableSink转换为Calcite的RelNode
          DynamicSinkUtils.convertSinkToRel(createRelBuilder, input, catalogSink, sink)
          // 对上述DynamicTableSink处理的结果进行匹配
          // 如果成功创建了Sink的RelNode，则返回它；否则抛出异常
        } match {
          case Some(sinkRel) => sinkRel
          case None =>
            throw new TableException(
              s"Sink '${catalogSink.getContextResolvedTable}' does not exists")
        }
      // 处理外部修改操作的情况（如外部系统如Kafka的修改）
      case externalModifyOperation: ExternalModifyOperation =>
        // 构建子查询的Calcite关系表达式
      val input = createRelBuilder.queryOperation(modifyOperation.getChild).build()
        // 使用DynamicSinkUtils工具类（或类似工具）将外部修改操作转换为Calcite的RelNode
        DynamicSinkUtils.convertExternalToRel(createRelBuilder, input, externalModifyOperation)

      // legacy
      // 处理遗留的输出转换操作（如老版本的Flink Table API中的输出转换）
      case outputConversion: OutputConversionModifyOperation =>
        // 构建子查询的Calcite关系表达式
      val input = createRelBuilder.queryOperation(outputConversion.getChild).build()
        // 根据更新模式确定是否需要在写入前更新以及是否需要变更标志
        val (needUpdateBefore, withChangeFlag) = outputConversion.getUpdateMode match {
          case UpdateMode.RETRACT => (true, true) // 撤回模式，需要更新前处理和变更标志
          case UpdateMode.APPEND => (false, false)// 追加模式，两者都不需要
          case UpdateMode.UPSERT => (false, true)// 插入更新模式，不需要更新前处理但需要变更标志
        }
        // 将输出类型转换为遗留的TypeInfo
        val typeInfo = LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(outputConversion.getType)
        // 获取输入的逻辑类型
        val inputLogicalType = FlinkTypeFactory.toLogicalRowType(input.getRowType)
        // 推断Sink的物理模式
        val sinkPhysicalSchema =
          inferSinkPhysicalSchema(outputConversion.getType, inputLogicalType, withChangeFlag)
        // validate query schema and sink schema, and apply cast if possible
        // 验证查询的schema和Sink的schema，并在可能的情况下应用隐式类型转换
        val query = validateSchemaAndApplyImplicitCast(
          input,
          catalogManager.getSchemaResolver.resolve(sinkPhysicalSchema.toSchema),
          null,
          dataTypeFactory,
          getTypeFactory)
        // 创建一个DataStreamTableSink实例，用于将数据流（DataStream）转换为表（Table）的Sink
        val tableSink = new DataStreamTableSink(
          FlinkTypeFactory.toTableSchema(query.getRowType),
          typeInfo,
          needUpdateBefore,
          withChangeFlag)
        // 使用LogicalLegacySink.create方法创建一个逻辑上的遗留Sink
        LogicalLegacySink.create(
          query,
          tableSink,
          "DataStreamTableSink",
          ConnectorCatalogTable.sink(tableSink, !isStreamingMode))

      case _ => //抛出异常
        throw new TableException(s"Unsupported ModifyOperation: $modifyOperation")
    }
  }

  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 私有方法，仅在flink包内部可见，用于优化给定的RelNode
   */
  @VisibleForTesting
  private[flink] def optimize(relNodes: Seq[RelNode]): Seq[RelNode] = {
    // 调用获取到的优化器对输入的RelNode序列进行优化
    val optimizedRelNodes = getOptimizer.optimize(relNodes)
    // 确保优化后的RelNode序列与输入序列的大小相同，这是一种校验手段，用于确保优化过程没有意外地增减节点
    // 如果不满足，将抛出IllegalArgumentException异常
    require(optimizedRelNodes.size == relNodes.size)
    // 返回优化后的RelNode序列
    optimizedRelNodes
  }

  @VisibleForTesting
  private[flink] def optimize(relNode: RelNode): RelNode = {
    val optimizedRelNodes = getOptimizer.optimize(Seq(relNode))
    require(optimizedRelNodes.size == 1)
    optimizedRelNodes.head
  }

  /**
   * Converts [[FlinkPhysicalRel]] DAG to [[ExecNodeGraph]], tries to reuse duplicate sub-plans and
   * transforms the graph based on the given processors.
   */
  @VisibleForTesting
  private[flink] def translateToExecNodeGraph(
      optimizedRelNodes: Seq[RelNode],
      isCompiled: Boolean): ExecNodeGraph = {
    val nonPhysicalRel = optimizedRelNodes.filterNot(_.isInstanceOf[FlinkPhysicalRel])
    if (nonPhysicalRel.nonEmpty) {
      throw new TableException(
        "The expected optimized plan is FlinkPhysicalRel plan, " +
          s"actual plan is ${nonPhysicalRel.head.getClass.getSimpleName} plan.")
    }

    require(optimizedRelNodes.forall(_.isInstanceOf[FlinkPhysicalRel]))

    // convert FlinkPhysicalRel DAG to ExecNodeGraph
    val generator = new ExecNodeGraphGenerator()
    val execGraph =
      generator.generate(optimizedRelNodes.map(_.asInstanceOf[FlinkPhysicalRel]), isCompiled)

    // process the graph
    val context = new ProcessorContext(this)
    val processors = getExecNodeGraphProcessors
    processors.foldLeft(execGraph)((graph, processor) => processor.process(graph, context))
  }

  protected def getExecNodeGraphProcessors: Seq[ExecNodeGraphProcessor]

  /**
   * Translates an [[ExecNodeGraph]] into a [[Transformation]] DAG.
   *
   * @param execGraph
   *   The node graph to translate.
   * @return
   *   The [[Transformation]] DAG that corresponds to the node DAG.
   */
  @VisibleForTesting
  def translateToPlan(execGraph: ExecNodeGraph): util.List[Transformation[_]]

  def addExtraTransformation(transformation: Transformation[_]): Unit = {
    if (!extraTransformations.contains(transformation)) {
      extraTransformations.add(transformation)
    }
  }

  private def getTableSink(
      contextResolvedTable: ContextResolvedTable,
      dynamicOptions: JMap[String, String]): Option[(ResolvedCatalogTable, Any)] = {
    contextResolvedTable.getTable[CatalogBaseTable] match {
      case connectorTable: ConnectorCatalogTable[_, _] =>
        val resolvedTable = contextResolvedTable.getResolvedTable[ResolvedCatalogTable]
        toScala(connectorTable.getTableSink) match {
          case Some(sink) => Some(resolvedTable, sink)
          case None => None
        }

      case regularTable: CatalogTable =>
        val resolvedTable = contextResolvedTable.getResolvedTable[ResolvedCatalogTable]
        val tableToFind = if (dynamicOptions.nonEmpty) {
          resolvedTable.copy(FlinkHints.mergeTableOptions(dynamicOptions, resolvedTable.getOptions))
        } else {
          resolvedTable
        }
        val catalog = toScala(contextResolvedTable.getCatalog)
        val objectIdentifier = contextResolvedTable.getIdentifier
        val isTemporary = contextResolvedTable.isTemporary

        if (
          isStreamingMode && isManagedTable(catalog.orNull, resolvedTable) &&
          !executor.isCheckpointingEnabled
        ) {
          throw new TableException(
            s"You should enable the checkpointing for sinking to managed table " +
              s"'$contextResolvedTable', managed table relies on checkpoint to commit and " +
              s"the data is visible only after commit.")
        }

        if (
          !contextResolvedTable.isAnonymous &&
          TableFactoryUtil.isLegacyConnectorOptions(
            catalogManager.getCatalog(objectIdentifier.getCatalogName).orElse(null),
            tableConfig,
            isStreamingMode,
            objectIdentifier,
            resolvedTable,
            isTemporary
          )
        ) {
          val tableSink = TableFactoryUtil.findAndCreateTableSink(
            catalog.orNull,
            objectIdentifier,
            tableToFind,
            getTableConfig,
            isStreamingMode,
            isTemporary)
          Option(resolvedTable, tableSink)
        } else {
          val factoryFromCatalog = catalog.flatMap(f => toScala(f.getFactory)) match {
            case Some(f: DynamicTableSinkFactory) => Some(f)
            case _ => None
          }

          val factoryFromModule = toScala(
            plannerContext.getFlinkContext.getModuleManager
              .getFactory(toJava((m: Module) => m.getTableSinkFactory)))

          // Since the catalog is more specific, we give it precedence over a factory provided by
          // any modules.
          val factory = factoryFromCatalog.orElse(factoryFromModule).orNull

          val tableSink = FactoryUtil.createDynamicTableSink(
            factory,
            objectIdentifier,
            tableToFind,
            Collections.emptyMap(),
            getTableConfig,
            getFlinkContext.getClassLoader,
            isTemporary)
          Option(resolvedTable, tableSink)
        }

      case _ => None
    }
  }

  protected def createSerdeContext: SerdeContext = {
    val planner = createFlinkPlanner
    new SerdeContext(
      getParser,
      planner.config.getContext.asInstanceOf[FlinkContext],
      plannerContext.getTypeFactory,
      planner.operatorTable
    )
  }

  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * 转换前置操作
   */
  protected def beforeTranslation(): Unit = {
    // Add query start time to TableConfig, these config are used internally,
    // these configs will be used by temporal functions like CURRENT_TIMESTAMP,LOCALTIMESTAMP.
    // 在转换操作之前，记录查询的开始时间到TableConfig中，这些配置是内部使用的，
    // 并将被时间函数如CURRENT_TIMESTAMP, LOCALTIMESTAMP等使用。
    val epochTime: JLong = System.currentTimeMillis() // 获取当前时间的毫秒数作为查询开始时间
    tableConfig.set(TABLE_QUERY_START_EPOCH_TIME, epochTime)// 将查询开始时间（UTC时间）保存到TableConfig中
    // 计算并设置查询开始的本地时间（考虑到时区偏移）
    val localTime: JLong = epochTime +
      TimeZone.getTimeZone(TableConfigUtils.getLocalTimeZone(tableConfig)).getOffset(epochTime)
    tableConfig.set(TABLE_QUERY_START_LOCAL_TIME, localTime)

    // 获取当前数据库名称，如果没有则默认为空字符串，并保存到TableConfig中
    val currentDatabase = Option(catalogManager.getCurrentDatabase).getOrElse("")
    tableConfig.set(TABLE_QUERY_CURRENT_DATABASE, currentDatabase)

    // We pass only the configuration to avoid reconfiguration with the rootConfiguration
    // 仅传递配置以避免与rootConfiguration重新配置，这里将TableConfig的配置应用到执行环境中
    getExecEnv.configure(tableConfig.getConfiguration, classLoader)

    // Use config parallelism to override env parallelism.
    // 使用配置中的并行度来覆盖执行环境的默认并行度
    val defaultParallelism =
      getTableConfig.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM)
    if (defaultParallelism > 0) {
      // 如果TableConfig中配置了默认的并行度，则将其设置到执行环境的配置中
      getExecEnv.getConfig.setParallelism(defaultParallelism)
    }
  }

  protected def afterTranslation(): Unit = {
    // Cleanup all internal configuration after plan translation finished.
    val configuration = tableConfig.getConfiguration
    configuration.removeConfig(TABLE_QUERY_START_EPOCH_TIME)
    configuration.removeConfig(TABLE_QUERY_START_LOCAL_TIME)
    configuration.removeConfig(TABLE_QUERY_CURRENT_DATABASE)

    // Clean caches that might have filled up during optimization
    CompileUtils.cleanUp()
    extraTransformations.clear()
  }

  /** Returns all the graphs required to execute EXPLAIN */
  private[flink] def getExplainGraphs(operations: util.List[Operation])
      : (mutable.Buffer[RelNode], Seq[RelNode], ExecNodeGraph, StreamGraph) = {
    require(operations.nonEmpty, "operations should not be empty")
    beforeTranslation()
    val sinkRelNodes = operations.map {
      case queryOperation: QueryOperation =>
        val relNode = createRelBuilder.queryOperation(queryOperation).build()
        relNode match {
          // SQL: explain plan for insert into xx
          case modify: LogicalTableModify =>
            // convert LogicalTableModify to SinkModifyOperation
            val qualifiedName = modify.getTable.getQualifiedName
            require(qualifiedName.size() == 3, "the length of qualified name should be 3.")
            val objectIdentifier =
              ObjectIdentifier.of(qualifiedName.get(0), qualifiedName.get(1), qualifiedName.get(2))
            val contextResolvedTable = catalogManager.getTableOrError(objectIdentifier)
            val modifyOperation = new SinkModifyOperation(
              contextResolvedTable,
              new PlannerQueryOperation(
                modify.getInput,
                () => queryOperation.asSerializableString())
            )
            translateToRel(modifyOperation)
          case _ =>
            relNode
        }
      case modifyOperation: ModifyOperation =>
        translateToRel(modifyOperation)
      case o => throw new TableException(s"Unsupported operation: ${o.getClass.getCanonicalName}")
    }
    val optimizedRelNodes = optimize(sinkRelNodes)
    val execGraph = translateToExecNodeGraph(optimizedRelNodes, isCompiled = false)
    val transformations = translateToPlan(execGraph)
    afterTranslation()

    // We pass only the configuration to avoid reconfiguration with the rootConfiguration
    val streamGraph = executor
      .createPipeline(transformations, tableConfig.getConfiguration, null)
      .asInstanceOf[StreamGraph]

    (sinkRelNodes, optimizedRelNodes, execGraph, streamGraph)
  }
}
