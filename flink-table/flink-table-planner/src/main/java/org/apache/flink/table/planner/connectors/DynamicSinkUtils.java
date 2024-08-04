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

package org.apache.flink.table.planner.connectors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.Column.MetadataColumn;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.operations.CollectModifyOperation;
import org.apache.flink.table.operations.ExternalModifyOperation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.plan.abilities.sink.OverwriteSpec;
import org.apache.flink.table.planner.plan.abilities.sink.RowLevelDeleteSpec;
import org.apache.flink.table.planner.plan.abilities.sink.RowLevelUpdateSpec;
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;
import org.apache.flink.table.planner.plan.abilities.sink.WritingMetadataSpec;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalSink;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.utils.RowLevelModificationContextUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeTransformations;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapContext;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;
import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsAvoidingCast;
import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsExplicitCast;
import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsImplicitCast;

/** Utilities for dealing with {@link DynamicTableSink}. */
@Internal
public final class DynamicSinkUtils {

    /** Converts an {@link TableResult#collect()} sink to a {@link RelNode}. */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 将一个 {@link TableResult#collect()} 接收器（即收集操作）转换为 {@link RelNode}。
     * 用于将Flink Table API中的collect操作转换成Calcite逻辑计划中的一个节点。
     */
    public static RelNode convertCollectToRel(
            FlinkRelBuilder relBuilder,
            RelNode input,
            CollectModifyOperation collectModifyOperation,
            ReadableConfig configuration,
            ClassLoader classLoader) {
        // 从关系表达式构建器中提取上下文，然后获取数据类型工厂
        final DataTypeFactory dataTypeFactory =
                unwrapContext(relBuilder).getCatalogManager().getDataTypeFactory();
        // 获取收集操作子查询的解析后的模式（即列名和列的数据类型）
        final ResolvedSchema childSchema = collectModifyOperation.getChild().getResolvedSchema();
        // 创建一个物理模式，直接使用子查询的列名和列数据类型
        final ResolvedSchema schema =
                ResolvedSchema.physical(
                        childSchema.getColumnNames(), childSchema.getColumnDataTypes());
        // 创建一个外部目录表，并将其与解析后的模式关联起来，形成一个解析后的目录表
        final ResolvedCatalogTable catalogTable =
                new ResolvedCatalogTable(
                        new ExternalCatalogTable(
                                Schema.newBuilder().fromResolvedSchema(schema).build()),
                        schema);
        // 创建一个匿名上下文解析表，这个表代表了collect操作的结果
        final ContextResolvedTable contextResolvedTable =
                ContextResolvedTable.anonymous("collect", catalogTable);
        // 根据数据类型工厂和解析后的模式，修正数据类型，以适配collect操作
        final DataType consumedDataType = fixCollectDataType(dataTypeFactory, schema);

        // 从配置中获取时区信息，并转换为ZoneId对象
        final String zone = configuration.get(TableConfigOptions.LOCAL_TIME_ZONE);
        final ZoneId zoneId =
                TableConfigOptions.LOCAL_TIME_ZONE.defaultValue().equals(zone)
                        ? ZoneId.systemDefault()
                        : ZoneId.of(zone);
         // 根据配置和上下文信息，创建一个CollectDynamicSink实例
        // 这个Sink用于处理collect操作的结果
        final CollectDynamicSink tableSink =
                new CollectDynamicSink(
                        contextResolvedTable.getIdentifier(),
                        consumedDataType,
                        configuration.get(CollectSinkOperatorFactory.MAX_BATCH_SIZE),
                        configuration.get(CollectSinkOperatorFactory.SOCKET_TIMEOUT),
                        classLoader,
                        zoneId,
                        configuration
                                .get(ExecutionConfigOptions.TABLE_EXEC_LEGACY_CAST_BEHAVIOUR)
                                .isEnabled(),
                        configuration);
        // 设置collectModifyOperation的SelectResultProvider，这样它就可以提供查询结果
        collectModifyOperation.setSelectResultProvider(tableSink.getSelectResultProvider());
        // 设置consumedDataType，这是Sink期望的数据类型
        collectModifyOperation.setConsumedDataType(consumedDataType);
        // 将Sink转换为RelNode，并添加到逻辑计划中
        return convertSinkToRel(
                relBuilder,
                input,
                Collections.emptyMap(), // dynamicOptions
                contextResolvedTable,
                Collections.emptyMap(), // staticPartitions
                null, // targetColumns
                false,
                tableSink);
    }

    /**
     * Converts an external sink (i.e. further {@link DataStream} transformations) to a {@link
     * RelNode}.
     */
    public static RelNode convertExternalToRel(
            FlinkRelBuilder relBuilder,
            RelNode input,
            ExternalModifyOperation externalModifyOperation) {
        final DynamicTableSink tableSink =
                new ExternalDynamicSink(
                        externalModifyOperation.getChangelogMode().orElse(null),
                        externalModifyOperation.getPhysicalDataType());
        return convertSinkToRel(
                relBuilder,
                input,
                Collections.emptyMap(),
                externalModifyOperation.getContextResolvedTable(),
                Collections.emptyMap(),
                null, // targetColumns
                false,
                tableSink);
    }

    /**
     * Converts a given {@link DynamicTableSink} to a {@link RelNode}. It adds helper projections if
     * necessary.
     */
    public static RelNode convertSinkToRel(
            FlinkRelBuilder relBuilder,
            RelNode input,
            SinkModifyOperation sinkModifyOperation,
            DynamicTableSink sink) {
        return convertSinkToRel(
                relBuilder,
                input,
                sinkModifyOperation.getDynamicOptions(),
                sinkModifyOperation.getContextResolvedTable(),
                sinkModifyOperation.getStaticPartitions(),
                sinkModifyOperation.getTargetColumns(),
                sinkModifyOperation.isOverwrite(),
                sink);
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 将Sink转换为RelNode
     */
    private static RelNode convertSinkToRel(
            FlinkRelBuilder relBuilder,
            RelNode input,
            Map<String, String> dynamicOptions,
            ContextResolvedTable contextResolvedTable,
            Map<String, String> staticPartitions,
            int[][] targetColumns,
            boolean isOverwrite,
            DynamicTableSink sink) {
        // 获取数据类型工厂，用于构建数据类型
        final DataTypeFactory dataTypeFactory =
                unwrapContext(relBuilder).getCatalogManager().getDataTypeFactory();
        // 获取Flink的类型工厂，用于构建Flink特定的数据类型
        final FlinkTypeFactory typeFactory = unwrapTypeFactory(relBuilder);
        // 获取已解析的表结构
        final ResolvedSchema schema = contextResolvedTable.getResolvedSchema();
        // 获取表的调试名称，便于日志和调试
        final String tableDebugName = contextResolvedTable.getIdentifier().asSummaryString();
        // 初始化Sink能力规范列表，用于记录Sink支持的操作类型
        List<SinkAbilitySpec> sinkAbilitySpecs = new ArrayList<>();
        // 判断输入是否是表修改操作，并确定操作类型是删除还是更新
        boolean isDelete = false;
        boolean isUpdate = false;
        if (input instanceof LogicalTableModify) {
            LogicalTableModify tableModify = (LogicalTableModify) input;
            isDelete = tableModify.getOperation() == TableModify.Operation.DELETE;
            isUpdate = tableModify.getOperation() == TableModify.Operation.UPDATE;
        }

        // 1. prepare table sink
        // 1. 准备动态Sink
        // 此步骤可能包括根据传入的参数（如动态选项、静态分区、是否覆盖等）
        // 对Sink进行必要的配置或初始化
        prepareDynamicSink(
                tableDebugName,
                staticPartitions,
                isOverwrite,
                sink,
                contextResolvedTable.getResolvedTable(),
                sinkAbilitySpecs);

        // rewrite rel node for delete
        // 根据操作类型重写RelNode
       // 如果是删除操作
        if (isDelete) {

            input =
                    convertDelete(
                            (LogicalTableModify) input,
                            sink,
                            contextResolvedTable,
                            tableDebugName,
                            dataTypeFactory,
                            typeFactory,
                            sinkAbilitySpecs);
        } else if (isUpdate) {    // 如果是更新操作
            input =
                    convertUpdate(
                            (LogicalTableModify) input,
                            sink,
                            contextResolvedTable,
                            tableDebugName,
                            dataTypeFactory,
                            typeFactory,
                            sinkAbilitySpecs);
        }
        // 应用Sink能力规范列表中的每一项到Sink上
        sinkAbilitySpecs.forEach(spec -> spec.apply(sink));

        // 2. validate the query schema to the sink's table schema and apply cast if possible
        // 验证查询模式与Sink的表模式，并在可能的情况下应用隐式转换
        RelNode query = input;
        // skip validate and implicit cast when it's delete/update since it has been done before
        // 如果是删除或更新操作，则跳过验证和隐式转换，因为这些在之前已经处理过了
        if (!isDelete && !isUpdate) {
            query =
                    validateSchemaAndApplyImplicitCast(
                            input, schema, tableDebugName, dataTypeFactory, typeFactory);
        }
        // 将处理后的查询（或原始输入，如果未进行验证和转换）推送到Flink关系表达式构建器中
        relBuilder.push(query);

        // 3. convert the sink's table schema to the consumed data type of the sink
        // 将Sink的表模式转换为Sink所消费的数据类型
        final List<Integer> metadataColumns = extractPersistedMetadataColumns(schema);
        // 如果存在需要持久化的元数据列，则推送一个元数据投影
        if (!metadataColumns.isEmpty()) {
            pushMetadataProjection(relBuilder, typeFactory, schema, sink);
        }

        // 准备动态选项作为查询提示
        List<RelHint> hints = new ArrayList<>();
        if (!dynamicOptions.isEmpty()) {
            // 将动态选项作为查询提示添加到列表中
            hints.add(RelHint.builder("OPTIONS").hintOptions(dynamicOptions).build());
        }
        // 使用Flink关系表达式构建器构建最终的查询
        final RelNode finalQuery = relBuilder.build();
        // 创建一个LogicalSink节点，该节点是查询计划中的一部分，表示数据输出的目标
        return LogicalSink.create(
                finalQuery, // finalQuery: 经过所有转换和验证后的最终查询节点
                hints, // hints: 查询执行时的提示列表，可能包含动态选项等
                contextResolvedTable,// contextResolvedTable: 已解析的表上下文，包含表的结构和元数据等信息
                sink,// sink: Sink节点，表示数据输出的目标
                staticPartitions,// staticPartitions: 静态分区信息，用于指定数据输出的分区策略
                targetColumns,// targetColumns: 目标列列表，指定哪些列的数据将被输出
                sinkAbilitySpecs.toArray(new SinkAbilitySpec[0]));// sinkAbilitySpecs: Sink的能力规范列表，转换为数组形式
    }

    /** Checks if the given query can be written into the given sink's table schema. */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 检查给定的查询是否可以写入到给定sink的表结构中，并应用隐式类型转换（如果需要）。
     *
     * @param query            待验证的查询，以RelNode形式表示。
     * @param sinkSchema       目标sink的解析后的表结构。
     * @param tableDebugName   目标表的调试名称，用于日志和错误消息中以便于调试。
     * @param dataTypeFactory  数据类型工厂，用于创建和操作数据类型。
     * @param typeFactory      Flink类型工厂，用于Flink特定的类型转换和表示。
     */
    public static RelNode validateSchemaAndApplyImplicitCast(
            RelNode query,
            ResolvedSchema sinkSchema,
            String tableDebugName,
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory) {
        // 首先，将sink的表结构（ResolvedSchema）转换为Flink的RowType，
        final RowType sinkType =
                (RowType)
                        fixSinkDataType(dataTypeFactory, sinkSchema.toSinkRowDataType())
                                .getLogicalType();
        // 然后，使用修正后的sinkType、查询query、调试名称和Flink类型工厂来验证schema并应用隐式类型转换
        return validateSchemaAndApplyImplicitCast(query, sinkType, tableDebugName, typeFactory);
    }

    /** Checks if the given query can be written into the given target types. */
    public static RelNode validateSchemaAndApplyImplicitCast(
            RelNode query,
            List<DataType> targetTypes,
            String tableDebugName,
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory) {
        final RowType sinkType =
                (RowType)
                        fixSinkDataType(
                                        dataTypeFactory,
                                        DataTypes.ROW(targetTypes.toArray(new DataType[0])))
                                .getLogicalType();
        return validateSchemaAndApplyImplicitCast(query, sinkType, tableDebugName, typeFactory);
    }

    /**
     * Checks if the given query can be written into the given sink type.
     *
     * <p>It checks whether field types are compatible (types should be equal including precisions).
     * If types are not compatible, but can be implicitly cast, a cast projection will be applied.
     * Otherwise, an exception will be thrown.
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 检查给定的查询是否可以写入给定的sink类型。
     */
    private static RelNode validateSchemaAndApplyImplicitCast(
            RelNode query, RowType sinkType, String tableDebugName, FlinkTypeFactory typeFactory) {
        // 将查询的Row类型转换为逻辑Row类型
        final RowType queryType = FlinkTypeFactory.toLogicalRowType(query.getRowType());
        // 获取查询的字段列表
        final List<RowField> queryFields = queryType.getFields();
        // 获取sink的字段列表
        final List<RowField> sinkFields = sinkType.getFields();
        // 检查查询和sink的字段数量是否相等
        if (queryFields.size() != sinkFields.size()) {
            // 如果不相等，抛出架构不匹配异常
            throw createSchemaMismatchException(
                    "Different number of columns.", tableDebugName, queryFields, sinkFields);
        }

        boolean requiresCasting = false;
        for (int i = 0; i < sinkFields.size(); i++) {
            // 获取查询和sink中对应位置的字段类型
            final LogicalType queryColumnType = queryFields.get(i).getType();
            final LogicalType sinkColumnType = sinkFields.get(i).getType();
            // 检查是否可以隐式转换
            if (!supportsImplicitCast(queryColumnType, sinkColumnType)) {
                // 如果不能隐式转换，抛出架构不匹配异常
                throw createSchemaMismatchException(
                        String.format(
                                "Incompatible types for sink column '%s' at position %s.",
                                sinkFields.get(i).getName(), i),
                        tableDebugName,
                        queryFields,
                        sinkFields);
            }
            // 检查是否支持避免转换（即是否严格相等）
            if (!supportsAvoidingCast(queryColumnType, sinkColumnType)) {
                // 如果不支持避免转换（即需要显式转换），则标记需要转换
                requiresCasting = true;
            }
        }
        // 如果需要转换，则应用转换投影
        if (requiresCasting) {
            // 使用类型工厂根据sink类型构建RelDataType
            final RelDataType castRelDataType = typeFactory.buildRelNodeRowType(sinkType);
            // 创建转换关系的节点
            return RelOptUtil.createCastRel(query, castRelDataType, true);
        }
        // 如果不需要转换，则直接返回原始查询
        return query;
    }

    private static RelNode convertDelete(
            LogicalTableModify tableModify,
            DynamicTableSink sink,
            ContextResolvedTable contextResolvedTable,
            String tableDebugName,
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory,
            List<SinkAbilitySpec> sinkAbilitySpecs) {
        if (!(sink instanceof SupportsRowLevelDelete)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Can't perform delete operation of the table %s because the corresponding dynamic table sink has not yet implemented %s.",
                            tableDebugName, SupportsRowLevelDelete.class.getName()));
        }

        // get the row-level delete info
        SupportsRowLevelDelete supportsRowLevelDelete = (SupportsRowLevelDelete) sink;
        RowLevelModificationScanContext context = RowLevelModificationContextUtils.getScanContext();
        SupportsRowLevelDelete.RowLevelDeleteInfo rowLevelDeleteInfo =
                supportsRowLevelDelete.applyRowLevelDelete(context);

        if (rowLevelDeleteInfo.getRowLevelDeleteMode()
                        != SupportsRowLevelDelete.RowLevelDeleteMode.DELETED_ROWS
                && rowLevelDeleteInfo.getRowLevelDeleteMode()
                        != SupportsRowLevelDelete.RowLevelDeleteMode.REMAINING_ROWS) {
            throw new TableException(
                    "Unknown delete mode: " + rowLevelDeleteInfo.getRowLevelDeleteMode());
        }

        if (rowLevelDeleteInfo.getRowLevelDeleteMode()
                == SupportsRowLevelDelete.RowLevelDeleteMode.REMAINING_ROWS) {
            // if it's for remaining row, convert the predicate in where clause
            // to the negative predicate
            convertPredicateToNegative(tableModify);
        }

        // convert the LogicalTableModify node to a RelNode representing row-level delete
        Tuple2<RelNode, int[]> deleteRelNodeAndRequireIndices =
                convertToRowLevelDelete(
                        tableModify,
                        contextResolvedTable,
                        rowLevelDeleteInfo,
                        tableDebugName,
                        dataTypeFactory,
                        typeFactory);
        sinkAbilitySpecs.add(
                new RowLevelDeleteSpec(
                        rowLevelDeleteInfo.getRowLevelDeleteMode(),
                        context,
                        deleteRelNodeAndRequireIndices.f1));
        return deleteRelNodeAndRequireIndices.f0;
    }

    private static RelNode convertUpdate(
            LogicalTableModify tableModify,
            DynamicTableSink sink,
            ContextResolvedTable contextResolvedTable,
            String tableDebugName,
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory,
            List<SinkAbilitySpec> sinkAbilitySpecs) {
        if (!(sink instanceof SupportsRowLevelUpdate)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Can't perform update operation of the table %s because the corresponding dynamic table sink has not yet implemented %s.",
                            tableDebugName, SupportsRowLevelUpdate.class.getName()));
        }
        SupportsRowLevelUpdate supportsRowLevelUpdate = (SupportsRowLevelUpdate) sink;
        ResolvedSchema resolvedSchema = contextResolvedTable.getResolvedSchema();
        List<Column> updatedColumns = getUpdatedColumns(tableModify, resolvedSchema);
        RowLevelModificationScanContext context = RowLevelModificationContextUtils.getScanContext();
        SupportsRowLevelUpdate.RowLevelUpdateInfo updateInfo =
                supportsRowLevelUpdate.applyRowLevelUpdate(updatedColumns, context);
        if (updateInfo.getRowLevelUpdateMode()
                        != SupportsRowLevelUpdate.RowLevelUpdateMode.UPDATED_ROWS
                && updateInfo.getRowLevelUpdateMode()
                        != SupportsRowLevelUpdate.RowLevelUpdateMode.ALL_ROWS) {
            throw new IllegalArgumentException(
                    "Unknown update mode:" + updateInfo.getRowLevelUpdateMode());
        }
        Tuple2<RelNode, int[]> updateRelNodeAndRequireIndices =
                convertToRowLevelUpdate(
                        tableModify,
                        contextResolvedTable,
                        updateInfo,
                        tableDebugName,
                        dataTypeFactory,
                        typeFactory);
        sinkAbilitySpecs.add(
                new RowLevelUpdateSpec(
                        updatedColumns,
                        updateInfo.getRowLevelUpdateMode(),
                        context,
                        updateRelNodeAndRequireIndices.f1));
        return updateRelNodeAndRequireIndices.f0;
    }

    private static List<Column> getUpdatedColumns(
            LogicalTableModify tableModify, ResolvedSchema resolvedSchema) {
        List<Column> updatedColumns = new ArrayList<>();
        List<String> updatedColumnNames = tableModify.getUpdateColumnList();
        for (Column column : resolvedSchema.getColumns()) {
            if (updatedColumnNames.contains(column.getName())) {
                updatedColumns.add(column);
            }
        }
        return updatedColumns;
    }

    /**
     * Convert tableModify node to a RelNode representing for row-level delete.
     *
     * @return a tuple contains the RelNode and the index for the required physical columns for
     *     row-level delete.
     */
    private static Tuple2<RelNode, int[]> convertToRowLevelDelete(
            LogicalTableModify tableModify,
            ContextResolvedTable contextResolvedTable,
            SupportsRowLevelDelete.RowLevelDeleteInfo rowLevelDeleteInfo,
            String tableDebugName,
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory) {
        // get the required columns
        ResolvedSchema resolvedSchema = contextResolvedTable.getResolvedSchema();
        Optional<List<Column>> optionalColumns = rowLevelDeleteInfo.requiredColumns();
        List<Column> requiredColumns = optionalColumns.orElse(resolvedSchema.getColumns());
        // get the root table scan which we may need rewrite it
        LogicalTableScan tableScan = getSourceTableScan(tableModify);
        // get the index for the required columns and extra meta cols if necessary
        Tuple2<List<Integer>, List<MetadataColumn>> colsIndexAndExtraMetaCols =
                getRequireColumnsIndexAndExtraMetaCols(tableScan, requiredColumns, resolvedSchema);
        List<Integer> colIndexes = colsIndexAndExtraMetaCols.f0;
        List<MetadataColumn> metadataColumns = colsIndexAndExtraMetaCols.f1;
        // if meta columns size is greater than 0, we need to modify the underlying
        // LogicalTableScan to make it can read meta column
        if (metadataColumns.size() > 0) {
            resolvedSchema =
                    addExtraMetaCols(
                            tableModify, tableScan, tableDebugName, metadataColumns, typeFactory);
        }

        // create a project only select the required columns for delete
        return Tuple2.of(
                projectColumnsForDelete(
                        tableModify,
                        resolvedSchema,
                        colIndexes,
                        tableDebugName,
                        dataTypeFactory,
                        typeFactory),
                getPhysicalColumnIndices(colIndexes, resolvedSchema));
    }

    /** Return the indices from {@param colIndexes} that belong to physical column. */
    private static int[] getPhysicalColumnIndices(List<Integer> colIndexes, ResolvedSchema schema) {
        return colIndexes.stream()
                .filter(i -> schema.getColumns().get(i).isPhysical())
                .mapToInt(i -> i)
                .toArray();
    }

    /** Convert the predicate in WHERE clause to the negative predicate. */
    private static void convertPredicateToNegative(LogicalTableModify tableModify) {
        RexBuilder rexBuilder = tableModify.getCluster().getRexBuilder();
        RelNode input = tableModify.getInput();
        LogicalFilter newFilter;
        // if the input is a table scan, there's no predicate which means it's always true
        // the negative predicate should be false
        if (input.getInput(0) instanceof LogicalTableScan) {
            newFilter = LogicalFilter.create(input.getInput(0), rexBuilder.makeLiteral(false));
        } else {
            LogicalFilter filter = (LogicalFilter) input.getInput(0);
            // create a filter with negative predicate
            RexNode complementFilter =
                    rexBuilder.makeCall(
                            filter.getCondition().getType(),
                            FlinkSqlOperatorTable.NOT,
                            Collections.singletonList(filter.getCondition()));
            newFilter = filter.copy(filter.getTraitSet(), filter.getInput(), complementFilter);
        }
        // replace with the new filter
        input.replaceInput(0, newFilter);
    }

    /** Get the index for the required columns and extra meta cols if necessary. */
    private static Tuple2<List<Integer>, List<MetadataColumn>>
            getRequireColumnsIndexAndExtraMetaCols(
                    LogicalTableScan tableScan,
                    List<Column> requiredColumns,
                    ResolvedSchema resolvedSchema) {
        // index list for the required columns
        List<Integer> columnIndexList = new ArrayList<>();
        // extra meta cols
        List<MetadataColumn> extraMetadataColumns = new ArrayList<>();
        List<String> fieldNames = resolvedSchema.getColumnNames();
        final TableSourceTable sourceTable = tableScan.getTable().unwrap(TableSourceTable.class);
        DynamicTableSource dynamicTableSource = sourceTable.tableSource();
        int additionCols = 0;
        // iterate for each required column
        for (Column column : requiredColumns) {
            int index = fieldNames.indexOf(column.getName());
            // if we can't find the column, we may need to add extra column
            if (index <= -1) {
                // we only consider add metadata column
                if (column instanceof Column.MetadataColumn) {
                    // need to add meta column
                    columnIndexList.add(fieldNames.size() + additionCols);
                    if (!(dynamicTableSource instanceof SupportsReadingMetadata)) {
                        throw new UnsupportedOperationException(
                                String.format(
                                        "The table source don't support reading metadata, but the require columns contains the meta columns: %s.",
                                        column));
                    }
                    // list what metas the source supports to read
                    SupportsReadingMetadata supportsReadingMetadata =
                            (SupportsReadingMetadata) dynamicTableSource;
                    Map<String, DataType> readableMetadata =
                            supportsReadingMetadata.listReadableMetadata();
                    // check the source can read the meta column
                    String metaCol =
                            ((MetadataColumn) column).getMetadataKey().orElse(column.getName());
                    if (!readableMetadata.containsKey(metaCol)) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Expect to read the meta column %s, but the table source for table %s doesn't support read the metadata column."
                                                + "Please make sure the readable metadata for the source contains %s.",
                                        column,
                                        UnresolvedIdentifier.of(
                                                tableScan.getTable().getQualifiedName()),
                                        metaCol));
                    }
                    // mark it as extra col
                    additionCols += 1;
                    DataType dataType = readableMetadata.get(metaCol);
                    if (!dataType.equals(column.getDataType())) {
                        throw new IllegalArgumentException(
                                String.format(
                                        "Un-matched data type: the required column %s has datatype %s, but the data type in readable metadata for the table %s has data type %s. ",
                                        column,
                                        column.getDataType(),
                                        UnresolvedIdentifier.of(
                                                tableScan.getTable().getQualifiedName()),
                                        dataType));
                    }
                    extraMetadataColumns.add((MetadataColumn) column);
                } else {
                    throw new IllegalArgumentException("Unknown required column " + column);
                }
            } else {
                columnIndexList.add(index);
            }
        }
        return Tuple2.of(columnIndexList, extraMetadataColumns);
    }

    private static LogicalTableScan getSourceTableScan(RelNode relNode) {
        while (!(relNode instanceof LogicalTableScan)) {
            relNode = relNode.getInput(0);
        }
        return (LogicalTableScan) relNode;
    }

    /**
     * Convert tableModify node to a RelNode representing for row-level update.
     *
     * @return a tuple contains the RelNode and the index for the required physical columns for
     *     row-level update.
     */
    private static Tuple2<RelNode, int[]> convertToRowLevelUpdate(
            LogicalTableModify tableModify,
            ContextResolvedTable contextResolvedTable,
            SupportsRowLevelUpdate.RowLevelUpdateInfo rowLevelUpdateInfo,
            String tableDebugName,
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory) {
        // get the required columns
        ResolvedSchema resolvedSchema = contextResolvedTable.getResolvedSchema();
        Optional<List<Column>> optionalColumns = rowLevelUpdateInfo.requiredColumns();
        List<Column> requiredColumns = optionalColumns.orElse(resolvedSchema.getColumns());
        // get the root table scan which we may need rewrite it
        LogicalTableScan tableScan = getSourceTableScan(tableModify);
        Tuple2<List<Integer>, List<MetadataColumn>> colsIndexAndExtraMetaCols =
                getRequireColumnsIndexAndExtraMetaCols(tableScan, requiredColumns, resolvedSchema);
        List<Integer> colIndexes = colsIndexAndExtraMetaCols.f0;
        List<MetadataColumn> metadataColumns = colsIndexAndExtraMetaCols.f1;
        // if meta columns size is greater than 0, we need to modify the underlying
        // LogicalTableScan to make it can read meta column
        int originColsCount = resolvedSchema.getColumnCount();
        if (metadataColumns.size() > 0) {
            resolvedSchema =
                    addExtraMetaCols(
                            tableModify, tableScan, tableDebugName, metadataColumns, typeFactory);
        }
        return Tuple2.of(
                projectColumnsForUpdate(
                        tableModify,
                        originColsCount,
                        resolvedSchema,
                        colIndexes,
                        rowLevelUpdateInfo.getRowLevelUpdateMode(),
                        tableDebugName,
                        dataTypeFactory,
                        typeFactory),
                getPhysicalColumnIndices(colIndexes, resolvedSchema));
    }

    // create a project only select the required column or expression for update
    private static RelNode projectColumnsForUpdate(
            LogicalTableModify tableModify,
            int originColsCount,
            ResolvedSchema resolvedSchema,
            List<Integer> updatedIndexes,
            SupportsRowLevelUpdate.RowLevelUpdateMode updateMode,
            String tableDebugName,
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory) {
        RexBuilder rexBuilder = tableModify.getCluster().getRexBuilder();
        // the updated columns, whose order is same to user's update clause
        List<String> updatedColumnNames = tableModify.getUpdateColumnList();
        List<RexNode> newRexNodeList = new ArrayList<>();
        List<String> newFieldNames = new ArrayList<>();
        List<DataType> updateTargetDataTypes = new ArrayList<>();
        Project project = (Project) (tableModify.getInput());

        LogicalFilter filter = null;
        // if the update mode is all rows, we need to know the filter to rewrite
        // the update expression to IF(filter, updated_expr, col_expr)
        if (updateMode == SupportsRowLevelUpdate.RowLevelUpdateMode.ALL_ROWS
                && project.getInput() instanceof LogicalFilter) {
            filter = (LogicalFilter) project.getInput();
        }

        // the rex nodes for the project are like: index for all col, update expressions for the
        // updated columns
        List<RexNode> oldRexNodes = project.getProjects();
        for (int index : updatedIndexes) {
            String colName = resolvedSchema.getColumnNames().get(index);
            // if the updated cols contain the col to be selected, the updated expression should
            // be in the project node
            if (updatedColumnNames.contains(colName)) {
                // get the index of the updated column in all updated columns
                int i = updatedColumnNames.indexOf(colName);
                // get the update expression
                RexNode rexNode = oldRexNodes.get(originColsCount + i);
                if (filter != null) {
                    rexNode =
                            rexBuilder.makeCall(
                                    FlinkSqlOperatorTable.IF,
                                    Arrays.asList(
                                            filter.getCondition(),
                                            rexNode,
                                            rexBuilder.makeInputRef(project.getInput(), index)));
                }
                newRexNodeList.add(rexNode);
            } else {
                newRexNodeList.add(rexBuilder.makeInputRef(project.getInput(), index));
            }
            newFieldNames.add(colName);
            updateTargetDataTypes.add(resolvedSchema.getColumnDataTypes().get(index));
        }

        project =
                project.copy(
                        project.getTraitSet(),
                        // if filter is not null, we need to remove the filter in the plan since we
                        // have rewritten the expression to IF(filter, updated_expr, col_expr)
                        filter != null ? filter.getInput() : project.getInput(),
                        newRexNodeList,
                        RexUtil.createStructType(typeFactory, newRexNodeList, newFieldNames, null));
        return validateSchemaAndApplyImplicitCast(
                project, updateTargetDataTypes, tableDebugName, dataTypeFactory, typeFactory);
    }

    /**
     * Add extra meta columns for underlying table scan, return a new resolve schema after adding
     * extra meta columns.
     */
    private static ResolvedSchema addExtraMetaCols(
            LogicalTableModify tableModify,
            LogicalTableScan tableScan,
            String tableDebugName,
            List<MetadataColumn> metadataColumns,
            FlinkTypeFactory typeFactory) {
        final TableSourceTable sourceTable = tableScan.getTable().unwrap(TableSourceTable.class);
        DynamicTableSource dynamicTableSource = sourceTable.tableSource();
        // get old schema and new schema after add some cols
        ResolvedSchema oldSchema = sourceTable.contextResolvedTable().getResolvedSchema();
        List<Column> newColumns = new ArrayList<>(oldSchema.getColumns());
        newColumns.addAll(metadataColumns);
        // get the new resolved schema after adding extra meta columns
        ResolvedSchema resolvedSchema = ResolvedSchema.of(newColumns);

        List<RelDataTypeField> oldFields = sourceTable.getRowType().getFieldList();
        List<RelDataTypeField> newFields = new ArrayList<>(sourceTable.getRowType().getFieldList());
        for (int i = 0; i < metadataColumns.size(); i++) {
            MetadataColumn column = metadataColumns.get(i);
            // add a new field
            newFields.add(
                    new RelDataTypeFieldImpl(
                            column.getName(),
                            oldFields.size() + i,
                            typeFactory.createFieldTypeFromLogicalType(
                                    column.getDataType().getLogicalType())));
        }
        // create a copy for TableSourceTable with new resolved schema
        TableSourceTable newTableSourceTab =
                sourceTable.copy(
                        dynamicTableSource,
                        sourceTable.contextResolvedTable().copy(resolvedSchema),
                        new RelRecordType(StructKind.FULLY_QUALIFIED, newFields, false),
                        sourceTable.abilitySpecs());

        // create a copy for table scan with new TableSourceTable
        LogicalTableScan newTableScan =
                new LogicalTableScan(
                        tableScan.getCluster(),
                        tableScan.getTraitSet(),
                        tableScan.getHints(),
                        newTableSourceTab);
        Project project = (Project) tableModify.getInput();
        // replace with the new table scan
        if (project.getInput() instanceof LogicalFilter) {
            LogicalFilter logicalFilter = (LogicalFilter) project.getInput();
            project.replaceInput(
                    0,
                    logicalFilter.copy(
                            logicalFilter.getTraitSet(),
                            newTableScan,
                            logicalFilter.getCondition()));
        } else {
            project.replaceInput(0, newTableScan);
        }
        // validate and apply metadata
        // TODO FLINK-33083 we should not ignore the produced abilities but actually put those into
        //  the table scan
        DynamicSourceUtils.validateAndApplyMetadata(
                tableDebugName, resolvedSchema, newTableSourceTab.tableSource(), new ArrayList<>());
        return resolvedSchema;
    }

    private static RelNode projectColumnsForDelete(
            LogicalTableModify tableModify,
            ResolvedSchema resolvedSchema,
            List<Integer> colIndexes,
            String tableDebugName,
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory) {
        // now we know which columns we may need
        List<RexNode> newRexNodeList = new ArrayList<>();
        List<String> newFieldNames = new ArrayList<>();
        List<DataType> deleteTargetDataTypes = new ArrayList<>();
        Project project = (Project) (tableModify.getInput());
        RexBuilder rexBuilder = tableModify.getCluster().getRexBuilder();
        // iterate each index for the column, create an input ref node for it.
        for (int index : colIndexes) {
            newRexNodeList.add(rexBuilder.makeInputRef(project.getInput(), index));
            newFieldNames.add(resolvedSchema.getColumnNames().get(index));
            deleteTargetDataTypes.add(resolvedSchema.getColumnDataTypes().get(index));
        }
        // a project to only get specific columns
        project =
                project.copy(
                        project.getTraitSet(),
                        project.getInput(),
                        newRexNodeList,
                        RexUtil.createStructType(typeFactory, newRexNodeList, newFieldNames, null));
        return validateSchemaAndApplyImplicitCast(
                project, deleteTargetDataTypes, tableDebugName, dataTypeFactory, typeFactory);
    }

    // --------------------------------------------------------------------------------------------

    /** Temporary solution until we drop legacy types. */
    private static DataType fixCollectDataType(
            DataTypeFactory dataTypeFactory, ResolvedSchema schema) {
        final DataType fixedDataType =
                DataTypeUtils.transform(
                        dataTypeFactory,
                        schema.toSourceRowDataType(),
                        TypeTransformations.legacyRawToTypeInfoRaw(),
                        TypeTransformations.legacyToNonLegacy());
        // TODO erase the conversion class earlier when dropping legacy code, esp. FLINK-22321
        return TypeConversions.fromLogicalToDataType(fixedDataType.getLogicalType());
    }

    /**
     * Creates a projection that reorders physical and metadata columns according to the consumed
     * data type of the sink. It casts metadata columns into the expected data type.
     *
     * @see SupportsWritingMetadata
     */
    private static void pushMetadataProjection(
            FlinkRelBuilder relBuilder,
            FlinkTypeFactory typeFactory,
            ResolvedSchema schema,
            DynamicTableSink sink) {
        final RexBuilder rexBuilder = relBuilder.getRexBuilder();
        final List<Column> columns = schema.getColumns();

        final List<Integer> physicalColumns = extractPhysicalColumns(schema);

        final Map<String, Integer> keyToMetadataColumn =
                extractPersistedMetadataColumns(schema).stream()
                        .collect(
                                Collectors.toMap(
                                        pos -> {
                                            final MetadataColumn metadataColumn =
                                                    (MetadataColumn) columns.get(pos);
                                            return metadataColumn
                                                    .getMetadataKey()
                                                    .orElse(metadataColumn.getName());
                                        },
                                        Function.identity()));

        final List<Integer> metadataColumns =
                createRequiredMetadataColumns(schema, sink).stream()
                        .map(col -> col.getMetadataKey().orElse(col.getName()))
                        .map(keyToMetadataColumn::get)
                        .collect(Collectors.toList());

        final List<String> fieldNames =
                Stream.concat(
                                physicalColumns.stream().map(columns::get).map(Column::getName),
                                metadataColumns.stream()
                                        .map(columns::get)
                                        .map(MetadataColumn.class::cast)
                                        .map(c -> c.getMetadataKey().orElse(c.getName())))
                        .collect(Collectors.toList());

        final Map<String, DataType> metadataMap = extractMetadataMap(sink);

        final List<RexNode> fieldNodes =
                Stream.concat(
                                physicalColumns.stream()
                                        .map(
                                                pos -> {
                                                    final int posAdjusted =
                                                            adjustByVirtualColumns(columns, pos);
                                                    return relBuilder.field(posAdjusted);
                                                }),
                                metadataColumns.stream()
                                        .map(
                                                pos -> {
                                                    final MetadataColumn metadataColumn =
                                                            (MetadataColumn) columns.get(pos);
                                                    final String metadataKey =
                                                            metadataColumn
                                                                    .getMetadataKey()
                                                                    .orElse(
                                                                            metadataColumn
                                                                                    .getName());

                                                    final LogicalType expectedType =
                                                            metadataMap
                                                                    .get(metadataKey)
                                                                    .getLogicalType();
                                                    final RelDataType expectedRelDataType =
                                                            typeFactory
                                                                    .createFieldTypeFromLogicalType(
                                                                            expectedType);

                                                    final int posAdjusted =
                                                            adjustByVirtualColumns(columns, pos);
                                                    return rexBuilder.makeAbstractCast(
                                                            expectedRelDataType,
                                                            relBuilder.field(posAdjusted));
                                                }))
                        .collect(Collectors.toList());

        relBuilder.projectNamed(fieldNodes, fieldNames, true);
    }

    /**
     * Prepares the given {@link DynamicTableSink}. It check whether the sink is compatible with the
     * INSERT INTO clause and applies initial parameters.
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 准备给定的 {@link DynamicTableSink}。
     * 该方法检查Sink是否与INSERT INTO子句兼容，并应用初始参数。
     *
     * @param tableDebugName 表的调试名称，用于日志和调试信息
     * @param staticPartitions 静态分区信息，指定了数据输出的静态分区策略
     * @param isOverwrite 是否覆盖目标表中的数据
     * @param sink 要准备的DynamicTableSink实例
     * @param table 已解析的表定义，包含了表的元数据和结构
     * @param sinkAbilitySpecs Sink的能力规范列表，定义了Sink支持的操作和特性
     */
    private static void prepareDynamicSink(
            String tableDebugName,
            Map<String, String> staticPartitions,
            boolean isOverwrite,
            DynamicTableSink sink,
            ResolvedCatalogTable table,
            List<SinkAbilitySpec> sinkAbilitySpecs) {
        // 验证分区信息是否与表和Sink兼容
        validatePartitioning(tableDebugName, staticPartitions, sink, table.getPartitionKeys());
        // 验证是否允许覆盖，并根据需要应用覆盖逻辑
        validateAndApplyOverwrite(tableDebugName, isOverwrite, sink, sinkAbilitySpecs);
        // 验证并应用元数据，确保Sink能够正确处理表的元数据（如列名、数据类型等）
        validateAndApplyMetadata(tableDebugName, sink, table.getResolvedSchema(), sinkAbilitySpecs);
    }

    /**
     * Returns a list of required metadata columns. Ordered by the iteration order of {@link
     * SupportsWritingMetadata#listWritableMetadata()}.
     *
     * <p>This method assumes that sink and schema have been validated via {@link
     * #prepareDynamicSink}.
     */
    private static List<MetadataColumn> createRequiredMetadataColumns(
            ResolvedSchema schema, DynamicTableSink sink) {
        final List<Column> tableColumns = schema.getColumns();
        final List<Integer> metadataColumns = extractPersistedMetadataColumns(schema);

        Map<String, MetadataColumn> metadataKeysToMetadataColumns = new HashMap<>();

        for (Integer columnIndex : metadataColumns) {
            MetadataColumn metadataColumn = (MetadataColumn) tableColumns.get(columnIndex);
            String metadataKey = metadataColumn.getMetadataKey().orElse(metadataColumn.getName());
            // After resolving, every metadata column has the unique metadata key.
            metadataKeysToMetadataColumns.put(metadataKey, metadataColumn);
        }

        final Map<String, DataType> metadataMap = extractMetadataMap(sink);

        return metadataMap.keySet().stream()
                .filter(metadataKeysToMetadataColumns::containsKey)
                .map(metadataKeysToMetadataColumns::get)
                .collect(Collectors.toList());
    }

    private static ValidationException createSchemaMismatchException(
            String cause,
            String tableDebugName,
            List<RowField> queryFields,
            List<RowField> sinkFields) {
        final String querySchema =
                queryFields.stream()
                        .map(f -> f.getName() + ": " + f.getType().asSummaryString())
                        .collect(Collectors.joining(", ", "[", "]"));
        final String sinkSchema =
                sinkFields.stream()
                        .map(
                                sinkField ->
                                        sinkField.getName()
                                                + ": "
                                                + sinkField.getType().asSummaryString())
                        .collect(Collectors.joining(", ", "[", "]"));

        return new ValidationException(
                String.format(
                        "Column types of query result and sink for '%s' do not match.\n"
                                + "Cause: %s\n\n"
                                + "Query schema: %s\n"
                                + "Sink schema:  %s",
                        tableDebugName, cause, querySchema, sinkSchema));
    }

    private static DataType fixSinkDataType(
            DataTypeFactory dataTypeFactory, DataType sinkDataType) {
        // we ignore NULL constraint, the NULL constraint will be checked during runtime
        // see StreamExecSink and BatchExecSink
        return DataTypeUtils.transform(
                dataTypeFactory,
                sinkDataType,
                TypeTransformations.legacyRawToTypeInfoRaw(),
                TypeTransformations.legacyToNonLegacy(),
                TypeTransformations.toNullable());
    }

    private static void validatePartitioning(
            String tableDebugName,
            Map<String, String> staticPartitions,
            DynamicTableSink sink,
            List<String> partitionKeys) {
        if (!partitionKeys.isEmpty()) {
            if (!(sink instanceof SupportsPartitioning)) {
                throw new TableException(
                        String.format(
                                "Table '%s' is a partitioned table, but the underlying %s doesn't "
                                        + "implement the %s interface.",
                                tableDebugName,
                                DynamicTableSink.class.getSimpleName(),
                                SupportsPartitioning.class.getSimpleName()));
            }
        }

        staticPartitions
                .keySet()
                .forEach(
                        p -> {
                            if (!partitionKeys.contains(p)) {
                                throw new ValidationException(
                                        String.format(
                                                "Static partition column '%s' should be in the partition keys list %s for table '%s'.",
                                                p, partitionKeys, tableDebugName));
                            }
                        });
    }

    private static void validateAndApplyOverwrite(
            String tableDebugName,
            boolean isOverwrite,
            DynamicTableSink sink,
            List<SinkAbilitySpec> sinkAbilitySpecs) {
        if (!isOverwrite) {
            return;
        }
        if (!(sink instanceof SupportsOverwrite)) {
            throw new ValidationException(
                    String.format(
                            "INSERT OVERWRITE requires that the underlying %s of table '%s' "
                                    + "implements the %s interface.",
                            DynamicTableSink.class.getSimpleName(),
                            tableDebugName,
                            SupportsOverwrite.class.getSimpleName()));
        }
        sinkAbilitySpecs.add(new OverwriteSpec(true));
    }

    private static List<Integer> extractPhysicalColumns(ResolvedSchema schema) {
        final List<Column> columns = schema.getColumns();
        return IntStream.range(0, schema.getColumnCount())
                .filter(pos -> columns.get(pos).isPhysical())
                .boxed()
                .collect(Collectors.toList());
    }

    private static List<Integer> extractPersistedMetadataColumns(ResolvedSchema schema) {
        final List<Column> columns = schema.getColumns();
        return IntStream.range(0, schema.getColumnCount())
                .filter(
                        pos -> {
                            final Column column = columns.get(pos);
                            return column instanceof MetadataColumn && column.isPersisted();
                        })
                .boxed()
                .collect(Collectors.toList());
    }

    private static int adjustByVirtualColumns(List<Column> columns, int pos) {
        return pos
                - (int) IntStream.range(0, pos).filter(i -> !columns.get(i).isPersisted()).count();
    }

    private static Map<String, DataType> extractMetadataMap(DynamicTableSink sink) {
        if (sink instanceof SupportsWritingMetadata) {
            return ((SupportsWritingMetadata) sink).listWritableMetadata();
        }
        return Collections.emptyMap();
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 验证并应用表的元数据到DynamicTableSink。
     *
     * @param tableDebugName 表的调试名称，用于日志和调试信息
     * @param sink 要应用元数据的DynamicTableSink实例
     * @param schema 已解析的表模式，包含了表的列和元数据定义
     * @param sinkAbilitySpecs Sink的能力规范列表，定义了Sink支持的操作和特性
     */
    private static void validateAndApplyMetadata(
            String tableDebugName,
            DynamicTableSink sink,
            ResolvedSchema schema,
            List<SinkAbilitySpec> sinkAbilitySpecs) {
        final List<Column> columns = schema.getColumns();// 获取表中的列定义
        // 提取需要持久化的元数据列索引
        final List<Integer> metadataColumns = extractPersistedMetadataColumns(schema);
        // 如果没有需要持久化的元数据列，则直接返回，不进行后续操作
        if (metadataColumns.isEmpty()) {
            return;
        }

        if (!(sink instanceof SupportsWritingMetadata)) {
            throw new ValidationException(
                    String.format(
                            "Table '%s' declares persistable metadata columns, but the underlying %s "
                                    + "doesn't implement the %s interface. If the column should not "
                                    + "be persisted, it can be declared with the VIRTUAL keyword.",
                            tableDebugName,
                            DynamicTableSink.class.getSimpleName(),
                            SupportsWritingMetadata.class.getSimpleName()));
        }

        final Map<String, DataType> metadataMap =
                ((SupportsWritingMetadata) sink).listWritableMetadata();
        metadataColumns.forEach(
                pos -> {
                    final MetadataColumn metadataColumn = (MetadataColumn) columns.get(pos);
                    final String metadataKey =
                            metadataColumn.getMetadataKey().orElse(metadataColumn.getName());
                    final LogicalType metadataType = metadataColumn.getDataType().getLogicalType();
                    final DataType expectedMetadataDataType = metadataMap.get(metadataKey);
                    // check that metadata key is valid
                    if (expectedMetadataDataType == null) {
                        throw new ValidationException(
                                String.format(
                                        "Invalid metadata key '%s' in column '%s' of table '%s'. "
                                                + "The %s class '%s' supports the following metadata keys for writing:\n%s",
                                        metadataKey,
                                        metadataColumn.getName(),
                                        tableDebugName,
                                        DynamicTableSink.class.getSimpleName(),
                                        sink.getClass().getName(),
                                        String.join("\n", metadataMap.keySet())));
                    }
                    // check that types are compatible
                    if (!supportsExplicitCast(
                            metadataType, expectedMetadataDataType.getLogicalType())) {
                        if (metadataKey.equals(metadataColumn.getName())) {
                            throw new ValidationException(
                                    String.format(
                                            "Invalid data type for metadata column '%s' of table '%s'. "
                                                    + "The column cannot be declared as '%s' because the type must be "
                                                    + "castable to metadata type '%s'.",
                                            metadataColumn.getName(),
                                            tableDebugName,
                                            metadataType,
                                            expectedMetadataDataType.getLogicalType()));
                        } else {
                            throw new ValidationException(
                                    String.format(
                                            "Invalid data type for metadata column '%s' with metadata key '%s' of table '%s'. "
                                                    + "The column cannot be declared as '%s' because the type must be "
                                                    + "castable to metadata type '%s'.",
                                            metadataColumn.getName(),
                                            metadataKey,
                                            tableDebugName,
                                            metadataType,
                                            expectedMetadataDataType.getLogicalType()));
                        }
                    }
                });

        sinkAbilitySpecs.add(
                new WritingMetadataSpec(
                        createRequiredMetadataColumns(schema, sink).stream()
                                .map(col -> col.getMetadataKey().orElse(col.getName()))
                                .collect(Collectors.toList()),
                        createConsumedType(schema, sink)));
    }

    /**
     * Returns the {@link DataType} that a sink should consume as the output from the runtime.
     *
     * <p>The format looks as follows: {@code PHYSICAL COLUMNS + PERSISTED METADATA COLUMNS}
     */
    private static RowType createConsumedType(ResolvedSchema schema, DynamicTableSink sink) {
        final Map<String, DataType> metadataMap = extractMetadataMap(sink);

        final Stream<RowField> physicalFields =
                schema.getColumns().stream()
                        .filter(Column::isPhysical)
                        .map(c -> new RowField(c.getName(), c.getDataType().getLogicalType()));

        final Stream<RowField> metadataFields =
                createRequiredMetadataColumns(schema, sink).stream()
                        .map(
                                column ->
                                        new RowField(
                                                // Use alias to ensures that physical and metadata
                                                // columns don't collide.
                                                column.getName(),
                                                metadataMap
                                                        .get(
                                                                column.getMetadataKey()
                                                                        .orElse(column.getName()))
                                                        .getLogicalType()));

        final List<RowField> rowFields =
                Stream.concat(physicalFields, metadataFields).collect(Collectors.toList());

        return new RowType(false, rowFields);
    }

    private DynamicSinkUtils() {
        // no instantiation
    }
}
