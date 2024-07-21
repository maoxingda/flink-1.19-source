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

package org.apache.flink.table.planner.plan;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.planner.calcite.FlinkSqlNameMatcher;
import org.apache.flink.table.planner.catalog.CatalogSchemaTable;
import org.apache.flink.table.planner.catalog.QueryOperationCatalogViewTable;
import org.apache.flink.table.planner.catalog.SqlCatalogViewTable;
import org.apache.flink.table.planner.plan.schema.CatalogSourceTable;
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase;
import org.apache.flink.table.planner.plan.schema.LegacyCatalogSourceTable;
import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.utils.TableSchemaUtils;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Flink specific {@link CalciteCatalogReader} that changes the RelOptTable which wrapped a {@link
 * CatalogSchemaTable} to a {@link FlinkPreparingTableBase}.
 */
public class FlinkCalciteCatalogReader extends CalciteCatalogReader {

    public FlinkCalciteCatalogReader(
            CalciteSchema rootSchema,
            List<List<String>> defaultSchemas,
            RelDataTypeFactory typeFactory,
            CalciteConnectionConfig config) {

        super(
                rootSchema,
                new FlinkSqlNameMatcher(
                        SqlNameMatchers.withCaseSensitive(config != null && config.caseSensitive()),
                        typeFactory),
                Stream.concat(defaultSchemas.stream(), Stream.of(Collections.<String>emptyList()))
                        .collect(Collectors.toList()),
                typeFactory,
                config);
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 获取表结构对象
     */
    @Override
    public Prepare.PreparingTable getTable(List<String> names) {
        // 调用父类的方法，根据给定的名称列表获取原始的表对象
        Prepare.PreparingTable originRelOptTable = super.getTable(names);
        // 如果未找到对应的表，则返回null
        if (originRelOptTable == null) {
            return null;
        } else {
            // Wrap as FlinkPreparingTableBase to use in query optimization.
            // 尝试将原始的表对象转换为CatalogSchemaTable类型，以便在查询优化中使用
            CatalogSchemaTable table = originRelOptTable.unwrap(CatalogSchemaTable.class);
            // 如果成功转换，说明这是一个可以被Flink进一步处理的表
            if (table != null) {
                // 调用toPreparingTable方法，将原始表对象、其schema、表的全名、行类型以及转换后的CatalogSchemaTable封装成一个新的PreparingTable对象
                // 这个新的对象将用于Flink的查询优化过程中
                return toPreparingTable(
                        originRelOptTable.getRelOptSchema(),
                        originRelOptTable.getQualifiedName(),
                        originRelOptTable.getRowType(),
                        table);
            } else {
                // 如果转换失败，说明这个表可能不是Flink直接支持的类型，直接返回原始的PreparingTable对象
                return originRelOptTable;
            }
        }
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 将给定的{@link CatalogSchemaTable}转换为Flink的源表。
     * @param relOptSchema Flink的RelOptSchema，表示逻辑优化中的Schema
     * @param names  表的名称列表，用于定位表
     * @param rowType 表的行类型，表示表中的数据结构
     */
    /** Translate this {@link CatalogSchemaTable} into Flink source table. */
    private static FlinkPreparingTableBase toPreparingTable(
            RelOptSchema relOptSchema,
            List<String> names,
            RelDataType rowType,
            CatalogSchemaTable schemaTable) {
        // 从CatalogSchemaTable中获取已解析的表对象
        final ResolvedCatalogBaseTable<?> resolvedBaseTable =
                schemaTable.getContextResolvedTable().getResolvedTable();
        // 从已解析的表对象中获取原始的表对象
        final CatalogBaseTable originTable = resolvedBaseTable.getOrigin();
        // 根据原始表对象的类型进行不同的转换处理
        if (originTable instanceof QueryOperationCatalogView) {
            // 如果原始表是一个查询操作视图，则进行相应的转换
            return convertQueryOperationView(
                    relOptSchema, names, rowType, (QueryOperationCatalogView) originTable);
        } else if (originTable instanceof ConnectorCatalogTable) {
            // 如果原始表是一个连接器表
            ConnectorCatalogTable<?, ?> connectorTable = (ConnectorCatalogTable<?, ?>) originTable;
            // 如果连接器表包含TableSource，则进行转换
            if ((connectorTable).getTableSource().isPresent()) {
                return convertLegacyTableSource(
                        relOptSchema,
                        rowType,
                        schemaTable.getContextResolvedTable().getIdentifier(),
                        connectorTable,
                        schemaTable.getStatistic(),
                        schemaTable.isStreamingMode());
            } else {
                // 如果连接器表没有TableSource，则抛出异常
                throw new ValidationException(
                        "Cannot convert a connector table " + "without source.");
            }
        } else if (originTable instanceof CatalogView) {
            // 如果原始表是一个目录视图，则进行相应的转换
            return convertCatalogView(
                    relOptSchema,
                    names,
                    rowType,
                    schemaTable.getStatistic(),
                    (CatalogView) originTable);
        } else if (originTable instanceof CatalogTable) {
            // 如果原始表是一个普通的目录表，则进行相应的转换
            return convertCatalogTable(relOptSchema, names, rowType, schemaTable);
        } else {
            // 如果原始表不是上述任何支持的类型，则抛出异常
            throw new ValidationException("Unsupported table type: " + originTable);
        }
    }

    private static FlinkPreparingTableBase convertQueryOperationView(
            RelOptSchema relOptSchema,
            List<String> names,
            RelDataType rowType,
            QueryOperationCatalogView view) {
        return QueryOperationCatalogViewTable.create(relOptSchema, names, rowType, view);
    }

    private static FlinkPreparingTableBase convertCatalogView(
            RelOptSchema relOptSchema,
            List<String> names,
            RelDataType rowType,
            FlinkStatistic statistic,
            CatalogView view) {
        return new SqlCatalogViewTable(
                relOptSchema, rowType, names, statistic, view, names.subList(0, 2));
    }

    private static FlinkPreparingTableBase convertLegacyTableSource(
            RelOptSchema relOptSchema,
            RelDataType rowType,
            ObjectIdentifier tableIdentifier,
            ConnectorCatalogTable<?, ?> table,
            FlinkStatistic statistic,
            boolean isStreamingMode) {
        TableSource<?> tableSource = table.getTableSource().get();
        if (!(tableSource instanceof StreamTableSource
                || tableSource instanceof LookupableTableSource)) {
            throw new ValidationException(
                    "Only StreamTableSource and LookupableTableSource can be used in planner.");
        }
        if (!isStreamingMode
                && tableSource instanceof StreamTableSource
                && !((StreamTableSource<?>) tableSource).isBounded()) {
            throw new ValidationException(
                    "Only bounded StreamTableSource can be used in batch mode.");
        }

        return new LegacyTableSourceTable<>(
                relOptSchema,
                tableIdentifier,
                rowType,
                statistic,
                tableSource,
                isStreamingMode,
                table);
    }
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 将目录表（CatalogTable）转换为Flink的源表表示，根据是否为旧版源选项进行区分。
     *
     * @param relOptSchema Flink的逻辑优化Schema
     * @param names 表的名称列表
     * @param rowType 表的行类型
     * @param schemaTable 要转换的目录表对象
     * @return 转换后的Flink源表表示
     */
    private static FlinkPreparingTableBase convertCatalogTable(
            RelOptSchema relOptSchema,
            List<String> names,
            RelDataType rowType,
            CatalogSchemaTable schemaTable) {
        // 检查目录表是否包含旧版的源选项
        if (isLegacySourceOptions(schemaTable)) {
            // 如果是旧版源选项，则使用LegacyCatalogSourceTable进行转换
            return new LegacyCatalogSourceTable<>(
                    relOptSchema,
                    names,
                    rowType,
                    schemaTable,
                    schemaTable.getContextResolvedTable().getResolvedTable());
        } else {
            // 如果不是旧版源选项，则使用CatalogSourceTable进行转换
            return new CatalogSourceTable(relOptSchema, names, rowType, schemaTable);
        }
    }

    /** Checks whether the {@link CatalogTable} uses legacy connector source options. */
    private static boolean isLegacySourceOptions(CatalogSchemaTable schemaTable) {
        // normalize option keys
        DescriptorProperties properties = new DescriptorProperties(true);
        properties.putProperties(
                schemaTable.getContextResolvedTable().getResolvedTable().getOptions());
        if (properties.containsKey(ConnectorDescriptorValidator.CONNECTOR_TYPE)) {
            return true;
        } else {
            // try to create legacy table source using the options,
            // some legacy factories uses the new 'connector' key
            try {
                // The input table is ResolvedCatalogTable that the
                // rowtime/proctime contains {@link TimestampKind}. However, rowtime
                // is the concept defined by the WatermarkGenerator and the
                // WatermarkGenerator is responsible to convert the rowtime column
                // to Long. For source, it only treats the rowtime column as regular
                // timestamp. So, we erase the rowtime indicator here. Please take a
                // look at the usage of the {@link
                // DataTypeUtils#removeTimeAttribute}
                ResolvedCatalogTable originTable =
                        schemaTable.getContextResolvedTable().getResolvedTable();
                ResolvedSchema resolvedSchemaWithRemovedTimeAttribute =
                        TableSchemaUtils.removeTimeAttributeFromResolvedSchema(
                                originTable.getResolvedSchema());
                TableFactoryUtil.findAndCreateTableSource(
                        schemaTable.getContextResolvedTable().getCatalog().orElse(null),
                        schemaTable.getContextResolvedTable().getIdentifier(),
                        new ResolvedCatalogTable(
                                CatalogTable.of(
                                        Schema.newBuilder()
                                                .fromResolvedSchema(
                                                        resolvedSchemaWithRemovedTimeAttribute)
                                                .build(),
                                        originTable.getComment(),
                                        originTable.getPartitionKeys(),
                                        originTable.getOptions()),
                                resolvedSchemaWithRemovedTimeAttribute),
                        new Configuration(),
                        schemaTable.isTemporary());
                // success, then we will use the legacy factories
                return true;
            } catch (Throwable e) {
                // fail, then we will use new factories
                return false;
            }
        }
    }
}
