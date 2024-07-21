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

package org.apache.flink.table.planner.catalog;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.planner.calcite.TimestampSchemaVersion;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;
import static org.apache.flink.table.planner.utils.CatalogTableStatisticsConverter.convertToTableStats;

/**
 * A mapping between Flink catalog's database and Calcite's schema. Tables are registered as tables
 * in the schema.
 */
class DatabaseCalciteSchema extends FlinkSchema {
    private final String catalogName;
    private final String databaseName;
    private final CatalogManager catalogManager;
    // Flag that tells if the current planner should work in a batch or streaming mode.
    private final boolean isStreamingMode;

    public DatabaseCalciteSchema(
            String catalogName,
            String databaseName,
            CatalogManager catalog,
            boolean isStreamingMode) {
        this.databaseName = databaseName;
        this.catalogName = catalogName;
        this.catalogManager = catalog;
        this.isStreamingMode = isStreamingMode;
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 根据tableName获取Table对象
     */
    @Override
    public Table getTable(String tableName) {
        // 构造对象标识符，用于唯一标识表，通常包括catalogName、databaseName和tableName
        final ObjectIdentifier identifier =
                ObjectIdentifier.of(catalogName, databaseName, tableName);
        // 声明一个Optional<ContextResolvedTable>来存储可能找到的表
        Optional<ContextResolvedTable> table;
        // 检查是否存在Schema版本
        if (getSchemaVersion().isPresent()) {
            // 获取Schema版本
            SchemaVersion schemaVersion = getSchemaVersion().get();
            // 判断Schema版本类型是否为TimestampSchemaVersion
            if (schemaVersion instanceof TimestampSchemaVersion) {
                // 如果是，则强制转换为TimestampSchemaVersion
                TimestampSchemaVersion timestampSchemaVersion =
                        (TimestampSchemaVersion) getSchemaVersion().get();
                // 使用时间戳和标识符从catalogManager中获取表
                table = catalogManager.getTable(identifier, timestampSchemaVersion.getTimestamp());
            } else {
                // 如果不是TimestampSchemaVersion类型，则抛出不支持的操作异常
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported schema version type: %s", schemaVersion.getClass()));
            }
        } else {
            // 如果没有指定Schema版本，则直接使用标识符从catalogManager中获取表
            table = catalogManager.getTable(identifier);
        }
        // 使用map操作处理Optional中的ContextResolvedTable，如果Optional非空，则进行转换
        return table.map(
                        lookupResult ->
                                new CatalogSchemaTable(
                                        lookupResult,
                                        getStatistic(lookupResult, identifier),
                                        isStreamingMode))
                .orElse(null);
    }

    private FlinkStatistic getStatistic(
            ContextResolvedTable contextResolvedTable, ObjectIdentifier identifier) {
        final ResolvedCatalogBaseTable<?> resolvedBaseTable =
                contextResolvedTable.getResolvedTable();
        switch (resolvedBaseTable.getTableKind()) {
            case TABLE:
                return FlinkStatistic.unknown(resolvedBaseTable.getResolvedSchema())
                        .tableStats(extractTableStats(contextResolvedTable, identifier))
                        .build();
            case VIEW:
            default:
                return FlinkStatistic.UNKNOWN();
        }
    }

    private TableStats extractTableStats(
            ContextResolvedTable lookupResult, ObjectIdentifier identifier) {
        if (lookupResult.isTemporary()) {
            return TableStats.UNKNOWN;
        }
        final Catalog catalog = lookupResult.getCatalog().orElseThrow(IllegalStateException::new);
        final ObjectPath tablePath = identifier.toObjectPath();
        try {
            final CatalogTableStatistics tableStatistics = catalog.getTableStatistics(tablePath);
            final CatalogColumnStatistics columnStatistics =
                    catalog.getTableColumnStatistics(tablePath);
            return convertToTableStats(tableStatistics, columnStatistics);
        } catch (TableNotExistException e) {
            throw new ValidationException(
                    format(
                            "Could not get statistic for table: [%s, %s, %s]",
                            identifier.getCatalogName(),
                            tablePath.getDatabaseName(),
                            tablePath.getObjectName()),
                    e);
        }
    }

    @Override
    public Set<String> getTableNames() {
        return catalogManager.listTables(catalogName, databaseName);
    }

    @Override
    public Schema getSubSchema(String s) {
        return null;
    }

    @Override
    public Set<String> getSubSchemaNames() {
        return new HashSet<>();
    }

    @Override
    public Expression getExpression(SchemaPlus parentSchema, String name) {
        return Schemas.subSchemaExpression(parentSchema, name, getClass());
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Override
    public DatabaseCalciteSchema copy() {
        return new DatabaseCalciteSchema(
                catalogName, databaseName, catalogManager, isStreamingMode);
    }
}
