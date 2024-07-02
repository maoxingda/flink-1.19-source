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

package org.apache.flink.table.planner.operations;

import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateTableAs;
import org.apache.flink.sql.parser.ddl.SqlCreateTableLike;
import org.apache.flink.sql.parser.ddl.SqlTableLike;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.CreateTableASOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.planner.calcite.FlinkCalciteSqlValidator;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Helper class for converting {@link SqlCreateTable} to {@link CreateTableOperation}. */
class SqlCreateTableConverter {

    private final MergeTableLikeUtil mergeTableLikeUtil;
    private final CatalogManager catalogManager;

    SqlCreateTableConverter(
            FlinkCalciteSqlValidator sqlValidator,
            CatalogManager catalogManager,
            Function<SqlNode, String> escapeExpression) {
        this.mergeTableLikeUtil =
                new MergeTableLikeUtil(
                        sqlValidator, escapeExpression, catalogManager.getDataTypeFactory());
        this.catalogManager = catalogManager;
    }

    /** Convert the {@link SqlCreateTable} node. */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将 {@link SqlCreateTable} 节点转换为操作。
     *
     * @param sqlCreateTable 要转换的 SqlCreateTable 对象
     * @return 转换后的 CreateTableOperation 操作
    */
    Operation convertCreateTable(SqlCreateTable sqlCreateTable) {
        // 根据 SqlCreateTable 节点创建一个 CatalogTable 对象
        CatalogTable catalogTable = createCatalogTable(sqlCreateTable);
        // 从 SqlCreateTable 中获取完整的表名，并创建一个 UnresolvedIdentifier 对象
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlCreateTable.fullTableName());
        // 使用 CatalogManager 将 UnresolvedIdentifier 解析为 ObjectIdentifier
        // ObjectIdentifier 是已解析的、具有完整命名空间信息的标识符
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

        // 创建一个 CreateTableOperation 对象，并传入解析后的标识符、CatalogTable 对象
        // 以及 SqlCreateTable 节点中的 ifNotExists 和 temporary 标志
        return new CreateTableOperation(
                identifier,
                catalogTable,
                sqlCreateTable.isIfNotExists(),
                sqlCreateTable.isTemporary());
    }

    /** Convert the {@link SqlCreateTableAs} node. */
    Operation convertCreateTableAS(
            FlinkPlannerImpl flinkPlanner, SqlCreateTableAs sqlCreateTableAs) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlCreateTableAs.fullTableName());
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

        PlannerQueryOperation query =
                (PlannerQueryOperation)
                        SqlNodeToOperationConversion.convert(
                                        flinkPlanner, catalogManager, sqlCreateTableAs.getAsQuery())
                                .orElseThrow(
                                        () ->
                                                new TableException(
                                                        "CTAS unsupported node type "
                                                                + sqlCreateTableAs
                                                                        .getAsQuery()
                                                                        .getClass()
                                                                        .getSimpleName()));
        CatalogTable catalogTable = createCatalogTable(sqlCreateTableAs);

        CreateTableOperation createTableOperation =
                new CreateTableOperation(
                        identifier,
                        CatalogTable.of(
                                Schema.newBuilder()
                                        .fromResolvedSchema(query.getResolvedSchema())
                                        .build(),
                                catalogTable.getComment(),
                                catalogTable.getPartitionKeys(),
                                catalogTable.getOptions()),
                        sqlCreateTableAs.isIfNotExists(),
                        sqlCreateTableAs.isTemporary());

        return new CreateTableASOperation(
                createTableOperation, Collections.emptyMap(), query, false);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建表
     */
    private CatalogTable createCatalogTable(SqlCreateTable sqlCreateTable) {

        // 源表的模式（Schema），用于创建新表时使用
        final Schema sourceTableSchema;
        // 源表的分区键列表，用于创建新表时使用
        final List<String> sourcePartitionKeys;
        // 类似表（LIKE）的选项列表，如包括注释、权限等
        final List<SqlTableLike.SqlTableLikeOption> likeOptions;
        // 源表的属性，如存储格式、压缩等
        final Map<String, String> sourceProperties;
        // 判断 SqlCreateTable 是否是 SqlCreateTableLike 的实例
        // 如果是，表示该创建表语句是基于另一个已存在的表来创建的
        if (sqlCreateTable instanceof SqlCreateTableLike) {
            // 强制转换为 SqlTableLike，因为已确认是 SqlCreateTableLike 的实例
            SqlTableLike sqlTableLike = ((SqlCreateTableLike) sqlCreateTable).getTableLike();
            // 查找并获取类似表（LIKE）的源表
            CatalogTable table = lookupLikeSourceTable(sqlTableLike);
            // 获取源表的未解析模式（Schema）
            sourceTableSchema = table.getUnresolvedSchema();
            // 获取源表的分区键
            sourcePartitionKeys = table.getPartitionKeys();
            // 获取类似表（LIKE）的选项
            likeOptions = sqlTableLike.getOptions();
            // 获取源表的属性
            sourceProperties = table.getOptions();
        } else {
            // 如果不是 SqlCreateTableLike 的实例，则使用默认的空模式、分区键列表、选项列表和属性映射
            sourceTableSchema = Schema.newBuilder().build();// 创建一个空的 Schema
            sourcePartitionKeys = Collections.emptyList();// 空的分区键列表
            likeOptions = Collections.emptyList();// 空的选项列表
            sourceProperties = Collections.emptyMap();// 空的属性映射
        }
        // 创建一个 Map 来存储合并策略，键为 FeatureOption，值为 MergingStrategy 将水位线、分区等转换为Map结构
        Map<SqlTableLike.FeatureOption, SqlTableLike.MergingStrategy> mergingStrategies =
                mergeTableLikeUtil.computeMergingStrategies(likeOptions);
        // 合并选项，将 sqlCreateTable 中的选项与源表的属性合并,也就是转换成Map结构
        Map<String, String> mergedOptions =
                mergeOptions(sqlCreateTable, sourceProperties, mergingStrategies);

        // 从 sqlCreateTable 的所有约束中查找主键约束（如果存在）
        Optional<SqlTableConstraint> primaryKey =
                sqlCreateTable.getFullConstraints().stream()
                        .filter(SqlTableConstraint::isPrimaryKey)
                        .findAny();
        // 合并源表模式与目标表列列表、水印和主键约束（如果存在），并应用合并策略
        Schema mergedSchema =
                mergeTableLikeUtil.mergeTables(
                        mergingStrategies,
                        sourceTableSchema,// 源表模式
                        sqlCreateTable.getColumnList().getList(),// 目标表列列表
                        sqlCreateTable
                                .getWatermark()// 水印（如果存在）
                                .map(Collections::singletonList)// 将单个水印封装为列表
                                .orElseGet(Collections::emptyList),// 如果没有水印，则返回空列表
                        primaryKey.orElse(null)); // 主键约束（如果存在）

        // 合并分区键，将源表的分区键与目标表的分区键列表合并，并应用合并策略
        List<String> partitionKeys =
                mergePartitions(
                        sourcePartitionKeys, // 源表的分区键
                        sqlCreateTable.getPartitionKeyList(),// 目标表的分区键列表
                        mergingStrategies); // 合并策略
        // 验证合并后的模式中是否包含所有分区键
        verifyPartitioningColumnsExist(mergedSchema, partitionKeys);
         // 获取表的注释（如果存在）
        String tableComment = OperationConverterUtils.getTableComment(sqlCreateTable.getComment());
        // 解析并返回 CatalogTable 对象
        return catalogManager.resolveCatalogTable(
                CatalogTable.of(
                        mergedSchema, tableComment, partitionKeys, new HashMap<>(mergedOptions)));
    }

    private CatalogTable lookupLikeSourceTable(SqlTableLike sqlTableLike) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlTableLike.getSourceTable().names);
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        ContextResolvedTable lookupResult =
                catalogManager
                        .getTable(identifier)
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                String.format(
                                                        "Source table '%s' of the LIKE clause not found in the catalog, at %s",
                                                        identifier,
                                                        sqlTableLike
                                                                .getSourceTable()
                                                                .getParserPosition())));
        if (!(lookupResult.getResolvedTable() instanceof CatalogTable)) {
            throw new ValidationException(
                    String.format(
                            "Source table '%s' of the LIKE clause can not be a VIEW, at %s",
                            identifier, sqlTableLike.getSourceTable().getParserPosition()));
        }
        return lookupResult.getResolvedTable();
    }

    private void verifyPartitioningColumnsExist(Schema mergedSchema, List<String> partitionKeys) {
        Set<String> columnNames =
                mergedSchema.getColumns().stream()
                        .map(Schema.UnresolvedColumn::getName)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        for (String partitionKey : partitionKeys) {
            if (!columnNames.contains(partitionKey)) {
                throw new ValidationException(
                        String.format(
                                "Partition column '%s' not defined in the table schema. Available columns: [%s]",
                                partitionKey,
                                columnNames.stream()
                                        .collect(Collectors.joining("', '", "'", "'"))));
            }
        }
    }

    private List<String> mergePartitions(
            List<String> sourcePartitionKeys,
            SqlNodeList derivedPartitionKeys,
            Map<SqlTableLike.FeatureOption, SqlTableLike.MergingStrategy> mergingStrategies) {
        // set partition key
        return mergeTableLikeUtil.mergePartitions(
                mergingStrategies.get(SqlTableLike.FeatureOption.PARTITIONS),
                sourcePartitionKeys,
                derivedPartitionKeys.getList().stream()
                        .map(p -> ((SqlIdentifier) p).getSimple())
                        .collect(Collectors.toList()));
    }

    private Map<String, String> mergeOptions(
            SqlCreateTable sqlCreateTable,
            Map<String, String> sourceProperties,
            Map<SqlTableLike.FeatureOption, SqlTableLike.MergingStrategy> mergingStrategies) {
        // set with properties
        Map<String, String> properties = new HashMap<>();
        sqlCreateTable
                .getPropertyList()
                .getList()
                .forEach(
                        p ->
                                properties.put(
                                        ((SqlTableOption) p).getKeyString(),
                                        ((SqlTableOption) p).getValueString()));
        return mergeTableLikeUtil.mergeOptions(
                mergingStrategies.get(SqlTableLike.FeatureOption.OPTIONS),
                sourceProperties,
                properties);
    }
}
