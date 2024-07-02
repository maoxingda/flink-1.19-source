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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;

import static org.apache.flink.table.factories.ManagedTableFactory.discoverManagedTableFactory;

/**
 * The listener for managed table operations.
 *
 * @deprecated This interface will be removed soon. Please see FLIP-346 for more details.
 */
@Deprecated
@Internal
public class ManagedTableListener {

    private final ClassLoader classLoader;

    private final ReadableConfig config;

    public ManagedTableListener(ClassLoader classLoader, ReadableConfig config) {
        this.classLoader = classLoader;
        this.config = config;
    }

    /** Notify for creating managed table. */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 通知表创建事件，并可能根据需要进行一些额外的处理。
     *
     * @param catalog          关联的Catalog，可能为null
     * @param identifier       表的完全限定标识符
     * @param table            要创建的已解析表
     * @param isTemporary      是否是一个临时表
     * @param ignoreIfExists   如果表已存在，是否忽略
     * @return 可能是经过处理的已解析表，或者直接返回传入的表
    */
    public ResolvedCatalogBaseTable<?> notifyTableCreation(
            @Nullable Catalog catalog,
            ObjectIdentifier identifier,
            ResolvedCatalogBaseTable<?> table,
            boolean isTemporary,
            boolean ignoreIfExists) {
        // 检查该表是否是需要管理的表
        if (isManagedTable(catalog, table)) {
            // 丰富表的选项（可能包括添加额外的元数据、属性等）
            ResolvedCatalogTable managedTable = enrichOptions(identifier, table, isTemporary);
            // 调用表工厂以发现和处理表的创建事件
            // 这可能包括触发额外的初始化步骤、验证或记录日志等
            discoverManagedTableFactory(classLoader)
                    .onCreateTable(
                            // 创建一个用于表工厂上下文的对象
                            createTableFactoryContext(identifier, managedTable, isTemporary),
                            // 传递是否忽略已存在的表标志
                            ignoreIfExists);
            // 返回经过处理的表
            return managedTable;
        }
        return table;
    }

    /** Notify for dropping managed table. */
    public void notifyTableDrop(
            @Nullable Catalog catalog,
            ObjectIdentifier identifier,
            ResolvedCatalogBaseTable<?> table,
            boolean isTemporary,
            boolean ignoreIfNotExists) {
        if (isManagedTable(catalog, table)) {
            discoverManagedTableFactory(classLoader)
                    .onDropTable(
                            createTableFactoryContext(
                                    identifier, (ResolvedCatalogTable) table, isTemporary),
                            ignoreIfNotExists);
        }
    }

    /** Notify compaction for managed table. */
    public Map<String, String> notifyTableCompaction(
            @Nullable Catalog catalog,
            ObjectIdentifier identifier,
            ResolvedCatalogBaseTable<?> table,
            CatalogPartitionSpec partitionSpec,
            boolean isTemporary) {
        if (isManagedTable(catalog, table)) {
            if (RuntimeExecutionMode.STREAMING.equals(config.get(ExecutionOptions.RUNTIME_MODE))) {
                throw new ValidationException("Compact managed table only works under batch mode.");
            }
            return discoverManagedTableFactory(classLoader)
                    .onCompactTable(
                            createTableFactoryContext(
                                    identifier, (ResolvedCatalogTable) table, isTemporary),
                            partitionSpec);
        }
        throw new ValidationException("Only managed table supports compaction");
    }

    /** Check a resolved catalog table is Flink's managed table or not. */
    public static boolean isManagedTable(@Nullable Catalog catalog, CatalogBaseTable table) {
        if (catalog == null || !catalog.supportsManagedTable()) {
            // catalog not support managed table
            return false;
        }

        if (table.getTableKind() != CatalogBaseTable.TableKind.TABLE
                || !(table instanceof CatalogTable)) {
            // view is not managed table
            return false;
        }

        Map<String, String> options;
        try {
            options = table.getOptions();
        } catch (TableException ignore) {
            // exclude abnormal tables, such as InlineCatalogTable that does not have the options
            return false;
        }

        // check legacy connector, here we need to check the factory, other properties are dummy
        if (TableFactoryUtil.isLegacyConnectorOptions(
                catalog,
                new Configuration(),
                true,
                ObjectIdentifier.of("dummy_catalog", "dummy_database", "dummy_table"),
                (CatalogTable) table,
                true)) {
            // legacy connector is not managed table
            return false;
        }

        if (!StringUtils.isNullOrWhitespaceOnly(options.get(FactoryUtil.CONNECTOR.key()))) {
            // with connector is not managed table
            return false;
        }

        if (table instanceof ResolvedCatalogBaseTable) {
            table = ((ResolvedCatalogBaseTable<?>) table).getOrigin();
        }
        // ConnectorCatalogTable is not managed table
        return !(table instanceof ConnectorCatalogTable);
    }

    /** Enrich options for creating managed table. */
    private ResolvedCatalogTable enrichOptions(
            ObjectIdentifier identifier, ResolvedCatalogBaseTable<?> table, boolean isTemporary) {
        if (!(table instanceof ResolvedCatalogTable)) {
            throw new UnsupportedOperationException(
                    "Managed table only supports catalog table, unsupported table type: "
                            + table.getClass());
        }
        ResolvedCatalogTable resolvedTable = (ResolvedCatalogTable) table;
        Map<String, String> newOptions =
                discoverManagedTableFactory(classLoader)
                        .enrichOptions(
                                createTableFactoryContext(identifier, resolvedTable, isTemporary));
        return resolvedTable.copy(newOptions);
    }

    private DynamicTableFactory.Context createTableFactoryContext(
            ObjectIdentifier identifier, ResolvedCatalogTable table, boolean isTemporary) {
        return new FactoryUtil.DefaultDynamicTableContext(
                identifier, table, Collections.emptyMap(), config, classLoader, isTemporary);
    }
}
