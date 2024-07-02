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

package org.apache.flink.table.api.bridge.java.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.internal.AbstractStreamTableEnvironmentImpl;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogStore;
import org.apache.flink.table.catalog.CatalogStoreHolder;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.SchemaResolver;
import org.apache.flink.table.catalog.SchemaTranslator;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.CatalogStoreFactory;
import org.apache.flink.table.factories.PlannerFactoryUtil;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.ExternalQueryOperation;
import org.apache.flink.table.operations.OutputConversionModifyOperation;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceValidation;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.MutableURLClassLoader;
import org.apache.flink.util.Preconditions;

import java.net.URL;
import java.util.Arrays;
import java.util.Optional;

/**
 * The implementation for a Java {@link StreamTableEnvironment}. This enables conversions from/to
 * {@link DataStream}.
 *
 * <p>It binds to a given {@link StreamExecutionEnvironment}.
 */
@Internal
public final class StreamTableEnvironmentImpl extends AbstractStreamTableEnvironmentImpl
        implements StreamTableEnvironment {

    public StreamTableEnvironmentImpl(
            CatalogManager catalogManager,
            ModuleManager moduleManager,
            ResourceManager resourceManager,
            FunctionCatalog functionCatalog,
            TableConfig tableConfig,
            StreamExecutionEnvironment executionEnvironment,
            Planner planner,
            Executor executor,
            boolean isStreamingMode) {
        super(
                catalogManager,
                moduleManager,
                resourceManager,
                tableConfig,
                executor,
                functionCatalog,
                planner,
                isStreamingMode,
                executionEnvironment);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建StreamTableEnvironment
     * @param executionEnvironment Flink 流处理的执行环境
     * @param settings TableEnvironment 的配置和类加载器设置
    */
    public static StreamTableEnvironment create(
            StreamExecutionEnvironment executionEnvironment, EnvironmentSettings settings) {
        /**
         * 创建一个 MutableURLClassLoader，用于加载用户代码
         * URL类加载器 公开URLClassLoader中的“addURL”方法
         */
        final MutableURLClassLoader userClassLoader =
                FlinkUserCodeClassLoaders.create(
                        new URL[0], settings.getUserClassLoader(), settings.getConfiguration());
        // 根据用户类加载器和执行环境查找或创建一个 Executor 对象
        final Executor executor = lookupExecutor(userClassLoader, executionEnvironment);
        // 创建一个默认的 TableConfig 对象，它是 Table API/SQL 的配置中心
        final TableConfig tableConfig = TableConfig.getDefault();
        // 设置 TableConfig 的根配置为 Executor 的配置
        tableConfig.setRootConfiguration(executor.getConfiguration());
        // 将 EnvironmentSettings 中的配置也添加到 TableConfig 中
        tableConfig.addConfiguration(settings.getConfiguration());
        /**
         * 创建一个 ResourceManager 对象，用于管理 Table API/SQL 中使用的资源
         * 用于处理所有用户定义的资源的管理器。
         * 如 Catalogs 和 Functions 等
         */
        final ResourceManager resourceManager =
                new ResourceManager(settings.getConfiguration(), userClassLoader);
        /**
         * 创建一个 ModuleManager 对象
         * 负责加载/卸载模块，管理其生命周期，并解决模块对象。
         * 比如包括函数、用户定义的类型、运算符、规则
         */
        final ModuleManager moduleManager = new ModuleManager();

        // 根据配置和用户类加载器查找并创建 CatalogStoreFactory 对象
        /**
         * 根据配置和用户类加载器查找并创建 CatalogStoreFactory 对象
         * CatalogStoreFactory 用于创建和管理 Catalog 的存储
         */
        final CatalogStoreFactory catalogStoreFactory =
                TableFactoryUtil.findAndCreateCatalogStoreFactory(
                        settings.getConfiguration(), userClassLoader);
        // 创建一个 CatalogStoreFactory.Context 对象，它包含了创建 CatalogStore 所需的上下文信息
        final CatalogStoreFactory.Context catalogStoreFactoryContext =
                TableFactoryUtil.buildCatalogStoreFactoryContext(
                        settings.getConfiguration(), userClassLoader);
        // 打开 CatalogStoreFactory，准备创建 CatalogStore
        catalogStoreFactory.open(catalogStoreFactoryContext);
        /**
         * 创建一个 CatalogStore 对象，用于存储和管理 Catalogs
         * 如果 settings 中已经提供了 CatalogStore，则使用它；否则，使用 CatalogStoreFactory 创建一个新的 CatalogStore
         */
        final CatalogStore catalogStore =
                settings.getCatalogStore() != null
                        ? settings.getCatalogStore()
                        : catalogStoreFactory.createCatalogStore();
        // 创建 CatalogManager 对象，负责管理 Catalogs
        final CatalogManager catalogManager =
                CatalogManager.newBuilder()
                        .classLoader(userClassLoader) // 设置用户自定义的类加载器
                        .config(tableConfig)// 设置 TableConfig，包含 Table API/SQL 的配置
                        .defaultCatalog( // 设置默认的 Catalog，通常是一个内置的、基于内存的 Catalog
                                settings.getBuiltInCatalogName(),
                                new GenericInMemoryCatalog(// 使用 GenericInMemoryCatalog 作为实现
                                        settings.getBuiltInCatalogName(),// 内置 Catalog 的名称
                                        settings.getBuiltInDatabaseName())) // 内置 Database 的名称
                        .executionConfig(executionEnvironment.getConfig())// 设置 Flink 的执行配置
                        .catalogModificationListeners(// 查找并设置 Catalog 修改监听器，用于监听 Catalog 的变化
                                TableFactoryUtil.findCatalogModificationListenerList(
                                        settings.getConfiguration(), userClassLoader))
                        .catalogStoreHolder(// 设置 CatalogStoreHolder，用于持有和管理 CatalogStore
                                CatalogStoreHolder.newBuilder()
                                        .classloader(userClassLoader)// 设置类加载器
                                        .config(tableConfig)// 设置 TableConfig
                                        .catalogStore(catalogStore)// 设置 CatalogStore
                                        .factory(catalogStoreFactory)// 设置 CatalogStoreFactory
                                        .build())// 构建 CatalogStoreHolder
                        .build();// 构建 CatalogManager
        // 创建 FunctionCatalog 对象，用于管理用户定义的函数
        final FunctionCatalog functionCatalog =
                new FunctionCatalog(tableConfig, resourceManager, catalogManager, moduleManager);
        // 创建 Planner 对象，即查询规划器，用于将 SQL 转换为可执行的 Flink 操作
        final Planner planner =
                PlannerFactoryUtil.createPlanner(
                        executor,
                        tableConfig,
                        userClassLoader,
                        moduleManager,
                        catalogManager,
                        functionCatalog);
        // 创建一个新的 StreamTableEnvironmentImpl 实例，它是 Flink Table API 的流式处理实现
        return new StreamTableEnvironmentImpl(
                catalogManager,// CatalogManager 对象，用于管理 Catalogs，包括表、视图、函数等目录对象
                moduleManager,// 模块管理器，用于管理 Flink Table API 的模块或插件
                resourceManager, // 资源管理器，通常用于管理 Flink 集群中的资源
                functionCatalog,// FunctionCatalog 对象，用于管理用户定义的函数
                tableConfig,// TableConfig 对象，包含 Table API/SQL 的配置信息
                executionEnvironment,// Flink 的执行环境，StreamExecutionEnvironment 实例
                planner,// Planner 对象，即查询规划器，用于将 SQL 转换为可执行的 Flink 操作
                executor,        // 执行器，用于执行规划后的 Flink 操作
                settings.isStreamingMode()); // 是否为流式处理模式，如果是 true，则启用流式处理，否则启用批处理
    }

    @Override
    public <T> void registerFunction(String name, TableFunction<T> tableFunction) {
        TypeInformation<T> typeInfo =
                UserDefinedFunctionHelper.getReturnTypeOfTableFunction(tableFunction);

        functionCatalog.registerTempSystemTableFunction(name, tableFunction, typeInfo);
    }

    @Override
    public <T, ACC> void registerFunction(
            String name, AggregateFunction<T, ACC> aggregateFunction) {
        TypeInformation<T> typeInfo =
                UserDefinedFunctionHelper.getReturnTypeOfAggregateFunction(aggregateFunction);
        TypeInformation<ACC> accTypeInfo =
                UserDefinedFunctionHelper.getAccumulatorTypeOfAggregateFunction(aggregateFunction);

        functionCatalog.registerTempSystemAggregateFunction(
                name, aggregateFunction, typeInfo, accTypeInfo);
    }

    @Override
    public <T, ACC> void registerFunction(
            String name, TableAggregateFunction<T, ACC> tableAggregateFunction) {
        TypeInformation<T> typeInfo =
                UserDefinedFunctionHelper.getReturnTypeOfAggregateFunction(tableAggregateFunction);
        TypeInformation<ACC> accTypeInfo =
                UserDefinedFunctionHelper.getAccumulatorTypeOfAggregateFunction(
                        tableAggregateFunction);

        functionCatalog.registerTempSystemAggregateFunction(
                name, tableAggregateFunction, typeInfo, accTypeInfo);
    }

    @Override
    public <T> Table fromDataStream(DataStream<T> dataStream) {
        return fromStreamInternal(dataStream, null, null, ChangelogMode.insertOnly());
    }

    @Override
    public <T> Table fromDataStream(DataStream<T> dataStream, Schema schema) {
        Preconditions.checkNotNull(schema, "Schema must not be null.");
        return fromStreamInternal(dataStream, schema, null, ChangelogMode.insertOnly());
    }

    @Override
    public Table fromChangelogStream(DataStream<Row> dataStream) {
        return fromStreamInternal(dataStream, null, null, ChangelogMode.all());
    }

    @Override
    public Table fromChangelogStream(DataStream<Row> dataStream, Schema schema) {
        Preconditions.checkNotNull(schema, "Schema must not be null.");
        return fromStreamInternal(dataStream, schema, null, ChangelogMode.all());
    }

    @Override
    public Table fromChangelogStream(
            DataStream<Row> dataStream, Schema schema, ChangelogMode changelogMode) {
        Preconditions.checkNotNull(schema, "Schema must not be null.");
        return fromStreamInternal(dataStream, schema, null, changelogMode);
    }

    @Override
    public <T> void createTemporaryView(String path, DataStream<T> dataStream) {
        createTemporaryView(
                path, fromStreamInternal(dataStream, null, path, ChangelogMode.insertOnly()));
    }

    @Override
    public <T> void createTemporaryView(String path, DataStream<T> dataStream, Schema schema) {
        createTemporaryView(
                path, fromStreamInternal(dataStream, schema, path, ChangelogMode.insertOnly()));
    }

    @Override
    public DataStream<Row> toDataStream(Table table) {
        Preconditions.checkNotNull(table, "Table must not be null.");
        // include all columns of the query (incl. metadata and computed columns)
        final DataType sourceType = table.getResolvedSchema().toSourceRowDataType();

        if (!(table.getQueryOperation() instanceof ExternalQueryOperation)) {
            return toDataStream(table, sourceType);
        }

        DataTypeFactory dataTypeFactory = getCatalogManager().getDataTypeFactory();
        SchemaResolver schemaResolver = getCatalogManager().getSchemaResolver();
        ExternalQueryOperation<?> queryOperation =
                (ExternalQueryOperation<?>) table.getQueryOperation();
        DataStream<?> dataStream = queryOperation.getDataStream();

        SchemaTranslator.ConsumingResult consumingResult =
                SchemaTranslator.createConsumingResult(dataTypeFactory, dataStream.getType(), null);
        ResolvedSchema defaultSchema = consumingResult.getSchema().resolve(schemaResolver);

        if (queryOperation.getChangelogMode().equals(ChangelogMode.insertOnly())
                && table.getResolvedSchema().equals(defaultSchema)
                && dataStream.getType() instanceof RowTypeInfo) {
            return (DataStream<Row>) dataStream;
        }

        return toDataStream(table, sourceType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataStream<T> toDataStream(Table table, Class<T> targetClass) {
        Preconditions.checkNotNull(table, "Table must not be null.");
        Preconditions.checkNotNull(targetClass, "Target class must not be null.");
        if (targetClass == Row.class) {
            // for convenience, we allow the Row class here as well
            return (DataStream<T>) toDataStream(table);
        }

        return toDataStream(table, DataTypes.of(targetClass));
    }

    @Override
    public <T> DataStream<T> toDataStream(Table table, AbstractDataType<?> targetDataType) {
        Preconditions.checkNotNull(table, "Table must not be null.");
        Preconditions.checkNotNull(targetDataType, "Target data type must not be null.");

        final SchemaTranslator.ProducingResult schemaTranslationResult =
                SchemaTranslator.createProducingResult(
                        getCatalogManager().getDataTypeFactory(),
                        table.getResolvedSchema(),
                        targetDataType);

        return toStreamInternal(table, schemaTranslationResult, ChangelogMode.insertOnly());
    }

    @Override
    public DataStream<Row> toChangelogStream(Table table) {
        Preconditions.checkNotNull(table, "Table must not be null.");

        final SchemaTranslator.ProducingResult schemaTranslationResult =
                SchemaTranslator.createProducingResult(table.getResolvedSchema(), null);

        return toStreamInternal(table, schemaTranslationResult, null);
    }

    @Override
    public DataStream<Row> toChangelogStream(Table table, Schema targetSchema) {
        Preconditions.checkNotNull(table, "Table must not be null.");
        Preconditions.checkNotNull(targetSchema, "Target schema must not be null.");

        final SchemaTranslator.ProducingResult schemaTranslationResult =
                SchemaTranslator.createProducingResult(table.getResolvedSchema(), targetSchema);

        return toStreamInternal(table, schemaTranslationResult, null);
    }

    @Override
    public DataStream<Row> toChangelogStream(
            Table table, Schema targetSchema, ChangelogMode changelogMode) {
        Preconditions.checkNotNull(table, "Table must not be null.");
        Preconditions.checkNotNull(targetSchema, "Target schema must not be null.");
        Preconditions.checkNotNull(changelogMode, "Changelog mode must not be null.");

        final SchemaTranslator.ProducingResult schemaTranslationResult =
                SchemaTranslator.createProducingResult(table.getResolvedSchema(), targetSchema);

        return toStreamInternal(table, schemaTranslationResult, changelogMode);
    }

    @Override
    public StreamStatementSet createStatementSet() {
        return new StreamStatementSetImpl(this);
    }

    @Override
    public <T> Table fromDataStream(DataStream<T> dataStream, Expression... fields) {
        return createTable(asQueryOperation(dataStream, Optional.of(Arrays.asList(fields))));
    }

    @Override
    public <T> void registerDataStream(String name, DataStream<T> dataStream) {
        createTemporaryView(name, dataStream);
    }

    @Override
    public <T> void createTemporaryView(
            String path, DataStream<T> dataStream, Expression... fields) {
        createTemporaryView(path, fromDataStream(dataStream, fields));
    }

    @Override
    public <T> DataStream<T> toAppendStream(Table table, Class<T> clazz) {
        TypeInformation<T> typeInfo = extractTypeInformation(table, clazz);
        return toAppendStream(table, typeInfo);
    }

    @Override
    public <T> DataStream<T> toAppendStream(Table table, TypeInformation<T> typeInfo) {
        OutputConversionModifyOperation modifyOperation =
                new OutputConversionModifyOperation(
                        table.getQueryOperation(),
                        TypeConversions.fromLegacyInfoToDataType(typeInfo),
                        OutputConversionModifyOperation.UpdateMode.APPEND);
        return toStreamInternal(table, modifyOperation);
    }

    @Override
    public <T> DataStream<Tuple2<Boolean, T>> toRetractStream(Table table, Class<T> clazz) {
        TypeInformation<T> typeInfo = extractTypeInformation(table, clazz);
        return toRetractStream(table, typeInfo);
    }

    @Override
    public <T> DataStream<Tuple2<Boolean, T>> toRetractStream(
            Table table, TypeInformation<T> typeInfo) {
        OutputConversionModifyOperation modifyOperation =
                new OutputConversionModifyOperation(
                        table.getQueryOperation(),
                        wrapWithChangeFlag(typeInfo),
                        OutputConversionModifyOperation.UpdateMode.RETRACT);
        return toStreamInternal(table, modifyOperation);
    }

    @Override
    protected void validateTableSource(TableSource<?> tableSource) {
        super.validateTableSource(tableSource);
        validateTimeCharacteristic(TableSourceValidation.hasRowtimeAttribute(tableSource));
    }
}
