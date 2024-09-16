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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.CustomSinkOperatorUidHashes;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ParallelismProvider;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.DynamicTableSink.SinkRuntimeProvider;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.SinkProvider;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.connectors.TransformationSinkProvider;
import org.apache.flink.table.planner.plan.abilities.sink.RowLevelDeleteSpec;
import org.apache.flink.table.planner.plan.abilities.sink.RowLevelUpdateSpec;
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.sink.ConstraintEnforcer;
import org.apache.flink.table.runtime.operators.sink.RowKindSetter;
import org.apache.flink.table.runtime.operators.sink.SinkOperator;
import org.apache.flink.table.runtime.operators.sink.StreamRecordTimestampInserter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.TemporaryClassLoaderContext;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Base {@link ExecNode} to write data to an external sink defined by a {@link DynamicTableSink}.
 */
public abstract class CommonExecSink extends ExecNodeBase<Object>
        implements MultipleTransformationTranslator<Object> {

    public static final String CONSTRAINT_VALIDATOR_TRANSFORMATION = "constraint-validator";
    public static final String PARTITIONER_TRANSFORMATION = "partitioner";
    public static final String UPSERT_MATERIALIZE_TRANSFORMATION = "upsert-materialize";
    public static final String TIMESTAMP_INSERTER_TRANSFORMATION = "timestamp-inserter";
    public static final String ROW_KIND_SETTER = "row-kind-setter";
    public static final String SINK_TRANSFORMATION = "sink";

    public static final String FIELD_NAME_DYNAMIC_TABLE_SINK = "dynamicTableSink";

    @JsonProperty(FIELD_NAME_DYNAMIC_TABLE_SINK)
    protected final DynamicTableSinkSpec tableSinkSpec;

    private final ChangelogMode inputChangelogMode;
    private final boolean isBounded;
    protected boolean sinkParallelismConfigured;

    protected CommonExecSink(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            DynamicTableSinkSpec tableSinkSpec,
            ChangelogMode inputChangelogMode,
            boolean isBounded,
            List<InputProperty> inputProperties,
            LogicalType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.tableSinkSpec = tableSinkSpec;
        this.inputChangelogMode = inputChangelogMode;
        this.isBounded = isBounded;
    }

    @Override
    public String getSimplifiedName() {
        return tableSinkSpec.getContextResolvedTable().getIdentifier().getObjectName();
    }

    public DynamicTableSinkSpec getTableSinkSpec() {
        return tableSinkSpec;
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 创建一个数据汇的Transformation对象。
     *
     * @param streamExecEnv Flink的流执行环境，用于配置和执行流处理作业。
     * @param config 执行节点的配置信息。
     * @param classLoader 类加载器，用于加载用户定义的函数或类。
     * @param inputTransform 输入的Transformation对象，表示数据源或之前处理步骤的输出。
     * @param tableSink 动态表汇，定义了如何将数据写入外部系统。
     * @param rowtimeFieldIndex 行时间字段的索引，在输入数据中的位置，用于时间相关的处理。
     * @param upsertMaterialize 是否将更新（upsert）操作物化（转换为插入或删除操作），以支持某些不支持直接更新操作的数据存储。
     * @param inputUpsertKey 输入数据中用于更新操作的键的索引数组。
     */
    @SuppressWarnings("unchecked")
    protected Transformation<Object> createSinkTransformation(
            StreamExecutionEnvironment streamExecEnv,
            ExecNodeConfig config,
            ClassLoader classLoader,
            Transformation<RowData> inputTransform,
            DynamicTableSink tableSink,
            int rowtimeFieldIndex,
            boolean upsertMaterialize,
            int[] inputUpsertKey) {
        // 从表汇规范中获取解析后的Schema
        final ResolvedSchema schema = tableSinkSpec.getContextResolvedTable().getResolvedSchema();
        // 获取数据汇的运行时提供者，这通常与具体的外部系统相关
        final SinkRuntimeProvider runtimeProvider =
                tableSink.getSinkRuntimeProvider(
                        new SinkRuntimeProviderContext(
                                isBounded, tableSinkSpec.getTargetColumns()));
        // 根据解析后的Schema获取物理行类型
        final RowType physicalRowType = getPhysicalRowType(schema);
        // 获取主键的索引数组，如果表定义了主键的话
        final int[] primaryKeys = getPrimaryKeyIndices(physicalRowType, schema);
        // 推导数据汇的并行度
        final int sinkParallelism = deriveSinkParallelism(inputTransform, runtimeProvider);
        // 标记是否显式配置了并行度
        sinkParallelismConfigured = isParallelismConfigured(runtimeProvider);
        // 获取输入Transformation的并行度
        final int inputParallelism = inputTransform.getParallelism();
        // 检查输入变更日志是否仅包含插入操作
        final boolean inputInsertOnly = inputChangelogMode.containsOnly(RowKind.INSERT);
        // 检查是否有定义主键
        final boolean hasPk = primaryKeys.length > 0;

        // 检查输入并行度、数据汇并行度和是否包含主键的条件
        if (!inputInsertOnly && sinkParallelism != inputParallelism && !hasPk) {
            // 如果输入不是仅插入模式，且数据汇并行度与输入并行度不同，且没有定义主键，则抛出异常
            throw new TableException(
                    String.format(
                            "The sink for table '%s' has a configured parallelism of %s, while the input parallelism is %s. "
                                    + "Since the configured parallelism is different from the input's parallelism and "
                                    + "the changelog mode is not insert-only, a primary key is required but could not "
                                    + "be found.",
                            tableSinkSpec
                                    .getContextResolvedTable()
                                    .getIdentifier()
                                    .asSummaryString(),
                            sinkParallelism,
                            inputParallelism));
        }

        // only add materialization if input has change
        // 仅当输入包含变更时才需要物化
        final boolean needMaterialization = !inputInsertOnly && upsertMaterialize;
        // 应用约束验证（可能是对输入数据的校验）
        Transformation<RowData> sinkTransform =
                applyConstraintValidations(inputTransform, config, physicalRowType);
        // 如果定义了主键
        if (hasPk) {
            // 应用主键约束，可能涉及到数据的重新排序或分区
            sinkTransform =
                    applyKeyBy(
                            config,
                            classLoader,
                            sinkTransform,
                            primaryKeys,
                            sinkParallelism,
                            inputParallelism,
                            needMaterialization);
        }
        // 如果需要物化（即输入包含更新或删除操作）
        if (needMaterialization) {
            // 应用物化逻辑，将更新和删除操作转换为插入和删除操作（如果目标系统不支持直接更新）
            sinkTransform =
                    applyUpsertMaterialize(
                            sinkTransform,
                            primaryKeys,
                            sinkParallelism,
                            config,
                            classLoader,
                            physicalRowType,
                            inputUpsertKey);
        }
        // 获取目标行类型（可能是INSERT、UPDATE、DELETE等）
        Optional<RowKind> targetRowKind = getTargetRowKind();
        if (targetRowKind.isPresent()) {
            // 如果指定了目标行类型，则应用行类型设置器
            sinkTransform = applyRowKindSetter(sinkTransform, targetRowKind.get(), config);
        }
        // 最后，应用数据汇提供者来创建并返回最终的数据汇Transformation对象
        return (Transformation<Object>)
                applySinkProvider(
                        sinkTransform,
                        streamExecEnv, // Flink的流执行环境
                        runtimeProvider,// 数据汇的运行时提供者
                        rowtimeFieldIndex,// 行时间字段的索引
                        sinkParallelism,// 数据汇的并行度
                        config,// 执行节点的配置
                        classLoader);//类加载器
    }

    /**
     * Apply an operator to filter or report error to process not-null values for not-null fields.
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 应用操作符以过滤或报告非空字段中非空值的错误。
     */
    private Transformation<RowData> applyConstraintValidations(
            Transformation<RowData> inputTransform,
            ExecNodeConfig config,
            RowType physicalRowType) {
        // 初始化约束执行器构建器
        final ConstraintEnforcer.Builder validatorBuilder = ConstraintEnforcer.newBuilder();
        // 获取物理行类型中所有字段的名称
        final String[] fieldNames = physicalRowType.getFieldNames().toArray(new String[0]);

        // Build NOT NULL enforcer
        // 构建非空约束执行器
        final int[] notNullFieldIndices = getNotNullFieldIndices(physicalRowType);
        if (notNullFieldIndices.length > 0) {
            // 从配置中获取非空约束执行器选项
            final ExecutionConfigOptions.NotNullEnforcer notNullEnforcer =
                    config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER);
            // 将非空字段索引转换为字段名称列表
            final List<String> notNullFieldNames =
                    Arrays.stream(notNullFieldIndices)
                            .mapToObj(idx -> fieldNames[idx])
                            .collect(Collectors.toList());
            // 向构建器添加非空约束
            validatorBuilder.addNotNullConstraint(
                    notNullEnforcer, notNullFieldIndices, notNullFieldNames, fieldNames);
        }
        // 从配置中获取类型长度约束执行器选项
        final ExecutionConfigOptions.TypeLengthEnforcer typeLengthEnforcer =
                config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER);

        // Build CHAR/VARCHAR length enforcer
        // 构建CHAR/VARCHAR长度约束执行器
        final List<ConstraintEnforcer.FieldInfo> charFieldInfo =
                getFieldInfoForLengthEnforcer(physicalRowType, LengthEnforcerType.CHAR);
        if (!charFieldInfo.isEmpty()) {
            // 将CHAR类型字段信息中的索引转换为字段名称列表
            final List<String> charFieldNames =
                    charFieldInfo.stream()
                            .map(cfi -> fieldNames[cfi.fieldIdx()])
                            .collect(Collectors.toList());
            // 向构建器添加CHAR长度约束
            validatorBuilder.addCharLengthConstraint(
                    typeLengthEnforcer, charFieldInfo, charFieldNames, fieldNames);
        }

        // Build BINARY/VARBINARY length enforcer
        // 构建BINARY/VARBINARY长度约束执行器
        final List<ConstraintEnforcer.FieldInfo> binaryFieldInfo =
                getFieldInfoForLengthEnforcer(physicalRowType, LengthEnforcerType.BINARY);
        if (!binaryFieldInfo.isEmpty()) {
            // 将BINARY/VARBINARY类型字段信息中的索引转换为字段名称列表
            final List<String> binaryFieldNames =
                    binaryFieldInfo.stream()
                            .map(cfi -> fieldNames[cfi.fieldIdx()])
                            .collect(Collectors.toList());
            // 向构建器添加BINARY/VARBINARY长度约束
            validatorBuilder.addBinaryLengthConstraint(
                    typeLengthEnforcer, binaryFieldInfo, binaryFieldNames, fieldNames);
        }
        // 构建最终的约束执行器
        ConstraintEnforcer constraintEnforcer = validatorBuilder.build();
        if (constraintEnforcer != null) {
            // 如果存在约束执行器（即至少有一个约束被添加），则创建一个新的转换逻辑
            // 该转换逻辑将包含约束验证操作符
            return ExecNodeUtil.createOneInputTransformation(
                    inputTransform,
                    createTransformationMeta(
                            CONSTRAINT_VALIDATOR_TRANSFORMATION,
                            constraintEnforcer.getOperatorName(),
                            "ConstraintEnforcer",
                            config),
                    constraintEnforcer,
                    getInputTypeInfo(),
                    inputTransform.getParallelism(),
                    false);
        } else {
            // there are no not-null fields, just skip adding the enforcer operator
            // 如果没有非空字段或长度约束字段，则跳过添加约束执行器操作符
            // 直接返回原始的输入转换逻辑
            return inputTransform;
        }
    }

    private int[] getNotNullFieldIndices(RowType physicalType) {
        return IntStream.range(0, physicalType.getFieldCount())
                .filter(pos -> !physicalType.getTypeAt(pos).isNullable())
                .toArray();
    }

    /**
     * Returns a List of {@link ConstraintEnforcer.FieldInfo}, each containing the info needed to
     * determine whether a string or binary value needs trimming and/or padding.
     */
    private List<ConstraintEnforcer.FieldInfo> getFieldInfoForLengthEnforcer(
            RowType physicalType, LengthEnforcerType enforcerType) {
        LogicalTypeRoot staticType = null;
        LogicalTypeRoot variableType = null;
        int maxLength = 0;
        switch (enforcerType) {
            case CHAR:
                staticType = LogicalTypeRoot.CHAR;
                variableType = LogicalTypeRoot.VARCHAR;
                maxLength = CharType.MAX_LENGTH;
                break;
            case BINARY:
                staticType = LogicalTypeRoot.BINARY;
                variableType = LogicalTypeRoot.VARBINARY;
                maxLength = BinaryType.MAX_LENGTH;
        }
        final List<ConstraintEnforcer.FieldInfo> fieldsAndLengths = new ArrayList<>();
        for (int i = 0; i < physicalType.getFieldCount(); i++) {
            LogicalType type = physicalType.getTypeAt(i);
            boolean isStatic = type.is(staticType);
            // Should trim and possibly pad
            if ((isStatic && (LogicalTypeChecks.getLength(type) < maxLength))
                    || (type.is(variableType) && (LogicalTypeChecks.getLength(type) < maxLength))) {
                fieldsAndLengths.add(
                        new ConstraintEnforcer.FieldInfo(
                                i, LogicalTypeChecks.getLength(type), isStatic));
            } else if (isStatic) { // Should pad
                fieldsAndLengths.add(new ConstraintEnforcer.FieldInfo(i, null, isStatic));
            }
        }
        return fieldsAndLengths;
    }

    /**
     * Returns the parallelism of sink operator, it assumes the sink runtime provider implements
     * {@link ParallelismProvider}. It returns parallelism defined in {@link ParallelismProvider} if
     * the parallelism is provided, otherwise it uses parallelism of input transformation.
     */
    private int deriveSinkParallelism(
            Transformation<RowData> inputTransform, SinkRuntimeProvider runtimeProvider) {
        final int inputParallelism = inputTransform.getParallelism();
        if (isParallelismConfigured(runtimeProvider)) {
            int sinkParallelism = ((ParallelismProvider) runtimeProvider).getParallelism().get();
            if (sinkParallelism <= 0) {
                throw new TableException(
                        String.format(
                                "Invalid configured parallelism %s for table '%s'.",
                                sinkParallelism,
                                tableSinkSpec
                                        .getContextResolvedTable()
                                        .getIdentifier()
                                        .asSummaryString()));
            }
            return sinkParallelism;
        } else {
            return inputParallelism;
        }
    }

    private boolean isParallelismConfigured(DynamicTableSink.SinkRuntimeProvider runtimeProvider) {
        return runtimeProvider instanceof ParallelismProvider
                && ((ParallelismProvider) runtimeProvider).getParallelism().isPresent();
    }

    /**
     * Apply a primary key partition transformation to guarantee the strict ordering of changelog
     * messages.
     */
    private Transformation<RowData> applyKeyBy(
            ExecNodeConfig config,
            ClassLoader classLoader,
            Transformation<RowData> inputTransform,
            int[] primaryKeys,
            int sinkParallelism,
            int inputParallelism,
            boolean needMaterialize) {
        final ExecutionConfigOptions.SinkKeyedShuffle sinkShuffleByPk =
                config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE);
        boolean sinkKeyBy = false;
        switch (sinkShuffleByPk) {
            case NONE:
                break;
            case AUTO:
                // should cover both insert-only and changelog input
                sinkKeyBy = sinkParallelism != inputParallelism && sinkParallelism != 1;
                break;
            case FORCE:
                // sink single parallelism has no problem (because none partitioner will cause worse
                // disorder)
                sinkKeyBy = sinkParallelism != 1;
                break;
        }
        if (!sinkKeyBy && !needMaterialize) {
            return inputTransform;
        }

        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(classLoader, primaryKeys, getInputTypeInfo());
        final KeyGroupStreamPartitioner<RowData, RowData> partitioner =
                new KeyGroupStreamPartitioner<>(
                        selector, KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM);
        Transformation<RowData> partitionedTransform =
                new PartitionTransformation<>(inputTransform, partitioner);
        createTransformationMeta(PARTITIONER_TRANSFORMATION, "Partitioner", "Partitioner", config)
                .fill(partitionedTransform);
        partitionedTransform.setParallelism(sinkParallelism, sinkParallelismConfigured);
        return partitionedTransform;
    }

    protected abstract Transformation<RowData> applyUpsertMaterialize(
            Transformation<RowData> inputTransform,
            int[] primaryKeys,
            int sinkParallelism,
            ExecNodeConfig config,
            ClassLoader classLoader,
            RowType physicalRowType,
            int[] inputUpsertKey);

    private Transformation<RowData> applyRowKindSetter(
            Transformation<RowData> inputTransform, RowKind rowKind, ExecNodeConfig config) {
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(
                        ROW_KIND_SETTER,
                        String.format("RowKindSetter(TargetRowKind=[%s])", rowKind),
                        "RowKindSetter",
                        config),
                new RowKindSetter(rowKind),
                inputTransform.getOutputType(),
                inputTransform.getParallelism(),
                false);
    }

    private Transformation<?> applySinkProvider(
            Transformation<RowData> inputTransform,
            StreamExecutionEnvironment env,
            SinkRuntimeProvider runtimeProvider,
            int rowtimeFieldIndex,
            int sinkParallelism,
            ExecNodeConfig config,
            ClassLoader classLoader) {
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {

            TransformationMetadata sinkMeta = createTransformationMeta(SINK_TRANSFORMATION, config);
            if (runtimeProvider instanceof DataStreamSinkProvider) {
                Transformation<RowData> sinkTransformation =
                        applyRowtimeTransformation(
                                inputTransform, rowtimeFieldIndex, sinkParallelism, config);
                final DataStream<RowData> dataStream = new DataStream<>(env, sinkTransformation);
                final DataStreamSinkProvider provider = (DataStreamSinkProvider) runtimeProvider;
                return provider.consumeDataStream(createProviderContext(config), dataStream)
                        .getTransformation();
            } else if (runtimeProvider instanceof TransformationSinkProvider) {
                final TransformationSinkProvider provider =
                        (TransformationSinkProvider) runtimeProvider;
                return provider.createTransformation(
                        new TransformationSinkProvider.Context() {
                            @Override
                            public Transformation<RowData> getInputTransformation() {
                                return inputTransform;
                            }

                            @Override
                            public int getRowtimeIndex() {
                                return rowtimeFieldIndex;
                            }

                            @Override
                            public Optional<String> generateUid(String name) {
                                return createProviderContext(config).generateUid(name);
                            }
                        });
            } else if (runtimeProvider instanceof SinkFunctionProvider) {
                final SinkFunction<RowData> sinkFunction =
                        ((SinkFunctionProvider) runtimeProvider).createSinkFunction();
                return createSinkFunctionTransformation(
                        sinkFunction,
                        env,
                        inputTransform,
                        rowtimeFieldIndex,
                        sinkMeta,
                        sinkParallelism);
            } else if (runtimeProvider instanceof OutputFormatProvider) {
                OutputFormat<RowData> outputFormat =
                        ((OutputFormatProvider) runtimeProvider).createOutputFormat();
                final SinkFunction<RowData> sinkFunction =
                        new OutputFormatSinkFunction<>(outputFormat);
                return createSinkFunctionTransformation(
                        sinkFunction,
                        env,
                        inputTransform,
                        rowtimeFieldIndex,
                        sinkMeta,
                        sinkParallelism);
            } else if (runtimeProvider instanceof SinkProvider) {
                Transformation<RowData> sinkTransformation =
                        applyRowtimeTransformation(
                                inputTransform, rowtimeFieldIndex, sinkParallelism, config);
                final DataStream<RowData> dataStream = new DataStream<>(env, sinkTransformation);
                final Transformation<?> transformation =
                        DataStreamSink.forSinkV1(
                                        dataStream,
                                        ((SinkProvider) runtimeProvider).createSink(),
                                        CustomSinkOperatorUidHashes.DEFAULT)
                                .getTransformation();
                transformation.setParallelism(sinkParallelism, sinkParallelismConfigured);
                sinkMeta.fill(transformation);
                return transformation;
            } else if (runtimeProvider instanceof SinkV2Provider) {
                Transformation<RowData> sinkTransformation =
                        applyRowtimeTransformation(
                                inputTransform, rowtimeFieldIndex, sinkParallelism, config);
                final DataStream<RowData> dataStream = new DataStream<>(env, sinkTransformation);
                final Transformation<?> transformation =
                        DataStreamSink.forSink(
                                        dataStream,
                                        ((SinkV2Provider) runtimeProvider).createSink(),
                                        CustomSinkOperatorUidHashes.DEFAULT)
                                .getTransformation();
                transformation.setParallelism(sinkParallelism, sinkParallelismConfigured);
                sinkMeta.fill(transformation);
                return transformation;
            } else {
                throw new TableException("Unsupported sink runtime provider.");
            }
        }
    }

    private ProviderContext createProviderContext(ExecNodeConfig config) {
        return name -> {
            if (this instanceof StreamExecNode && config.shouldSetUid()) {
                return Optional.of(createTransformationUid(name, config));
            }
            return Optional.empty();
        };
    }

    private Transformation<?> createSinkFunctionTransformation(
            SinkFunction<RowData> sinkFunction,
            StreamExecutionEnvironment env,
            Transformation<RowData> inputTransformation,
            int rowtimeFieldIndex,
            TransformationMetadata transformationMetadata,
            int sinkParallelism) {
        final SinkOperator operator = new SinkOperator(env.clean(sinkFunction), rowtimeFieldIndex);

        if (sinkFunction instanceof InputTypeConfigurable) {
            ((InputTypeConfigurable) sinkFunction)
                    .setInputType(getInputTypeInfo(), env.getConfig());
        }

        final Transformation<?> transformation =
                new LegacySinkTransformation<>(
                        inputTransformation,
                        transformationMetadata.getName(),
                        SimpleOperatorFactory.of(operator),
                        sinkParallelism,
                        sinkParallelismConfigured);
        transformationMetadata.fill(transformation);
        return transformation;
    }

    private Transformation<RowData> applyRowtimeTransformation(
            Transformation<RowData> inputTransform,
            int rowtimeFieldIndex,
            int sinkParallelism,
            ExecNodeConfig config) {
        // Don't apply the transformation/operator if there is no rowtimeFieldIndex
        if (rowtimeFieldIndex == -1) {
            return inputTransform;
        }
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(
                        TIMESTAMP_INSERTER_TRANSFORMATION,
                        String.format(
                                "StreamRecordTimestampInserter(rowtime field: %s)",
                                rowtimeFieldIndex),
                        "StreamRecordTimestampInserter",
                        config),
                new StreamRecordTimestampInserter(rowtimeFieldIndex),
                inputTransform.getOutputType(),
                sinkParallelism,
                sinkParallelismConfigured);
    }

    private InternalTypeInfo<RowData> getInputTypeInfo() {
        return InternalTypeInfo.of(getInputEdges().get(0).getOutputType());
    }

    protected int[] getPrimaryKeyIndices(RowType sinkRowType, ResolvedSchema schema) {
        return schema.getPrimaryKey()
                .map(k -> k.getColumns().stream().mapToInt(sinkRowType::getFieldIndex).toArray())
                .orElse(new int[0]);
    }

    protected RowType getPhysicalRowType(ResolvedSchema schema) {
        return (RowType) schema.toPhysicalRowDataType().getLogicalType();
    }

    private enum LengthEnforcerType {
        CHAR,
        BINARY
    }

    /**
     * Get the target row-kind that the row data should change to, assuming the current row kind is
     * RowKind.INSERT. Return Optional.empty() if it doesn't need to change. Currently, it'll only
     * consider row-level delete/update.
     */
    private Optional<RowKind> getTargetRowKind() {
        if (tableSinkSpec.getSinkAbilities() != null) {
            for (SinkAbilitySpec sinkAbilitySpec : tableSinkSpec.getSinkAbilities()) {
                if (sinkAbilitySpec instanceof RowLevelDeleteSpec) {
                    RowLevelDeleteSpec deleteSpec = (RowLevelDeleteSpec) sinkAbilitySpec;
                    if (deleteSpec.getRowLevelDeleteMode()
                            == SupportsRowLevelDelete.RowLevelDeleteMode.DELETED_ROWS) {
                        return Optional.of(RowKind.DELETE);
                    }
                } else if (sinkAbilitySpec instanceof RowLevelUpdateSpec) {
                    RowLevelUpdateSpec updateSpec = (RowLevelUpdateSpec) sinkAbilitySpec;
                    if (updateSpec.getRowLevelUpdateMode()
                            == SupportsRowLevelUpdate.RowLevelUpdateMode.UPDATED_ROWS) {
                        return Optional.of(RowKind.UPDATE_AFTER);
                    }
                }
            }
        }
        return Optional.empty();
    }
}
