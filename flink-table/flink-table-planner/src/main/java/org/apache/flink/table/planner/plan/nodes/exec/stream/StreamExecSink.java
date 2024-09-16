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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.RowtimeInserter;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.connectors.CollectDynamicSink;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.typeutils.RowTypeUtils;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializer;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Stream {@link ExecNode} to write data into an external sink defined by a {@link
 * DynamicTableSink}.
 */
@ExecNodeMetadata(
        name = "stream-exec-sink",
        version = 1,
        consumedOptions = {
            "table.exec.sink.not-null-enforcer",
            "table.exec.sink.type-length-enforcer",
            "table.exec.sink.upsert-materialize",
            "table.exec.sink.keyed-shuffle",
            "table.exec.sink.rowtime-inserter"
        },
        producedTransformations = {
            CommonExecSink.CONSTRAINT_VALIDATOR_TRANSFORMATION,
            CommonExecSink.PARTITIONER_TRANSFORMATION,
            CommonExecSink.UPSERT_MATERIALIZE_TRANSFORMATION,
            CommonExecSink.TIMESTAMP_INSERTER_TRANSFORMATION,
            CommonExecSink.SINK_TRANSFORMATION
        },
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecSink extends CommonExecSink implements StreamExecNode<Object> {

    public static final String FIELD_NAME_INPUT_CHANGELOG_MODE = "inputChangelogMode";
    public static final String FIELD_NAME_REQUIRE_UPSERT_MATERIALIZE = "requireUpsertMaterialize";
    public static final String FIELD_NAME_INPUT_UPSERT_KEY = "inputUpsertKey";

    /** New introduced state metadata to enable operator-level state TTL configuration. */
    public static final String STATE_NAME = "sinkMaterializeState";

    @JsonProperty(FIELD_NAME_INPUT_CHANGELOG_MODE)
    private final ChangelogMode inputChangelogMode;

    @JsonProperty(FIELD_NAME_REQUIRE_UPSERT_MATERIALIZE)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private final boolean upsertMaterialize;

    @JsonProperty(FIELD_NAME_INPUT_UPSERT_KEY)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private final int[] inputUpsertKey;

    @Nullable
    @JsonProperty(FIELD_NAME_STATE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<StateMetadata> stateMetadataList;

    public StreamExecSink(
            ReadableConfig tableConfig,
            DynamicTableSinkSpec tableSinkSpec,
            ChangelogMode inputChangelogMode,
            InputProperty inputProperty,
            LogicalType outputType,
            boolean upsertMaterialize,
            int[] inputUpsertKey,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecSink.class),
                ExecNodeContext.newPersistedConfig(StreamExecSink.class, tableConfig),
                tableSinkSpec,
                inputChangelogMode,
                upsertMaterialize,
                // do not serialize state metadata if upsertMaterialize is not required
                upsertMaterialize
                        ? StateMetadata.getOneInputOperatorDefaultMeta(tableConfig, STATE_NAME)
                        : null,
                inputUpsertKey,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecSink(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_DYNAMIC_TABLE_SINK) DynamicTableSinkSpec tableSinkSpec,
            @JsonProperty(FIELD_NAME_INPUT_CHANGELOG_MODE) ChangelogMode inputChangelogMode,
            @JsonProperty(FIELD_NAME_REQUIRE_UPSERT_MATERIALIZE) boolean upsertMaterialize,
            @Nullable @JsonProperty(FIELD_NAME_STATE) List<StateMetadata> stateMetadataList,
            @JsonProperty(FIELD_NAME_INPUT_UPSERT_KEY) int[] inputUpsertKey,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) LogicalType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(
                id,
                context,
                persistedConfig,
                tableSinkSpec,
                inputChangelogMode,
                false, // isBounded
                inputProperties,
                outputType,
                description);
        this.inputChangelogMode = inputChangelogMode;
        this.upsertMaterialize = upsertMaterialize;
        this.inputUpsertKey = inputUpsertKey;
        this.stateMetadataList = stateMetadataList;
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 将当前执行节点内部转换为计划的一部分。
     */
    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<Object> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        // 获取当前节点的第一个输入边
        final ExecEdge inputEdge = getInputEdges().get(0);
        // 将输入边的配置转换为Flink的Transformation对象，这里假设输入类型是RowData
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        // 获取输入边的输出类型，这里假设是RowType
        final RowType inputRowType = (RowType) inputEdge.getOutputType();
        // 根据计划器的Flink上下文获取动态表接收器
        final DynamicTableSink tableSink = tableSinkSpec.getTableSink(planner.getFlinkContext());
        // 判断动态表接收器是否为CollectDynamicSink类型
        final boolean isCollectSink = tableSink instanceof CollectDynamicSink;
        // 检查是否禁用了行时间插入器，这可能会影响行时间字段的处理
        final boolean isDisabled =
                config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_ROWTIME_INSERTER)
                        == RowtimeInserter.DISABLED;
        // 初始化一个列表，用于存储行时间字段的索引
        final List<Integer> rowtimeFieldIndices = new ArrayList<>();
        // 遍历输入行的所有字段，查找行时间字段并记录其索引
        for (int i = 0; i < inputRowType.getFieldCount(); ++i) {
            if (TypeCheckUtils.isRowTime(inputRowType.getTypeAt(i))) {
                rowtimeFieldIndices.add(i);
            }
        }
        // 声明一个整型变量来存储行时间字段的索引
        final int rowtimeFieldIndex;
        // 如果是收集接收器（通常用于测试或调试）或行时间插入器被禁用，则行时间字段索引设置为-1
        if (isCollectSink || isDisabled) {
            rowtimeFieldIndex = -1;
            // 如果存在多个行时间字段，则抛出异常，因为写入表接收器时只能有一个行时间字段
        } else if (rowtimeFieldIndices.size() > 1) {
            throw new TableException(
                    String.format(
                            "The query contains more than one rowtime attribute column [%s] for writing into table '%s'.\n"
                                    + "Please select the column that should be used as the event-time timestamp "
                                    + "for the table sink by casting all other columns to regular TIMESTAMP or TIMESTAMP_LTZ.",
                            rowtimeFieldIndices.stream()
                                    .map(i -> inputRowType.getFieldNames().get(i))
                                    .collect(Collectors.joining(", ")),
                            tableSinkSpec
                                    .getContextResolvedTable()
                                    .getIdentifier()
                                    .asSummaryString()));
            // 如果只有一个行时间字段，则设置行时间字段索引为该字段的索引
        } else if (rowtimeFieldIndices.size() == 1) {
            rowtimeFieldIndex = rowtimeFieldIndices.get(0);
            // 如果没有行时间字段，则行时间字段索引也设置为-1
        } else {
            rowtimeFieldIndex = -1;
        }
       // 调用方法创建并返回包含接收器逻辑的Transformation对象
       // 该方法根据Flink执行环境、配置、类加载器、输入Transformation、表接收器、行时间字段索引等参数来构建
        return createSinkTransformation(
                planner.getExecEnv(),
                config,
                planner.getFlinkContext().getClassLoader(),
                inputTransform,
                tableSink,
                rowtimeFieldIndex,
                upsertMaterialize,
                inputUpsertKey);
    }

    @Override
    protected Transformation<RowData> applyUpsertMaterialize(
            Transformation<RowData> inputTransform,
            int[] primaryKeys,
            int sinkParallelism,
            ExecNodeConfig config,
            ClassLoader classLoader,
            RowType physicalRowType,
            int[] inputUpsertKey) {
        final GeneratedRecordEqualiser rowEqualiser =
                new EqualiserCodeGenerator(physicalRowType, classLoader)
                        .generateRecordEqualiser("SinkMaterializeEqualiser");
        final GeneratedRecordEqualiser upsertKeyEqualiser =
                inputUpsertKey == null
                        ? null
                        : new EqualiserCodeGenerator(
                                        RowTypeUtils.projectRowType(
                                                physicalRowType, inputUpsertKey),
                                        classLoader)
                                .generateRecordEqualiser("SinkMaterializeUpsertKeyEqualiser");

        final long stateRetentionTime =
                StateMetadata.getStateTtlForOneInputOperator(config, stateMetadataList);

        SinkUpsertMaterializer operator =
                new SinkUpsertMaterializer(
                        StateConfigUtil.createTtlConfig(stateRetentionTime),
                        InternalSerializers.create(physicalRowType),
                        rowEqualiser,
                        upsertKeyEqualiser,
                        inputUpsertKey);
        final String[] fieldNames = physicalRowType.getFieldNames().toArray(new String[0]);
        final List<String> pkFieldNames =
                Arrays.stream(primaryKeys)
                        .mapToObj(idx -> fieldNames[idx])
                        .collect(Collectors.toList());

        OneInputTransformation<RowData, RowData> materializeTransform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(
                                UPSERT_MATERIALIZE_TRANSFORMATION,
                                String.format(
                                        "SinkMaterializer(pk=[%s])",
                                        String.join(", ", pkFieldNames)),
                                "SinkMaterializer",
                                config),
                        operator,
                        inputTransform.getOutputType(),
                        sinkParallelism,
                        sinkParallelismConfigured);
        RowDataKeySelector keySelector =
                KeySelectorUtil.getRowDataSelector(
                        classLoader, primaryKeys, InternalTypeInfo.of(physicalRowType));
        materializeTransform.setStateKeySelector(keySelector);
        materializeTransform.setStateKeyType(keySelector.getProducedType());
        return materializeTransform;
    }
}
