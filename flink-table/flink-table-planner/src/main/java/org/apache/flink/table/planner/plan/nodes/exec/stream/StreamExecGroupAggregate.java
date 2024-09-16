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
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.plan.utils.AggregateUtil;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.plan.utils.MinibatchUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.GroupAggFunction;
import org.apache.flink.table.runtime.operators.aggregate.MiniBatchGroupAggFunction;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Stream {@link ExecNode} for unbounded group aggregate.
 *
 * <p>This node does support un-splittable aggregate function (e.g. STDDEV_POP).
 */
@ExecNodeMetadata(
        name = "stream-exec-group-aggregate",
        version = 1,
        consumedOptions = {"table.exec.mini-batch.enabled", "table.exec.mini-batch.size"},
        producedTransformations = StreamExecGroupAggregate.GROUP_AGGREGATE_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecGroupAggregate extends StreamExecAggregateBase {

    private static final Logger LOG = LoggerFactory.getLogger(StreamExecGroupAggregate.class);

    public static final String GROUP_AGGREGATE_TRANSFORMATION = "group-aggregate";

    public static final String STATE_NAME = "groupAggregateState";

    @JsonProperty(FIELD_NAME_GROUPING)
    private final int[] grouping;

    @JsonProperty(FIELD_NAME_AGG_CALLS)
    private final AggregateCall[] aggCalls;

    /** Each element indicates whether the corresponding agg call needs `retract` method. */
    @JsonProperty(FIELD_NAME_AGG_CALL_NEED_RETRACTIONS)
    private final boolean[] aggCallNeedRetractions;

    /** Whether this node will generate UPDATE_BEFORE messages. */
    @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE)
    private final boolean generateUpdateBefore;

    /** Whether this node consumes retraction messages. */
    @JsonProperty(FIELD_NAME_NEED_RETRACTION)
    private final boolean needRetraction;

    @Nullable
    @JsonProperty(FIELD_NAME_STATE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<StateMetadata> stateMetadataList;

    public StreamExecGroupAggregate(
            ReadableConfig tableConfig,
            int[] grouping,
            AggregateCall[] aggCalls,
            boolean[] aggCallNeedRetractions,
            boolean generateUpdateBefore,
            boolean needRetraction,
            @Nullable Long stateTtlFromHint,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecGroupAggregate.class),
                ExecNodeContext.newPersistedConfig(StreamExecGroupAggregate.class, tableConfig),
                grouping,
                aggCalls,
                aggCallNeedRetractions,
                generateUpdateBefore,
                needRetraction,
                StateMetadata.getOneInputOperatorDefaultMeta(
                        stateTtlFromHint, tableConfig, STATE_NAME),
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecGroupAggregate(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_GROUPING) int[] grouping,
            @JsonProperty(FIELD_NAME_AGG_CALLS) AggregateCall[] aggCalls,
            @JsonProperty(FIELD_NAME_AGG_CALL_NEED_RETRACTIONS) boolean[] aggCallNeedRetractions,
            @JsonProperty(FIELD_NAME_GENERATE_UPDATE_BEFORE) boolean generateUpdateBefore,
            @JsonProperty(FIELD_NAME_NEED_RETRACTION) boolean needRetraction,
            @Nullable @JsonProperty(FIELD_NAME_STATE) List<StateMetadata> stateMetadataList,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.grouping = checkNotNull(grouping);
        this.aggCalls = checkNotNull(aggCalls);
        this.aggCallNeedRetractions = checkNotNull(aggCallNeedRetractions);
        checkArgument(aggCalls.length == aggCallNeedRetractions.length);
        this.generateUpdateBefore = generateUpdateBefore;
        this.needRetraction = needRetraction;
        this.stateMetadataList = stateMetadataList;
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 将当前节点转换为执行计划中的Transformation。
     */
    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        // 根据配置和状态元数据列表获取状态保留时间
        final long stateRetentionTime =
                StateMetadata.getStateTtlForOneInputOperator(config, stateMetadataList);
        // 如果分组键存在但状态保留时间未配置（即小于0），则发出警告
        if (grouping.length > 0 && stateRetentionTime < 0) {
            LOG.warn(
                    "No state retention interval configured for a query which accumulates state. "
                            + "Please provide a query configuration with valid retention interval to prevent excessive "
                            + "state size. You may specify a retention time of 0 to not clean up the state.");
        }
        // 获取输入边的第一个Transformation
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        // 获取输入边的输出类型，即RowType
        final RowType inputRowType = (RowType) inputEdge.getOutputType();
        // 创建聚合处理器代码生成器
        // 聚合处理器代码生成器根据配置、Flink上下文类加载器、输入RowType的子类型等信息来生成聚合逻辑的代码
        final AggsHandlerCodeGenerator generator =
                new AggsHandlerCodeGenerator(
                                new CodeGeneratorContext(
                                        config, planner.getFlinkContext().getClassLoader()),
                                planner.createRelBuilder(),
                                JavaScalaConversionUtil.toScala(inputRowType.getChildren()),
                                // TODO: heap state backend do not copy key currently,
                                //  we have to copy input field
                                // TODO: copy is not need when state backend is rocksdb,
                                //  improve this in future
                                // TODO: but other operators do not copy this input field.....
                                true)
                        .needAccumulate();
        // 如果需要撤回（retraction）机制（如处理更新前和更新后的值），则调用needRetract()方法
        if (needRetraction) {
            generator.needRetract();
        }
        // 将聚合调用转换为流聚合信息列表
        final AggregateInfoList aggInfoList =
                AggregateUtil.transformToStreamAggregateInfoList(
                        planner.getTypeFactory(),// 类型工厂，用于创建和转换类型
                        inputRowType, // 输入行的类型
                        JavaScalaConversionUtil.toScala(Arrays.asList(aggCalls)),// 聚合调用的Java列表转换为Scala列表
                        aggCallNeedRetractions,// 聚合调用是否需要撤回信息
                        needRetraction,// 当前聚合操作是否需要撤回机制
                        true,// 是否考虑时间属性（如窗口聚合）
                        true); // 是否生成累积器（accumulate）
        // 使用聚合信息列表生成聚合处理器函数
        final GeneratedAggsHandleFunction aggsHandler =
                generator.generateAggsHandler("GroupAggsHandler", aggInfoList);
        // 将累积器类型（数据类型）转换为逻辑类型
        final LogicalType[] accTypes =
                Arrays.stream(aggInfoList.getAccTypes())
                        .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
                        .toArray(LogicalType[]::new);
        // 将实际值类型（数据类型）转换为逻辑类型
        final LogicalType[] aggValueTypes =
                Arrays.stream(aggInfoList.getActualValueTypes())
                        .map(LogicalTypeDataTypeConverter::fromDataTypeToLogicalType)
                        .toArray(LogicalType[]::new);
        // 生成记录等值比较器
        final GeneratedRecordEqualiser recordEqualiser =
                new EqualiserCodeGenerator(
                                aggValueTypes, planner.getFlinkContext().getClassLoader())
                        .generateRecordEqualiser("GroupAggValueEqualiser");
        // 获取COUNT(*)在聚合信息列表中的索引
        final int inputCountIndex = aggInfoList.getIndexOfCountStar();
        // 判断是否启用微批处理
        final boolean isMiniBatchEnabled = MinibatchUtil.isMiniBatchEnabled(config);
         // 根据是否启用微批处理，选择不同的操作符实现
        final OneInputStreamOperator<RowData, RowData> operator;
        if (isMiniBatchEnabled) {
            // 如果启用微批处理，则使用MiniBatchGroupAggFunction
            MiniBatchGroupAggFunction aggFunction =
                    new MiniBatchGroupAggFunction(
                            aggsHandler,
                            recordEqualiser,
                            accTypes,
                            inputRowType,
                            inputCountIndex,
                            generateUpdateBefore,
                            stateRetentionTime);
            operator =
                    new KeyedMapBundleOperator<>(
                            aggFunction, MinibatchUtil.createMiniBatchTrigger(config));
        } else {
            // 如果未启用微批处理，则使用GroupAggFunction
            GroupAggFunction aggFunction =
                    new GroupAggFunction(
                            aggsHandler,
                            recordEqualiser,
                            accTypes,
                            inputCountIndex,
                            generateUpdateBefore,
                            stateRetentionTime);
            operator = new KeyedProcessOperator<>(aggFunction);
        }

        // partitioned aggregation
        // 创建一个用于分组聚合的OneInputTransformation
        final OneInputTransformation<RowData, RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(GROUP_AGGREGATE_TRANSFORMATION, config),
                        operator,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism(),
                        false);

        // set KeyType and Selector for state
        // 设置状态键的类型和选择器
        // 状态键用于在状态后端中唯一标识每个键的状态
        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(
                        planner.getFlinkContext().getClassLoader(),
                        grouping,
                        InternalTypeInfo.of(inputRowType));
        // 设置状态键选择器，用于从输入数据中提取键
        transform.setStateKeySelector(selector);
        // 设置状态键的类型，即分组键的类型
        transform.setStateKeyType(selector.getProducedType());
         // 返回构建好的转换
        return transform;
    }
}
