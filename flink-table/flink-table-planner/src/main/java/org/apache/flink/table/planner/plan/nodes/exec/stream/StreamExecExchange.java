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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty.HashDistribution;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecExchange;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * This {@link ExecNode} represents a change of partitioning of the input elements for stream.
 *
 * <p>TODO Remove this class once FLINK-21224 is finished.
 */
@ExecNodeMetadata(
        name = "stream-exec-exchange",
        version = 1,
        producedTransformations = StreamExecExchange.EXCHANGE_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecExchange extends CommonExecExchange implements StreamExecNode<RowData> {

    public static final String EXCHANGE_TRANSFORMATION = "exchange";

    public StreamExecExchange(
            ReadableConfig tableConfig,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecExchange.class),
                ExecNodeContext.newPersistedConfig(StreamExecExchange.class, tableConfig),
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecExchange(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
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
        // 获取输入边的第一个Transformation，作为当前节点的输入
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);
        // 初始化分区器和并行度
        final StreamPartitioner<RowData> partitioner;
        final int parallelism;
        // 获取输入属性的第一个，并判断其分布类型
        final InputProperty inputProperty = getInputProperties().get(0);
        final InputProperty.DistributionType distributionType =
                inputProperty.getRequiredDistribution().getType();
        switch (distributionType) {
            case SINGLETON:// 单例分布
                partitioner = new GlobalPartitioner<>();// 使用全局分区器
                parallelism = 1;// 设置并行度为1
                break;
            case HASH:// 哈希分布
                // TODO Eliminate duplicate keys
                // TODO 消除重复键（需要额外的逻辑来确保键的唯一性）
                int[] keys = ((HashDistribution) inputProperty.getRequiredDistribution()).getKeys();
                InternalTypeInfo<RowData> inputType =
                        (InternalTypeInfo<RowData>) inputTransform.getOutputType();
                RowDataKeySelector keySelector =
                        KeySelectorUtil.getRowDataSelector(// 创建键选择器
                                planner.getFlinkContext().getClassLoader(), keys, inputType);
                partitioner =
                        new KeyGroupStreamPartitioner<>(// 使用键组流分区器
                                keySelector, DEFAULT_LOWER_BOUND_MAX_PARALLELISM);
                parallelism = ExecutionConfig.PARALLELISM_DEFAULT;
                break;
            default:// 如果分布类型不支持，则抛出异常
                throw new TableException(
                        String.format("%s is not supported now!", distributionType));
        }
        // 创建PartitionTransformation，这是一个包含分区逻辑的Transformation
        final Transformation<RowData> transformation =
                new PartitionTransformation<>(inputTransform, partitioner);
        // 填充Transformation的元数据，例如名称和配置
        createTransformationMeta(EXCHANGE_TRANSFORMATION, config).fill(transformation);
        // 设置Transformation的并行度
        transformation.setParallelism(parallelism);
        // 设置Transformation的输出类型
        transformation.setOutputType(InternalTypeInfo.of(getOutputType()));
        // 返回转换后的Transformation
        return transformation;
    }
}
