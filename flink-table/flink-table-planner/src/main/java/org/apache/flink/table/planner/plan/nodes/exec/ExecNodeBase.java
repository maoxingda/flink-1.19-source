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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.fusion.OpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.nodes.exec.serde.ConfigurationJsonSerializerFilter;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.ExecNodeVisitor;
import org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JacksonInject;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for {@link ExecNode}.
 *
 * @param <T> The type of the elements that result from this node.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class ExecNodeBase<T> implements ExecNode<T> {

    /**
     * The default value of this flag is false. Other cases must set this flag accordingly via
     * {@link #setCompiled(boolean)}. It is not exposed via a constructor arg to avoid complex
     * constructor overloading for all {@link ExecNode}s. However, during deserialization this flag
     * will always be set to true.
     */
    @JacksonInject("isDeserialize")
    private boolean isCompiled;

    private final String description;

    private final LogicalType outputType;

    private final List<InputProperty> inputProperties;

    private List<ExecEdge> inputEdges;

    private transient Transformation<T> transformation;

    private @Nullable transient OpFusionCodegenSpecGenerator fusionCodegenSpecGenerator;

    /** Holds the context information (id, name, version) as deserialized from a JSON plan. */
    @JsonProperty(value = FIELD_NAME_TYPE, access = JsonProperty.Access.WRITE_ONLY)
    private final ExecNodeContext context;

    /**
     * Retrieves the default context from the {@link ExecNodeMetadata} annotation to be serialized
     * into the JSON plan.
     */
    @JsonProperty(value = FIELD_NAME_TYPE, access = JsonProperty.Access.READ_ONLY, index = 1)
    protected final ExecNodeContext getContextFromAnnotation() {
        return isCompiled ? context : ExecNodeContext.newContext(this.getClass()).withId(getId());
    }

    @JsonProperty(value = FIELD_NAME_CONFIGURATION, access = JsonProperty.Access.WRITE_ONLY)
    private final ReadableConfig persistedConfig;

    @JsonProperty(
            value = FIELD_NAME_CONFIGURATION,
            access = JsonProperty.Access.READ_ONLY,
            index = 2)
    // Custom filter to exclude node configuration if no consumed options are used
    @JsonInclude(
            value = JsonInclude.Include.CUSTOM,
            valueFilter = ConfigurationJsonSerializerFilter.class)
    public ReadableConfig getPersistedConfig() {
        return persistedConfig;
    }

    protected ExecNodeBase(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            List<InputProperty> inputProperties,
            LogicalType outputType,
            String description) {
        this.context = checkNotNull(context).withId(id);
        this.persistedConfig = persistedConfig == null ? new Configuration() : persistedConfig;
        this.inputProperties = checkNotNull(inputProperties);
        this.outputType = checkNotNull(outputType);
        this.description = checkNotNull(description);
    }

    @Override
    public final int getId() {
        return context.getId();
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public LogicalType getOutputType() {
        return outputType;
    }

    @Override
    public List<InputProperty> getInputProperties() {
        return inputProperties;
    }

    @Override
    public List<ExecEdge> getInputEdges() {
        return checkNotNull(
                inputEdges,
                "inputEdges should not null, please call `setInputEdges(List<ExecEdge>)` first.");
    }

    @Override
    public void setInputEdges(List<ExecEdge> inputEdges) {
        checkNotNull(inputEdges, "inputEdges should not be null.");
        this.inputEdges = new ArrayList<>(inputEdges);
    }

    @Override
    public void replaceInputEdge(int index, ExecEdge newInputEdge) {
        List<ExecEdge> edges = getInputEdges();
        checkArgument(index >= 0 && index < edges.size());
        edges.set(index, newInputEdge);
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 将ExecNode转换为Transformation
     */
    @Override
    public final Transformation<T> translateToPlan(Planner planner) {
        // 如果transformation还未被初始化，则进行初始化
        if (transformation == null) {
            transformation =
                    translateToPlanInternal(
                            (PlannerBase) planner,// 将planner强制转换为PlannerBase类型，并传入translateToPlanInternal方法
                            ExecNodeConfig.of(// 创建ExecNodeConfig对象，传入planner的表配置、持久化配置和编译状态
                                    ((PlannerBase) planner).getTableConfig(),
                                    persistedConfig,
                                    isCompiled));
            // 如果当前对象是SingleTransformationTranslator的实例
            if (this instanceof SingleTransformationTranslator) {
                // 并且transformation包含单例输入
                if (inputsContainSingleton(transformation)) {
                    // 将transformation的并行度和最大并行度设置为1
                    transformation.setParallelism(1);
                    transformation.setMaxParallelism(1);
                }
            }
        }
        return transformation;// 返回transformation对象
    }

    @Override
    public void accept(ExecNodeVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public void setCompiled(boolean compiled) {
        isCompiled = compiled;
    }

    /**
     * Internal method, translates this node into a Flink operator.
     *
     * @param planner The planner.
     * @param config per-{@link ExecNode} configuration that contains the merged configuration from
     *     various layers which all the nodes implementing this method should use, instead of
     *     retrieving configuration from the {@code planner}. For more details check {@link
     *     ExecNodeConfig}.
     */
    protected abstract Transformation<T> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config);

    private boolean inputsContainSingleton(Transformation<T> transformation) {
        return inputsContainSingleton()
                || transformation.getInputs().stream()
                        .anyMatch(
                                input ->
                                        input instanceof PartitionTransformation
                                                && ((PartitionTransformation<?>) input)
                                                                .getPartitioner()
                                                        instanceof GlobalPartitioner);
    }

    /** Whether singleton distribution is required. */
    protected boolean inputsContainSingleton() {
        return getInputProperties().stream()
                .anyMatch(
                        p ->
                                p.getRequiredDistribution().getType()
                                        == InputProperty.DistributionType.SINGLETON);
    }

    @JsonIgnore
    protected String getSimplifiedName() {
        return getClass().getSimpleName().replace("StreamExec", "").replace("BatchExec", "");
    }

    protected String createTransformationUid(String operatorName, ExecNodeConfig config) {
        return context.generateUid(operatorName, config);
    }

    protected String createTransformationName(ReadableConfig config) {
        return createFormattedTransformationName(getDescription(), getSimplifiedName(), config);
    }

    protected String createTransformationDescription(ReadableConfig config) {
        return createFormattedTransformationDescription(getDescription(), config);
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 创建转换的元数据。
     *
     * @param operatorName 操作符的名称，用于在需要时生成唯一标识符（UID）。
     * @param config 执行节点配置，包含了是否应该设置UID以及其他可能的配置信息。
     * @return 返回创建的转换元数据对象。
     */
    protected TransformationMetadata createTransformationMeta(
            String operatorName, ExecNodeConfig config) {
        // 检查当前类是否被标记为不支持或者配置指示不应设置UID
        if (ExecNodeMetadataUtil.isUnsupported(this.getClass()) || !config.shouldSetUid()) {
            // 如果不支持或者不应设置UID，则只使用配置信息来创建基本的转换元数据
            // 包括转换的名称和描述
            return new TransformationMetadata(
                    createTransformationName(config), createTransformationDescription(config));
        } else {
            // 如果支持且配置指示应设置UID，则除了名称和描述外，还使用操作符名称和配置信息来生成UID
            // 并创建包含UID、名称和描述的完整转换元数据
            return new TransformationMetadata(
                    createTransformationUid(operatorName, config),
                    createTransformationName(config),
                    createTransformationDescription(config));
        }
    }

    protected TransformationMetadata createTransformationMeta(
            String operatorName, String detailName, String simplifiedName, ExecNodeConfig config) {
        final String name = createFormattedTransformationName(detailName, simplifiedName, config);
        final String desc = createFormattedTransformationDescription(detailName, config);
        if (ExecNodeMetadataUtil.isUnsupported(this.getClass()) || !config.shouldSetUid()) {
            return new TransformationMetadata(name, desc);
        } else {
            return new TransformationMetadata(
                    createTransformationUid(operatorName, config), name, desc);
        }
    }

    protected String createFormattedTransformationDescription(
            String description, ReadableConfig config) {
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_SIMPLIFY_OPERATOR_NAME_ENABLED)) {
            return String.format("[%d]:%s", getId(), description);
        }
        return description;
    }

    protected String createFormattedTransformationName(
            String detailName, String simplifiedName, ReadableConfig config) {
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_SIMPLIFY_OPERATOR_NAME_ENABLED)) {
            return String.format("%s[%d]", simplifiedName, getId());
        }
        return detailName;
    }

    @VisibleForTesting
    @JsonIgnore
    public Transformation<T> getTransformation() {
        return this.transformation;
    }

    @Override
    public boolean supportFusionCodegen() {
        return false;
    }

    @Override
    public OpFusionCodegenSpecGenerator translateToFusionCodegenSpec(
            Planner planner, CodeGeneratorContext parentCtx) {
        if (fusionCodegenSpecGenerator == null) {
            fusionCodegenSpecGenerator =
                    translateToFusionCodegenSpecInternal(
                            (PlannerBase) planner,
                            ExecNodeConfig.of(
                                    ((PlannerBase) planner).getTableConfig(),
                                    persistedConfig,
                                    isCompiled),
                            parentCtx);
        }

        return fusionCodegenSpecGenerator;
    }

    /**
     * Internal method, translates this node into a operator codegen spec generator.
     *
     * @param planner The planner.
     * @param config per-{@link ExecNode} configuration that contains the merged configuration from
     *     various layers which all the nodes implementing this method should use, instead of
     *     retrieving configuration from the {@code planner}. For more details check {@link
     *     ExecNodeConfig}.
     */
    protected OpFusionCodegenSpecGenerator translateToFusionCodegenSpecInternal(
            PlannerBase planner, ExecNodeConfig config, CodeGeneratorContext parentCtx) {
        throw new TableException("This ExecNode doesn't support operator fusion codegen now.");
    }
}
