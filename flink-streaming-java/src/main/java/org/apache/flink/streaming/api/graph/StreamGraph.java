/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.JobStatusHook;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.OutputFormatOperatorFactory;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.partitioner.ForwardForConsecutiveHashPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardForUnspecifiedPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.tasks.MultipleInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamIterationTail;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.flink.util.OutputTag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Class representing the streaming topology. It contains all the information necessary to build the
 * jobgraph for the execution.
 */
/**
  * @授课老师(V): yi_locus
  * email: 156184212@qq.com
  * 流拓扑的类。它包含为执行构建作业图所需的所有信息。
  */
@Internal
public class StreamGraph implements Pipeline {

    private static final Logger LOG = LoggerFactory.getLogger(StreamGraph.class);

    public static final String ITERATION_SOURCE_NAME_PREFIX = "IterationSource";

    public static final String ITERATION_SINK_NAME_PREFIX = "IterationSink";

    /** job名字 */
    private String jobName;

    /** 配置文件 */
    private final Configuration jobConfiguration;
    /** ExecutionConfig配置文件 */
    private final ExecutionConfig executionConfig;
    /** CheckpointConfig配置文件 */
    private final CheckpointConfig checkpointConfig;
    private SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.none();

    /** 时间语义 */
    private TimeCharacteristic timeCharacteristic;

    /**数据交换模式*/
    /**
     * 数据交换模式
     * BLOCKING:阻塞分区表示阻塞数据交换，其中数据流首先被完全产生，然后被消耗。
     * PIPELINED_BOUNDED:带有一个有限大小的本地缓冲池。对于流计算作业来说，固定大小的缓冲池可以避免缓冲太多的数据和检查点延迟太久。
     */

    private GlobalStreamExchangeMode globalExchangeMode;

    /** .14 开始 Flink 支持在部分任务结束后继续进行Checkpoint。 */
    private boolean enableCheckpointsAfterTasksFinish;

    /** Flag to indicate whether to put all vertices into the same slot sharing group by default. */
    /**
     * 用于指示默认情况下是否将所有顶点放入同一槽共享组的标志
     */
    private boolean allVerticesInSameSlotSharingGroupByDefault = true;

    /**
     * 存放StreamNode(transformationId,StreamNode)
     */
    private Map<Integer, StreamNode> streamNodes;
    private Set<Integer> sources;
    private Set<Integer> sinks;
    /**
     * 用来存储虚拟的侧输出流
     */
    private Map<Integer, Tuple2<Integer, OutputTag>> virtualSideOutputNodes;
    /**
     * 用来存储虚拟的Nodes(虚拟生成的Id,Tuple3<对应的输入id, 以及分区策略<?>)
     */
    private Map<Integer, Tuple3<Integer, StreamPartitioner<?>, StreamExchangeMode>>
            virtualPartitionNodes;

    protected Map<Integer, String> vertexIDtoBrokerID;
    protected Map<Integer, Long> vertexIDtoLoopTimeout;
    /** 状态后端*/
    private StateBackend stateBackend;
    /** 该类用来存储检查点、以及容错回复*/
    private CheckpointStorage checkpointStorage;
    private Set<Tuple2<StreamNode, StreamNode>> iterationSourceSinkPairs;
    private InternalTimeServiceManager.Provider timerServiceProvider;
    /** JobType 类型*/
    private JobType jobType = JobType.STREAMING;
    private Map<String, ResourceProfile> slotSharingGroupResources;
    private PipelineOptions.VertexDescriptionMode descriptionMode =
            PipelineOptions.VertexDescriptionMode.TREE;
    private boolean vertexNameIncludeIndexPrefix = false;

    private final List<JobStatusHook> jobStatusHooks = new ArrayList<>();

    private boolean dynamic;

    private boolean autoParallelismEnabled;

    public StreamGraph(
            Configuration jobConfiguration,
            ExecutionConfig executionConfig,
            CheckpointConfig checkpointConfig,
            SavepointRestoreSettings savepointRestoreSettings) {
        this.jobConfiguration = new Configuration(checkNotNull(jobConfiguration));
        this.executionConfig = checkNotNull(executionConfig);
        this.checkpointConfig = checkNotNull(checkpointConfig);
        this.savepointRestoreSettings = checkNotNull(savepointRestoreSettings);

        // create an empty new stream graph.
        clear();
    }

    /** Remove all registered nodes etc. */
    public void clear() {
        streamNodes = new HashMap<>();
        virtualSideOutputNodes = new HashMap<>();
        virtualPartitionNodes = new HashMap<>();
        vertexIDtoBrokerID = new HashMap<>();
        vertexIDtoLoopTimeout = new HashMap<>();
        iterationSourceSinkPairs = new HashSet<>();
        sources = new HashSet<>();
        sinks = new HashSet<>();
        slotSharingGroupResources = new HashMap<>();
    }

    public ExecutionConfig getExecutionConfig() {
        return executionConfig;
    }

    public Configuration getJobConfiguration() {
        return jobConfiguration;
    }

    public CheckpointConfig getCheckpointConfig() {
        return checkpointConfig;
    }

    public void setSavepointRestoreSettings(SavepointRestoreSettings savepointRestoreSettings) {
        this.savepointRestoreSettings = savepointRestoreSettings;
    }

    public SavepointRestoreSettings getSavepointRestoreSettings() {
        return savepointRestoreSettings;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public void setStateBackend(StateBackend backend) {
        this.stateBackend = backend;
    }

    public StateBackend getStateBackend() {
        return this.stateBackend;
    }

    public void setCheckpointStorage(CheckpointStorage checkpointStorage) {
        this.checkpointStorage = checkpointStorage;
    }

    public CheckpointStorage getCheckpointStorage() {
        return this.checkpointStorage;
    }

    public InternalTimeServiceManager.Provider getTimerServiceProvider() {
        return timerServiceProvider;
    }

    public void setTimerServiceProvider(InternalTimeServiceManager.Provider timerServiceProvider) {
        this.timerServiceProvider = checkNotNull(timerServiceProvider);
    }

    public Collection<Tuple2<String, DistributedCache.DistributedCacheEntry>> getUserArtifacts() {
        return Optional.ofNullable(jobConfiguration.get(PipelineOptions.CACHED_FILES))
                .map(DistributedCache::parseCachedFilesFromString)
                .orElse(new ArrayList<>());
    }

    public TimeCharacteristic getTimeCharacteristic() {
        return timeCharacteristic;
    }

    public void setTimeCharacteristic(TimeCharacteristic timeCharacteristic) {
        this.timeCharacteristic = timeCharacteristic;
    }

    public GlobalStreamExchangeMode getGlobalStreamExchangeMode() {
        return globalExchangeMode;
    }

    public void setGlobalStreamExchangeMode(GlobalStreamExchangeMode globalExchangeMode) {
        this.globalExchangeMode = globalExchangeMode;
    }

    public void setSlotSharingGroupResource(
            Map<String, ResourceProfile> slotSharingGroupResources) {
        this.slotSharingGroupResources.putAll(slotSharingGroupResources);
    }

    public Optional<ResourceProfile> getSlotSharingGroupResource(String groupId) {
        return Optional.ofNullable(slotSharingGroupResources.get(groupId));
    }

    public boolean hasFineGrainedResource() {
        return slotSharingGroupResources.values().stream()
                .anyMatch(resourceProfile -> !resourceProfile.equals(ResourceProfile.UNKNOWN));
    }

    /**
     * Set whether to put all vertices into the same slot sharing group by default.
     *
     * @param allVerticesInSameSlotSharingGroupByDefault indicates whether to put all vertices into
     *     the same slot sharing group by default.
     */
    public void setAllVerticesInSameSlotSharingGroupByDefault(
            boolean allVerticesInSameSlotSharingGroupByDefault) {
        this.allVerticesInSameSlotSharingGroupByDefault =
                allVerticesInSameSlotSharingGroupByDefault;
    }

    /**
     * Gets whether to put all vertices into the same slot sharing group by default.
     *
     * @return whether to put all vertices into the same slot sharing group by default.
     */
    public boolean isAllVerticesInSameSlotSharingGroupByDefault() {
        return allVerticesInSameSlotSharingGroupByDefault;
    }

    public boolean isEnableCheckpointsAfterTasksFinish() {
        return enableCheckpointsAfterTasksFinish;
    }

    public void setEnableCheckpointsAfterTasksFinish(boolean enableCheckpointsAfterTasksFinish) {
        this.enableCheckpointsAfterTasksFinish = enableCheckpointsAfterTasksFinish;
    }

    // Checkpointing

    public boolean isChainingEnabled() {
        return jobConfiguration.get(PipelineOptions.OPERATOR_CHAINING);
    }

    public boolean isChainingOfOperatorsWithDifferentMaxParallelismEnabled() {
        return jobConfiguration.get(
                PipelineOptions.OPERATOR_CHAINING_CHAIN_OPERATORS_WITH_DIFFERENT_MAX_PARALLELISM);
    }

    public boolean isIterative() {
        return !vertexIDtoLoopTimeout.isEmpty();
    }

    public <IN, OUT> void addSource(
            Integer vertexID,
            @Nullable String slotSharingGroup,
            @Nullable String coLocationGroup,
            SourceOperatorFactory<OUT> operatorFactory,
            TypeInformation<IN> inTypeInfo,
            TypeInformation<OUT> outTypeInfo,
            String operatorName) {
        addOperator(
                vertexID,
                slotSharingGroup,
                coLocationGroup,
                operatorFactory,
                inTypeInfo,
                outTypeInfo,
                operatorName,
                SourceOperatorStreamTask.class);
        sources.add(vertexID);
    }

    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * 添加数据源类型的StreamNode
      * @param vertexID transformation
      * @param slotSharingGroup slot 共享组
      * @param coLocationGroup null
      * @param operatorFactory StreamOperator
      * @param inTypeInfo 输入类型类型
      * @param outTypeInfo 输出类型类型
      * @param operatorName operator名字
      * @param <IN>
      * @param <OUT>
      */
    public <IN, OUT> void addLegacySource(
            Integer vertexID,
            @Nullable String slotSharingGroup,
            @Nullable String coLocationGroup,
            StreamOperatorFactory<OUT> operatorFactory,
            TypeInformation<IN> inTypeInfo,
            TypeInformation<OUT> outTypeInfo,
            String operatorName) {
        /**
         * addOperator方法内部就会构建StreamNode
         */
        addOperator(
                vertexID,
                slotSharingGroup,
                coLocationGroup,
                operatorFactory,
                inTypeInfo,
                outTypeInfo,
                operatorName);
        /**
         * 将来vertexId 放入Set<Integer> sources中
         */
        sources.add(vertexID);
    }
    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * 添加一个输出（sink）操作
     * @param vertexID: 流的唯一标识符，通常在流图中表示一个节点或操作。
     * @param slotSharingGroup: 一个可选的字符串，用于指定哪些操作可以共享相同的任务槽。
     * @param  coLocationGroup: 一个可选的字符串，用于指定操作的协同定位组，以便它们可以在同一任务管理器上运行。
     * @param operatorFactory: 一个工厂对象，用于创建输出操作。
     * @param inTypeInfo: 输入数据的类型信息。
     * @param outTypeInfo: 输出数据的类型信息。
     * @param operatorName: 操作的名称。
      */

    public <IN, OUT> void addSink(
            Integer vertexID,
            @Nullable String slotSharingGroup,
            @Nullable String coLocationGroup,
            StreamOperatorFactory<OUT> operatorFactory,
            TypeInformation<IN> inTypeInfo,
            TypeInformation<OUT> outTypeInfo,
            String operatorName) {
        /**
         *调用addOperator方法（该方法在代码中未给出），以将操作添加到流图中。
         * 它传递了所有必要的参数，包括顶点ID、槽共享组、协同定位组、操作工厂、输入和输出类型信息以及操作名称。
         */
        addOperator(
                vertexID,
                slotSharingGroup,
                coLocationGroup,
                operatorFactory,
                inTypeInfo,
                outTypeInfo,
                operatorName);
        /**
         * 检查operatorFactory是否是OutputFormatOperatorFactory的实例。如果是，则调用setOutputFormat方法
         */
        if (operatorFactory instanceof OutputFormatOperatorFactory) {
            setOutputFormat(
                    vertexID, ((OutputFormatOperatorFactory) operatorFactory).getOutputFormat());
        }
        /**
         * 新添加的sink的顶点ID添加到sinks列表中
         */
        sinks.add(vertexID);
    }

    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * 很重要的一个方法内部做了两件事
      * 1.根据当前Transformtion获取对应的StreamTask。StreamTask 在Task线程启动任务的时候会被处理。进而构造StreamOperator实行open、setup方法等
      * 2.调用 addOperator重载方法构建StreamNode
      */
    public <IN, OUT> void addOperator(
            Integer vertexID,
            @Nullable String slotSharingGroup,
            @Nullable String coLocationGroup,
            StreamOperatorFactory<OUT> operatorFactory,
            TypeInformation<IN> inTypeInfo,
            TypeInformation<OUT> outTypeInfo,
            String operatorName) {
        /**
         * 最最最重要的一个类
         * 这里获取对应的StreamTask，后面Task执行的时候会通过StreamTask构造SourceStreamTask、OneInputStreamTask
         * class org.apache.flink.streaming.runtime.tasks.SourceStreamTask
         */
        Class<? extends TaskInvokable> invokableClass =
                operatorFactory.isStreamSource()
                        ? SourceStreamTask.class
                        : OneInputStreamTask.class;
        /**
         * 构建StreamNode
         */
        addOperator(
                vertexID,
                slotSharingGroup,
                coLocationGroup,
                operatorFactory,
                inTypeInfo,
                outTypeInfo,
                operatorName,
                invokableClass);
    }
    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * addOperator的私有方法,内部调用addNode方法进行构建StreamNode
      * @param vertexID: transformationId。
      * @param slotSharingGroup: 操作符所属的槽共享组，用于优化资源利用。
      * @param coLocationGroup: 操作符的协同定位组，用于确保操作符在相同的任务槽中执行。
      * @param operatorFactory: 用于创建StreamOperator的工厂类。
      * @param inTypeInfo: 输入数据的类型信息。
      * @param outTypeInfo: 输出数据的类型信息。
      * @param operatorName: StreamOperator的名称。
      * @param invokableClass: 操作符的可调用类，它定义了操作符的行为。
      */
    private <IN, OUT> void addOperator(
            Integer vertexID,
            @Nullable String slotSharingGroup,
            @Nullable String coLocationGroup,
            StreamOperatorFactory<OUT> operatorFactory,
            TypeInformation<IN> inTypeInfo,
            TypeInformation<OUT> outTypeInfo,
            String operatorName,
            Class<? extends TaskInvokable> invokableClass) {
        /**
         * 调用addNode方法，将操作符作为一个节点添加到图中。这个方法使用提供的参数来配置节点，包括操作符工厂、操作符名称、槽共享组和协同定位组。
         */
        addNode(
                vertexID,
                slotSharingGroup,
                coLocationGroup,
                invokableClass,
                operatorFactory,
                operatorName);
        /**
         *使用setSerializers方法设置节点的输入和输出序列化器。
         * 输入和输出序列化器用于将数据从一种格式转换为另一种格式，为了在不同的组件之间传输数据。
         * createSerializer方法用于根据输入和输出类型信息创建序列化器。
         * 为什么为传入vertexId，通过vertexId得到StreamNode，那么StreamNode内部肯定有对类型相关的序列化对象吧（TypeSerializer）
         * 注意Source这种作为头的数据源inType是什么（null吧）
         */
        setSerializers(vertexID, createSerializer(inTypeInfo), null, createSerializer(outTypeInfo));

        /**
         * 操作符工厂允许配置输出类型，并且已经提供了输出类型信息，那么会调用setOutputType方法来设置操作符的输出类型。
         */
        if (operatorFactory.isOutputTypeConfigurable() && outTypeInfo != null) {
            // sets the output type which must be know at StreamGraph creation time
            operatorFactory.setOutputType(outTypeInfo, executionConfig);
        }
        /**
         * 如果操作符工厂允许配置输入类型，那么会调用setInputType方法来设置操作符的输入类型。
         */
        if (operatorFactory.isInputTypeConfigurable()) {
            operatorFactory.setInputType(inTypeInfo, executionConfig);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Vertex: {}", vertexID);
        }
    }

    public <IN1, IN2, OUT> void addCoOperator(
            Integer vertexID,
            String slotSharingGroup,
            @Nullable String coLocationGroup,
            StreamOperatorFactory<OUT> taskOperatorFactory,
            TypeInformation<IN1> in1TypeInfo,
            TypeInformation<IN2> in2TypeInfo,
            TypeInformation<OUT> outTypeInfo,
            String operatorName) {

        Class<? extends TaskInvokable> vertexClass = TwoInputStreamTask.class;

        addNode(
                vertexID,
                slotSharingGroup,
                coLocationGroup,
                vertexClass,
                taskOperatorFactory,
                operatorName);

        TypeSerializer<OUT> outSerializer = createSerializer(outTypeInfo);

        setSerializers(
                vertexID,
                in1TypeInfo.createSerializer(executionConfig.getSerializerConfig()),
                in2TypeInfo.createSerializer(executionConfig.getSerializerConfig()),
                outSerializer);

        if (taskOperatorFactory.isOutputTypeConfigurable()) {
            // sets the output type which must be known at StreamGraph creation time
            taskOperatorFactory.setOutputType(outTypeInfo, executionConfig);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("CO-TASK: {}", vertexID);
        }
    }

    public <OUT> void addMultipleInputOperator(
            Integer vertexID,
            String slotSharingGroup,
            @Nullable String coLocationGroup,
            StreamOperatorFactory<OUT> operatorFactory,
            List<TypeInformation<?>> inTypeInfos,
            TypeInformation<OUT> outTypeInfo,
            String operatorName) {

        Class<? extends TaskInvokable> vertexClass = MultipleInputStreamTask.class;

        addNode(
                vertexID,
                slotSharingGroup,
                coLocationGroup,
                vertexClass,
                operatorFactory,
                operatorName);

        setSerializers(vertexID, inTypeInfos, createSerializer(outTypeInfo));

        if (operatorFactory.isOutputTypeConfigurable()) {
            // sets the output type which must be known at StreamGraph creation time
            operatorFactory.setOutputType(outTypeInfo, executionConfig);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("CO-TASK: {}", vertexID);
        }
    }

    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
     * addNode方法内部进行创建StreamNode
      * vertexID: 节点的唯一标识符。
      * slotSharingGroup: 节点所属的槽共享组，这通常用于优化资源使用，允许节点在相同的TaskSlot中运行，以减少任务启动和通信的开销。
      * coLocationGroup: 协同定位组，用于确保节点在同一物理或逻辑位置执行，有助于减少网络通信延迟。
      * vertexClass: 节点对应的可调用类，它定义了节点的执行逻辑。
      * operatorFactory: 用于创建操作符（Operator）的工厂类，操作符是处理流数据的主要组件。
     * operatorName: 操作符的名称，通常用于日志记录或调试。
      * 两个核心
      * 1.构建StreamNode
      * 2.将来构建后的 StreamNode 放入map对象，Map<Integer, StreamNode> streamNodes
      * key为transformation,value为StreamNode对象实例
      */
    protected StreamNode addNode(
            Integer vertexID,
            @Nullable String slotSharingGroup,
            @Nullable String coLocationGroup,
            Class<? extends TaskInvokable> vertexClass,
            StreamOperatorFactory<?> operatorFactory,
            String operatorName) {
        /**
         * 检查重复的vertexID,如果存在，则抛出一个运行时异常，因为每个节点的vertexID应该是唯一的。
         */
        if (streamNodes.containsKey(vertexID)) {
            throw new RuntimeException("Duplicate vertexID " + vertexID);
        }
        /**
         * 使用传入的参数创建一个新的StreamNode实例。这个实例代表图中的一个节点，它包含了操作符工厂、名称、类以及其他配置信息。
         * 大家可以想象，现在所有做的构建图或者简单一点一个对象实例就是用来后面执行所用的。
         */
        StreamNode vertex =
                new StreamNode(
                        vertexID,
                        slotSharingGroup,
                        coLocationGroup,
                        operatorFactory,
                        operatorName,
                        vertexClass);
        /**
         * 将新节点添加到streamNodes映射中
         */
        streamNodes.put(vertexID, vertex);
        /**
         * 返回新创建的StreamNode实例
         */
        return vertex;
    }

    /**
     * Adds a new virtual node that is used to connect a downstream vertex to only the outputs with
     * the selected side-output {@link OutputTag}.
     *
     * @param originalId ID of the node that should be connected to.
     * @param virtualId ID of the virtual node.
     * @param outputTag The selected side-output {@code OutputTag}.
     */
    public void addVirtualSideOutputNode(
            Integer originalId, Integer virtualId, OutputTag outputTag) {

        if (virtualSideOutputNodes.containsKey(virtualId)) {
            throw new IllegalStateException("Already has virtual output node with id " + virtualId);
        }

        // verify that we don't already have a virtual node for the given originalId/outputTag
        // combination with a different TypeInformation. This would indicate that someone is trying
        // to read a side output from an operation with a different type for the same side output
        // id.

        for (Tuple2<Integer, OutputTag> tag : virtualSideOutputNodes.values()) {
            if (!tag.f0.equals(originalId)) {
                // different source operator
                continue;
            }

            if (tag.f1.getId().equals(outputTag.getId())
                    && !tag.f1.getTypeInfo().equals(outputTag.getTypeInfo())) {
                throw new IllegalArgumentException(
                        "Trying to add a side output for the same "
                                + "side-output id with a different type. This is not allowed. Side-output ID: "
                                + tag.f1.getId());
            }
        }

        virtualSideOutputNodes.put(virtualId, new Tuple2<>(originalId, outputTag));
    }

    /**
     * Adds a new virtual node that is used to connect a downstream vertex to an input with a
     * certain partitioning.
     *
     * <p>When adding an edge from the virtual node to a downstream node the connection will be made
     * to the original node, but with the partitioning given here.
     *
     * @param originalId ID of the node that should be connected to.
     * @param virtualId ID of the virtual node.
     * @param partitioner The partitioner
     */
    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * 这个方法的主要目的是向virtualPartitionNodes 集合中添加一个新的虚拟分区节点。在添加之前，
     * 它会检查是否已经存在一个具有相同ID的虚拟节点，以确保不会有重复的节点被添加进去。
      */
    public void addVirtualPartitionNode(
            Integer originalId,
            Integer virtualId,
            StreamPartitioner<?> partitioner,
            StreamExchangeMode exchangeMode) {
        /**
         * 检查虚拟节点是否已存在。
         * 检查Map结构中是否已包含给定 virtualId 的键。
         * 如果包含，那么会抛出一个 IllegalStateException 异常，
         * 说明已经存在一个具有该ID的虚拟分区节点。
         */
        if (virtualPartitionNodes.containsKey(virtualId)) {
            throw new IllegalStateException(
                    "Already has virtual partition node with id " + virtualId);
        }
        /**
         * 如果虚拟节点不存在，那么将创建一个新的 Tuple3 对象（很可能是 Apache Flink 中的一个三元组类），
         * 其中包含了 originalId、partitioner 和 exchangeMode 这三个元素。然后，将这个三元组对象作为值，
         * 与 virtualId 作为键一起，添加到 virtualPartitionNodes 映射中。
         */
        virtualPartitionNodes.put(virtualId, new Tuple3<>(originalId, partitioner, exchangeMode));
    }

    /** Determines the slot sharing group of an operation across virtual nodes. */
    public String getSlotSharingGroup(Integer id) {
        if (virtualSideOutputNodes.containsKey(id)) {
            Integer mappedId = virtualSideOutputNodes.get(id).f0;
            return getSlotSharingGroup(mappedId);
        } else if (virtualPartitionNodes.containsKey(id)) {
            Integer mappedId = virtualPartitionNodes.get(id).f0;
            return getSlotSharingGroup(mappedId);
        } else {
            StreamNode node = getStreamNode(id);
            return node.getSlotSharingGroup();
        }
    }

    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * 向图中添加边
      * upStreamVertexID：上游顶点的ID。
      * downStreamVertexID：下游顶点的ID。
      * typeNumber：边的类型编号。
      */
    public void addEdge(Integer upStreamVertexID, Integer downStreamVertexID, int typeNumber) {
        addEdge(upStreamVertexID, downStreamVertexID, typeNumber, null);
    }

    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * addEdgeInternal内部方法添加StreamNode添加入边出边
      * upStreamVertexID：上游顶点的ID。
      * downStreamVertexID：下游顶点的ID。
      * typeNumber：边的类型编号。
      * intermediateDataSetId：中间数据集ID。
      */
    public void addEdge(
            Integer upStreamVertexID,
            Integer downStreamVertexID,
            int typeNumber,
            IntermediateDataSetID intermediateDataSetId) {
        addEdgeInternal(
                upStreamVertexID,
                downStreamVertexID,
                typeNumber,
                null,
                new ArrayList<String>(),
                null,
                null,
                intermediateDataSetId);
    }
    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * 向StreamNode内部添加入边、出边
      * upStreamVertexID 和 downStreamVertexID：分别表示上游和下游顶点的ID。
      * typeNumber：边的类型编号。
      * partitioner：流分区器，用于确定数据如何在顶点之间划分。
      * outputNames：输出名称列表，可能用于标识边的输出。
      * outputTag：输出标签，可能用于标识边产生的特定输出。
      * exchangeMode：流交换模式，可能用于确定顶点之间数据交换的方式。
      * intermediateDataSetId：中间数据集ID，可能用于标识在数据流动过程中产生的中间数据集。
      */
    private void addEdgeInternal(
            Integer upStreamVertexID,
            Integer downStreamVertexID,
            int typeNumber,
            StreamPartitioner<?> partitioner,
            List<String> outputNames,
            OutputTag outputTag,
            StreamExchangeMode exchangeMode,
            IntermediateDataSetID intermediateDataSetId) {
        /**
         * 检查上游顶点是否为虚拟侧输出节点
         */
        if (virtualSideOutputNodes.containsKey(upStreamVertexID)) {
            /** 则获取虚拟ID */
            int virtualId = upStreamVertexID;
            /**
             * 更新upStreamVertexID为实际的上游顶点ID，并检查是否需要设置outputTag。
             * 注意：这段代码就是获取虚拟上游的transformationId，然后继续递归调用
             */
            upStreamVertexID = virtualSideOutputNodes.get(virtualId).f0;
            if (outputTag == null) {
                outputTag = virtualSideOutputNodes.get(virtualId).f1;
            }
            /**
             * 如果是虚拟边则继续递归调用
             */
            addEdgeInternal(
                    upStreamVertexID,
                    downStreamVertexID,
                    typeNumber,
                    partitioner,
                    null,
                    outputTag,
                    exchangeMode,
                    intermediateDataSetId);
            /**
             * 检查上游顶点是否为虚拟节点
             */
        } else if (virtualPartitionNodes.containsKey(upStreamVertexID)) {
            /** 将上游transformationVertexId赋值给virtualId*/
            int virtualId = upStreamVertexID;
            /** 更新upStreamVertexId 为上游的id*/
            upStreamVertexID = virtualPartitionNodes.get(virtualId).f0;
            /**
             * 判断partitioner是否为空
             */
            if (partitioner == null) {
                /** 获取虚拟边对应的分区策略也就是StreamPartitioner*/
                partitioner = virtualPartitionNodes.get(virtualId).f1;
            }
            /**
             * StreamGraph构建期间期间确定StreamOperator之间的数据交换模式
             */
            exchangeMode = virtualPartitionNodes.get(virtualId).f2;
            /**
             * 如果是虚拟边则继续递归调用
             */
            addEdgeInternal(
                    upStreamVertexID,
                    downStreamVertexID,
                    typeNumber,
                    partitioner,
                    outputNames,
                    outputTag,
                    exchangeMode,
                    intermediateDataSetId);
        } else {
            /**
             * 如果不是虚拟变，则直接调用createActualEdge增加入边出边。
             */
            createActualEdge(
                    upStreamVertexID,
                    downStreamVertexID,
                    typeNumber,
                    partitioner,
                    outputTag,
                    exchangeMode,
                    intermediateDataSetId);
        }
    }

    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      * 添加实际的边
      * upStreamVertexID 和 downStreamVertexID：分别表示上游和下游顶点的ID。
      * typeNumber：边的类型编号。
      * partitioner：分区器，用于确定数据如何在顶点之间划分。
      * outputNames：输出名称列表，可能用于标识边的输出。
      * outputTag：输出标签，可能用于标识边产生的特定输出。
      * exchangeMode：流交换模式，可能用于确定顶点之间数据交换的方式。
      * intermediateDataSetId：中间数据集ID，可能用于标识在数据流动过程中产生的中间数据集。
      */
    private void createActualEdge(
            Integer upStreamVertexID,
            Integer downStreamVertexID,
            int typeNumber,
            StreamPartitioner<?> partitioner,
            OutputTag outputTag,
            StreamExchangeMode exchangeMode,
            IntermediateDataSetID intermediateDataSetId) {
        /**
         * 通过上游TransformationId获取到StreamNode
         * 通过下游TransformationId获取到StreamNode
         * 这里为什么是上游下游？
         * 说明这里设置的边是上游的出边、下游的入边。
         */
        StreamNode upstreamNode = getStreamNode(upStreamVertexID);
        /**
         * 通过下游TransformationId获取到StreamNode
         */
        StreamNode downstreamNode = getStreamNode(downStreamVertexID);

        // If no partitioner was specified and the parallelism of upstream and downstream
        // operator matches use forward partitioning, use rebalance otherwise.
        /**
         * 如果没有指定分隔器，并且上游和下游的平行度,则是ForwardPartitioner
         * 运算符匹配使用正向分区，否则使用重新平衡。
         */
        if (partitioner == null
                && upstreamNode.getParallelism() == downstreamNode.getParallelism()) {
            partitioner =
                    dynamic ? new ForwardForUnspecifiedPartitioner<>() : new ForwardPartitioner<>();
        } else if (partitioner == null) {
            /**
             * 如果上游和下游节点的并行度不同，并且没有指定partitioner，则使用RebalancePartitioner。
             */
            partitioner = new RebalancePartitioner<Object>();
        }
        /**
         * 代码检查partitioner是否是ForwardPartitioner的实例。如果是，并且上游和下游节点的并行度不同，
         */
        if (partitioner instanceof ForwardPartitioner) {
            if (upstreamNode.getParallelism() != downstreamNode.getParallelism()) {
                /**
                 * 如果partitioner是ForwardForConsecutiveHashPartitioner的实例，则获取其内部的HashPartitioner
                 */
                if (partitioner instanceof ForwardForConsecutiveHashPartitioner) {
                    partitioner =
                            ((ForwardForConsecutiveHashPartitioner<?>) partitioner)
                                    .getHashPartitioner();
                    /**
                     * 如果不是，则抛出一个UnsupportedOperationException异常，因为前向分区不允许改变并行度。
                     */
                } else {
                    throw new UnsupportedOperationException(
                            "Forward partitioning does not allow "
                                    + "change of parallelism. Upstream operation: "
                                    + upstreamNode
                                    + " parallelism: "
                                    + upstreamNode.getParallelism()
                                    + ", downstream operation: "
                                    + downstreamNode
                                    + " parallelism: "
                                    + downstreamNode.getParallelism()
                                    + " You must use another partitioning strategy, such as broadcast, rebalance, shuffle or global.");
                }
            }
        }
        /**
         * 代码检查exchangeMode（流交换模式）是否为null。如果为null，则将其设置为StreamExchangeMode.UNDEFINED，表示没有指定具体的交换模式
         */
        if (exchangeMode == null) {
            exchangeMode = StreamExchangeMode.UNDEFINED;
        }

        /**
         * Just make sure that {@link StreamEdge} connecting same nodes (for example as a result of
         * self unioning a {@link DataStream}) are distinct and unique. Otherwise it would be
         * difficult on the {@link StreamTask} to assign {@link RecordWriter}s to correct {@link
         * StreamEdge}.
         */
        /**
         * 代码获取连接相同上下游节点的StreamEdge的数量，并使用这个数量作为新StreamEdge的唯一标识符（uniqueId）。这样，即使存在多个连接相同节点的边，它们也将拥有不同的uniqueId。
         */
        int uniqueId = getStreamEdges(upstreamNode.getId(), downstreamNode.getId()).size();
        /**
         * 创建的StreamEdge添加到上游节点的输出边集合中，并添加到下游节点的输入边集合中
         * (包括上下游节点、类型编号、分区器、输出标签、交换模式、唯一标识符和中间数据集ID)
         */
        StreamEdge edge =
                new StreamEdge(
                        upstreamNode,
                        downstreamNode,
                        typeNumber,
                        partitioner,
                        outputTag,
                        exchangeMode,
                        uniqueId,
                        intermediateDataSetId);
        /**
         * 将edge添加到上游的出边种
         * edge.getSourceId() 刚好是上游的TransformationId
         */
        getStreamNode(edge.getSourceId()).addOutEdge(edge);
        /**
         * 将edge添加到下游游的入边
         * edge.getTargetId() 刚好是下游的TransformationID
         */
        getStreamNode(edge.getTargetId()).addInEdge(edge);
    }

    public void setParallelism(Integer vertexID, int parallelism) {
        if (getStreamNode(vertexID) != null) {
            getStreamNode(vertexID).setParallelism(parallelism);
        }
    }

    public boolean isDynamic() {
        return dynamic;
    }

    /**
      * @授课老师(V): yi_locus
      * email: 156184212@qq.com
      *
      */
    public void setParallelism(Integer vertexId, int parallelism, boolean parallelismConfigured) {
        if (getStreamNode(vertexId) != null) {
            getStreamNode(vertexId).setParallelism(parallelism, parallelismConfigured);
        }
    }

    public void setDynamic(boolean dynamic) {
        this.dynamic = dynamic;
    }

    public void setMaxParallelism(int vertexID, int maxParallelism) {
        if (getStreamNode(vertexID) != null) {
            getStreamNode(vertexID).setMaxParallelism(maxParallelism);
        }
    }

    public void setResources(
            int vertexID, ResourceSpec minResources, ResourceSpec preferredResources) {
        if (getStreamNode(vertexID) != null) {
            getStreamNode(vertexID).setResources(minResources, preferredResources);
        }
    }

    public void setManagedMemoryUseCaseWeights(
            int vertexID,
            Map<ManagedMemoryUseCase, Integer> operatorScopeUseCaseWeights,
            Set<ManagedMemoryUseCase> slotScopeUseCases) {
        if (getStreamNode(vertexID) != null) {
            getStreamNode(vertexID)
                    .setManagedMemoryUseCaseWeights(operatorScopeUseCaseWeights, slotScopeUseCases);
        }
    }

    public void setOneInputStateKey(
            Integer vertexID, KeySelector<?, ?> keySelector, TypeSerializer<?> keySerializer) {
        StreamNode node = getStreamNode(vertexID);
        node.setStatePartitioners(keySelector);
        node.setStateKeySerializer(keySerializer);
    }

    public void setTwoInputStateKey(
            Integer vertexID,
            KeySelector<?, ?> keySelector1,
            KeySelector<?, ?> keySelector2,
            TypeSerializer<?> keySerializer) {
        StreamNode node = getStreamNode(vertexID);
        node.setStatePartitioners(keySelector1, keySelector2);
        node.setStateKeySerializer(keySerializer);
    }

    public void setMultipleInputStateKey(
            Integer vertexID,
            List<KeySelector<?, ?>> keySelectors,
            TypeSerializer<?> keySerializer) {
        StreamNode node = getStreamNode(vertexID);
        node.setStatePartitioners(keySelectors.stream().toArray(KeySelector[]::new));
        node.setStateKeySerializer(keySerializer);
    }

    public void setBufferTimeout(Integer vertexID, long bufferTimeout) {
        if (getStreamNode(vertexID) != null) {
            getStreamNode(vertexID).setBufferTimeout(bufferTimeout);
        }
    }

    public void setSerializers(
            Integer vertexID, TypeSerializer<?> in1, TypeSerializer<?> in2, TypeSerializer<?> out) {
        StreamNode vertex = getStreamNode(vertexID);
        vertex.setSerializersIn(in1, in2);
        vertex.setSerializerOut(out);
    }

    private <OUT> void setSerializers(
            Integer vertexID, List<TypeInformation<?>> inTypeInfos, TypeSerializer<OUT> out) {

        StreamNode vertex = getStreamNode(vertexID);

        vertex.setSerializersIn(
                inTypeInfos.stream()
                        .map(
                                typeInfo ->
                                        typeInfo.createSerializer(
                                                executionConfig.getSerializerConfig()))
                        .toArray(TypeSerializer[]::new));
        vertex.setSerializerOut(out);
    }

    public void setInputFormat(Integer vertexID, InputFormat<?, ?> inputFormat) {
        getStreamNode(vertexID).setInputFormat(inputFormat);
    }

    public void setOutputFormat(Integer vertexID, OutputFormat<?> outputFormat) {
        getStreamNode(vertexID).setOutputFormat(outputFormat);
    }

    public void setTransformationUID(Integer nodeId, String transformationId) {
        StreamNode node = streamNodes.get(nodeId);
        if (node != null) {
            node.setTransformationUID(transformationId);
        }
    }

    void setTransformationUserHash(Integer nodeId, String nodeHash) {
        StreamNode node = streamNodes.get(nodeId);
        if (node != null) {
            node.setUserHash(nodeHash);
        }
    }

    public StreamNode getStreamNode(Integer vertexID) {
        return streamNodes.get(vertexID);
    }

    protected Collection<? extends Integer> getVertexIDs() {
        return streamNodes.keySet();
    }

    @VisibleForTesting
    public List<StreamEdge> getStreamEdges(int sourceId) {
        return getStreamNode(sourceId).getOutEdges();
    }

    @VisibleForTesting
    public List<StreamEdge> getStreamEdges(int sourceId, int targetId) {
        List<StreamEdge> result = new ArrayList<>();
        for (StreamEdge edge : getStreamNode(sourceId).getOutEdges()) {
            if (edge.getTargetId() == targetId) {
                result.add(edge);
            }
        }
        return result;
    }

    @VisibleForTesting
    @Deprecated
    public List<StreamEdge> getStreamEdgesOrThrow(int sourceId, int targetId) {
        List<StreamEdge> result = getStreamEdges(sourceId, targetId);
        if (result.isEmpty()) {
            throw new RuntimeException(
                    "No such edge in stream graph: " + sourceId + " -> " + targetId);
        }
        return result;
    }

    public Collection<Integer> getSourceIDs() {
        return sources;
    }

    public Collection<Integer> getSinkIDs() {
        return sinks;
    }

    public Collection<StreamNode> getStreamNodes() {
        return streamNodes.values();
    }

    public Set<Tuple2<Integer, StreamOperatorFactory<?>>> getAllOperatorFactory() {
        Set<Tuple2<Integer, StreamOperatorFactory<?>>> operatorSet = new HashSet<>();
        for (StreamNode vertex : streamNodes.values()) {
            operatorSet.add(new Tuple2<>(vertex.getId(), vertex.getOperatorFactory()));
        }
        return operatorSet;
    }

    public String getBrokerID(Integer vertexID) {
        return vertexIDtoBrokerID.get(vertexID);
    }

    public long getLoopTimeout(Integer vertexID) {
        return vertexIDtoLoopTimeout.get(vertexID);
    }

    public Tuple2<StreamNode, StreamNode> createIterationSourceAndSink(
            int loopId,
            int sourceId,
            int sinkId,
            long timeout,
            int parallelism,
            int maxParallelism,
            ResourceSpec minResources,
            ResourceSpec preferredResources) {

        final String coLocationGroup = "IterationCoLocationGroup-" + loopId;

        StreamNode source =
                this.addNode(
                        sourceId,
                        null,
                        coLocationGroup,
                        StreamIterationHead.class,
                        null,
                        ITERATION_SOURCE_NAME_PREFIX + "-" + loopId);
        sources.add(source.getId());
        setParallelism(source.getId(), parallelism);
        setMaxParallelism(source.getId(), maxParallelism);
        setResources(source.getId(), minResources, preferredResources);

        StreamNode sink =
                this.addNode(
                        sinkId,
                        null,
                        coLocationGroup,
                        StreamIterationTail.class,
                        null,
                        ITERATION_SINK_NAME_PREFIX + "-" + loopId);
        sinks.add(sink.getId());
        setParallelism(sink.getId(), parallelism);
        setMaxParallelism(sink.getId(), parallelism);
        // The tail node is always in the same slot sharing group with the head node
        // so that they can share resources (they do not use non-sharable resources,
        // i.e. managed memory). There is no contract on how the resources should be
        // divided for head and tail nodes at the moment. To be simple, we assign all
        // resources to the head node and set the tail node resources to be zero if
        // resources are specified.
        final ResourceSpec tailResources =
                minResources.equals(ResourceSpec.UNKNOWN)
                        ? ResourceSpec.UNKNOWN
                        : ResourceSpec.ZERO;
        setResources(sink.getId(), tailResources, tailResources);

        iterationSourceSinkPairs.add(new Tuple2<>(source, sink));

        this.vertexIDtoBrokerID.put(source.getId(), "broker-" + loopId);
        this.vertexIDtoBrokerID.put(sink.getId(), "broker-" + loopId);
        this.vertexIDtoLoopTimeout.put(source.getId(), timeout);
        this.vertexIDtoLoopTimeout.put(sink.getId(), timeout);

        return new Tuple2<>(source, sink);
    }

    public Set<Tuple2<StreamNode, StreamNode>> getIterationSourceSinkPairs() {
        return iterationSourceSinkPairs;
    }

    public StreamNode getSourceVertex(StreamEdge edge) {
        return streamNodes.get(edge.getSourceId());
    }

    public StreamNode getTargetVertex(StreamEdge edge) {
        return streamNodes.get(edge.getTargetId());
    }

    private void removeEdge(StreamEdge edge) {
        getSourceVertex(edge).getOutEdges().remove(edge);
        getTargetVertex(edge).getInEdges().remove(edge);
    }

    private void removeVertex(StreamNode toRemove) {
        Set<StreamEdge> edgesToRemove = new HashSet<>();

        edgesToRemove.addAll(toRemove.getInEdges());
        edgesToRemove.addAll(toRemove.getOutEdges());

        for (StreamEdge edge : edgesToRemove) {
            removeEdge(edge);
        }
        streamNodes.remove(toRemove.getId());
    }

    /** Gets the assembled {@link JobGraph} with a random {@link JobID}. */
    @VisibleForTesting
    public JobGraph getJobGraph() {
        return getJobGraph(Thread.currentThread().getContextClassLoader(), null);
    }

    /** Gets the assembled {@link JobGraph} with a specified {@link JobID}. */
    public JobGraph getJobGraph(ClassLoader userClassLoader, @Nullable JobID jobID) {
        return StreamingJobGraphGenerator.createJobGraph(userClassLoader, this, jobID);
    }

    public String getStreamingPlanAsJSON() {
        try {
            return new JSONGenerator(this).getJSON();
        } catch (Exception e) {
            throw new RuntimeException("JSON plan creation failed", e);
        }
    }

    private <T> TypeSerializer<T> createSerializer(TypeInformation<T> typeInfo) {
        return typeInfo != null && !(typeInfo instanceof MissingTypeInfo)
                ? typeInfo.createSerializer(executionConfig.getSerializerConfig())
                : null;
    }

    public void setJobType(JobType jobType) {
        this.jobType = jobType;
    }

    public JobType getJobType() {
        return jobType;
    }

    public boolean isAutoParallelismEnabled() {
        return autoParallelismEnabled;
    }

    public void setAutoParallelismEnabled(boolean autoParallelismEnabled) {
        this.autoParallelismEnabled = autoParallelismEnabled;
    }

    public PipelineOptions.VertexDescriptionMode getVertexDescriptionMode() {
        return descriptionMode;
    }

    public void setVertexDescriptionMode(PipelineOptions.VertexDescriptionMode mode) {
        this.descriptionMode = mode;
    }

    public void setVertexNameIncludeIndexPrefix(boolean includePrefix) {
        this.vertexNameIncludeIndexPrefix = includePrefix;
    }

    public boolean isVertexNameIncludeIndexPrefix() {
        return this.vertexNameIncludeIndexPrefix;
    }

    /** Registers the JobStatusHook. */
    public void registerJobStatusHook(JobStatusHook hook) {
        checkNotNull(hook, "Registering a null JobStatusHook is not allowed. ");
        if (!jobStatusHooks.contains(hook)) {
            this.jobStatusHooks.add(hook);
        }
    }

    public List<JobStatusHook> getJobStatusHooks() {
        return this.jobStatusHooks;
    }

    public void setSupportsConcurrentExecutionAttempts(
            Integer vertexId, boolean supportsConcurrentExecutionAttempts) {
        final StreamNode streamNode = getStreamNode(vertexId);
        if (streamNode != null) {
            streamNode.setSupportsConcurrentExecutionAttempts(supportsConcurrentExecutionAttempts);
        }
    }
}
