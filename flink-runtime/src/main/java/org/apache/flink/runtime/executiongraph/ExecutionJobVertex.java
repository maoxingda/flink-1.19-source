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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.Archiveable;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.InputSplitSource;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinatorHolder;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;
import org.apache.flink.runtime.scheduler.VertexParallelismInformation;
import org.apache.flink.runtime.source.coordinator.SourceCoordinator;
import org.apache.flink.types.Either;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An {@code ExecutionJobVertex} is part of the {@link ExecutionGraph}, and the peer to the {@link
 * JobVertex}.
 *
 * <p>The {@code ExecutionJobVertex} corresponds to a parallelized operation. It contains an {@link
 * ExecutionVertex} for each parallel instance of that operation.
 */
public class ExecutionJobVertex
        implements AccessExecutionJobVertex, Archiveable<ArchivedExecutionJobVertex> {

    /** Use the same log for all ExecutionGraph classes. */
    private static final Logger LOG = DefaultExecutionGraph.LOG;

    private final Object stateMonitor = new Object();

    private final InternalExecutionGraphAccessor graph;

    private final JobVertex jobVertex;

    @Nullable private ExecutionVertex[] taskVertices;

    @Nullable private IntermediateResult[] producedDataSets;

    @Nullable private List<IntermediateResult> inputs;

    private final VertexParallelismInformation parallelismInfo;

    private final SlotSharingGroup slotSharingGroup;

    @Nullable private final CoLocationGroup coLocationGroup;

    @Nullable private InputSplit[] inputSplits;

    private final ResourceProfile resourceProfile;

    private int numExecutionVertexFinished;

    /**
     * Either store a serialized task information, which is for all sub tasks the same, or the
     * permanent blob key of the offloaded task information BLOB containing the serialized task
     * information.
     */
    private Either<SerializedValue<TaskInformation>, PermanentBlobKey> taskInformationOrBlobKey =
            null;

    private final Collection<OperatorCoordinatorHolder> operatorCoordinators;

    @Nullable private InputSplitAssigner splitAssigner;

    @VisibleForTesting
    public ExecutionJobVertex(
            InternalExecutionGraphAccessor graph,
            JobVertex jobVertex,
            VertexParallelismInformation parallelismInfo,
            CoordinatorStore coordinatorStore,
            JobManagerJobMetricGroup jobManagerJobMetricGroup)
            throws JobException {

        if (graph == null || jobVertex == null) {
            throw new NullPointerException();
        }

        this.graph = graph;
        this.jobVertex = jobVertex;

        this.parallelismInfo = parallelismInfo;

        // verify that our parallelism is not higher than the maximum parallelism
        /** 验证并行度是否不高于最大并行度 */
        if (this.parallelismInfo.getParallelism() > this.parallelismInfo.getMaxParallelism()) {
            throw new JobException(
                    String.format(
                            "Vertex %s's parallelism (%s) is higher than the max parallelism (%s). Please lower the parallelism or increase the max parallelism.",
                            jobVertex.getName(),
                            this.parallelismInfo.getParallelism(),
                            this.parallelismInfo.getMaxParallelism()));
        }
        /** 获取ResourceProfile 基于配置构建*/
        this.resourceProfile =
                ResourceProfile.fromResourceSpec(jobVertex.getMinResources(), MemorySize.ZERO);

        // take the sharing group
        this.slotSharingGroup = checkNotNull(jobVertex.getSlotSharingGroup());
        this.coLocationGroup = jobVertex.getCoLocationGroup();

        final List<SerializedValue<OperatorCoordinator.Provider>> coordinatorProviders =
                getJobVertex().getOperatorCoordinators();
        if (coordinatorProviders.isEmpty()) {
            this.operatorCoordinators = Collections.emptyList();
        } else {
            final ArrayList<OperatorCoordinatorHolder> coordinators =
                    new ArrayList<>(coordinatorProviders.size());
            try {
                for (final SerializedValue<OperatorCoordinator.Provider> provider :
                        coordinatorProviders) {
                    coordinators.add(
                            createOperatorCoordinatorHolder(
                                    provider,
                                    graph.getUserClassLoader(),
                                    coordinatorStore,
                                    jobManagerJobMetricGroup));
                }
            } catch (Exception | LinkageError e) {
                IOUtils.closeAllQuietly(coordinators);
                throw new JobException(
                        "Cannot instantiate the coordinator for operator " + getName(), e);
            }
            this.operatorCoordinators = Collections.unmodifiableList(coordinators);
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * ExecutionVertex、IntermediateResult 初始化
    */
    protected void initialize(
            int executionHistorySizeLimit,
            Time timeout,
            long createTimestamp,
            SubtaskAttemptNumberStore initialAttemptCounts)
            throws JobException {
        /** 校验并行度必须大于了 否则抛出异常 */
        checkState(parallelismInfo.getParallelism() > 0);
        /** 校验是否做过初始化 taskVertices ！= null */
        checkState(!isInitialized());
        /** taskVertices 是一个ExecutionVertex类型的数组，其大小与并行度相同。 */
        this.taskVertices = new ExecutionVertex[parallelismInfo.getParallelism()];
        /** inputs 是一个ArrayList，其初始容量设置为jobVertex的输入数量。 */
        this.inputs = new ArrayList<>(jobVertex.getInputs().size());

        // create the intermediate results
        /** 基于jobVertex results数量构建  IntermediateResult[] producedDataSets数组 用来存储中间结果*/
        this.producedDataSets =
                new IntermediateResult[jobVertex.getNumberOfProducedIntermediateDataSets()];
        /***
         * 代码遍历jobVertex产生的所有数据集（IntermediateDataSet），并为每个数据集创建一个IntermediateResult对象
         */
        for (int i = 0; i < jobVertex.getProducedDataSets().size(); i++) {
            /** 获取JobGraph的IntermediateDataSet */
            final IntermediateDataSet result = jobVertex.getProducedDataSets().get(i);
            /** 构建IntermediateResult */
            this.producedDataSets[i] =
                    new IntermediateResult(
                            result,
                            this,
                            this.parallelismInfo.getParallelism(),
                            result.getResultType());
        }

        // create all task vertices
        /**
         * 根据并行度循环创建ExecutionVertex
         */
        for (int i = 0; i < this.parallelismInfo.getParallelism(); i++) {
            /**
             * 循环中创建一个新的ExecutionVertex对象，并将其存储在this.taskVertices数组中。
             */
            ExecutionVertex vertex =
                    createExecutionVertex(
                            this,
                            i,
                            producedDataSets,
                            timeout,
                            createTimestamp,
                            executionHistorySizeLimit,
                            initialAttemptCounts.getAttemptCount(i));

            this.taskVertices[i] = vertex;
        }

        // sanity check for the double referencing between intermediate result partitions and
        // execution vertices
        /** 中间结果分区和执行顶点之间双重引用的健全性检查 */
        for (IntermediateResult ir : this.producedDataSets) {
            if (ir.getNumberOfAssignedPartitions() != this.parallelismInfo.getParallelism()) {
                throw new RuntimeException(
                        "The intermediate result's partitions were not correctly assigned.");
            }
        }

        // set up the input splits, if the vertex has any
        /** 设置输入拆分，如果顶点有 */
        try {
            /**
             * 尝试从jobVertex中获取InputSplitSource。
             * InputSplit是分布式计算中用于描述数据分区的对象，而InputSplitSource则是产生这些InputSplit的源。
             */
            @SuppressWarnings("unchecked")
            InputSplitSource<InputSplit> splitSource =
                    (InputSplitSource<InputSplit>) jobVertex.getInputSplitSource();
            /**
             * 如果splitSource不为空，代码会更改当前线程的上下文类加载器为graph.getUserClassLoader()。上下文类加载器用于加载类，特别是在处理类路径和类加载器层次结构时。这里更改类加载器可能是为了确保在创建InputSplit时，能够使用用户提供的类加载器来加载相关类。
             */
            if (splitSource != null) {
                /** 获取Thread对象 */
                Thread currentThread = Thread.currentThread();
                /** 获取当前类加载器 */
                ClassLoader oldContextClassLoader = currentThread.getContextClassLoader();
                /** 设置上下文环境的类加载器 */
                currentThread.setContextClassLoader(graph.getUserClassLoader());
                try {
                    /**
                     * 使用splitSource的createInputSplits方法来创建InputSplit对象。
                     * 这些InputSplit对象代表分布式数据集的分区，每个分区可以由一个子任务处理。
                     */
                    inputSplits =
                            splitSource.createInputSplits(this.parallelismInfo.getParallelism());
                    /** 设置给分配器 */
                    if (inputSplits != null) {
                        splitAssigner = splitSource.getInputSplitAssigner(inputSplits);
                    }
                } finally {
                    /** 恢复类加载器 */
                    currentThread.setContextClassLoader(oldContextClassLoader);
                }
            } else {
                /** 设置为null*/
                inputSplits = null;
            }
        } catch (Throwable t) {
            throw new JobException(
                    "Creating the input splits caused an error: " + t.getMessage(), t);
        }
    }

    protected ExecutionVertex createExecutionVertex(
            ExecutionJobVertex jobVertex,
            int subTaskIndex,
            IntermediateResult[] producedDataSets,
            Time timeout,
            long createTimestamp,
            int executionHistorySizeLimit,
            int initialAttemptCount) {
        return new ExecutionVertex(
                jobVertex,
                subTaskIndex,
                producedDataSets,
                timeout,
                createTimestamp,
                executionHistorySizeLimit,
                initialAttemptCount);
    }

    protected OperatorCoordinatorHolder createOperatorCoordinatorHolder(
            SerializedValue<OperatorCoordinator.Provider> provider,
            ClassLoader classLoader,
            CoordinatorStore coordinatorStore,
            JobManagerJobMetricGroup jobManagerJobMetricGroup)
            throws Exception {
        return OperatorCoordinatorHolder.create(
                provider,
                this,
                classLoader,
                coordinatorStore,
                false,
                getTaskInformation(),
                jobManagerJobMetricGroup);
    }

    public boolean isInitialized() {
        return taskVertices != null;
    }

    public boolean isParallelismDecided() {
        return parallelismInfo.getParallelism() > 0;
    }

    /**
     * Returns a list containing the ID pairs of all operators contained in this execution job
     * vertex.
     *
     * @return list containing the ID pairs of all contained operators
     */
    public List<OperatorIDPair> getOperatorIDs() {
        return jobVertex.getOperatorIDs();
    }

    public void setMaxParallelism(int maxParallelism) {
        parallelismInfo.setMaxParallelism(maxParallelism);
    }

    public InternalExecutionGraphAccessor getGraph() {
        return graph;
    }

    public void setParallelism(int parallelism) {
        parallelismInfo.setParallelism(parallelism);
    }

    public JobVertex getJobVertex() {
        return jobVertex;
    }

    @Override
    public String getName() {
        return getJobVertex().getName();
    }

    @Override
    public int getParallelism() {
        return parallelismInfo.getParallelism();
    }

    @Override
    public int getMaxParallelism() {
        return parallelismInfo.getMaxParallelism();
    }

    @Override
    public ResourceProfile getResourceProfile() {
        return resourceProfile;
    }

    public boolean canRescaleMaxParallelism(int desiredMaxParallelism) {
        return parallelismInfo.canRescaleMaxParallelism(desiredMaxParallelism);
    }

    public JobID getJobId() {
        return graph.getJobID();
    }

    @Override
    public JobVertexID getJobVertexId() {
        return jobVertex.getID();
    }

    @Override
    public ExecutionVertex[] getTaskVertices() {
        if (taskVertices == null) {
            // The REST/web may try to get execution vertices of an uninitialized job vertex. Using
            // DEBUG log level to avoid flooding logs.
            LOG.debug(
                    "Trying to get execution vertices of an uninitialized job vertex "
                            + getJobVertexId());
            return new ExecutionVertex[0];
        }
        return taskVertices;
    }

    public IntermediateResult[] getProducedDataSets() {
        checkState(isInitialized());
        return producedDataSets;
    }

    public InputSplitAssigner getSplitAssigner() {
        checkState(isInitialized());
        return splitAssigner;
    }

    public SlotSharingGroup getSlotSharingGroup() {
        return slotSharingGroup;
    }

    @Nullable
    public CoLocationGroup getCoLocationGroup() {
        return coLocationGroup;
    }

    public List<IntermediateResult> getInputs() {
        checkState(isInitialized());
        return inputs;
    }

    public Collection<OperatorCoordinatorHolder> getOperatorCoordinators() {
        checkState(isInitialized());
        return operatorCoordinators;
    }

    public List<SourceCoordinator<?, ?>> getSourceCoordinators() {
        List<SourceCoordinator<?, ?>> sourceCoordinators = new ArrayList<>();
        for (OperatorCoordinatorHolder oph : operatorCoordinators) {
            if (oph.coordinator() instanceof RecreateOnResetOperatorCoordinator) {
                RecreateOnResetOperatorCoordinator opc =
                        (RecreateOnResetOperatorCoordinator) oph.coordinator();
                try {
                    if (opc.getInternalCoordinator() instanceof SourceCoordinator) {
                        sourceCoordinators.add(
                                (SourceCoordinator<?, ?>) opc.getInternalCoordinator());
                    }
                } catch (Throwable e) {
                    throw new RuntimeException(
                            "Unexpected error occurred when get sourceCoordinators.", e);
                }
            }
        }
        return sourceCoordinators;
    }

    int getNumExecutionVertexFinished() {
        return numExecutionVertexFinished;
    }

    public Either<SerializedValue<TaskInformation>, PermanentBlobKey> getTaskInformationOrBlobKey()
            throws IOException {
        // only one thread should offload the task information, so let's also let only one thread
        // serialize the task information!
        synchronized (stateMonitor) {
            if (taskInformationOrBlobKey == null) {
                final BlobWriter blobWriter = graph.getBlobWriter();
                final TaskInformation taskInformation = getTaskInformation();
                taskInformationOrBlobKey =
                        BlobWriter.serializeAndTryOffload(taskInformation, getJobId(), blobWriter);
            }

            return taskInformationOrBlobKey;
        }
    }

    public TaskInformation getTaskInformation() {
        return new TaskInformation(
                jobVertex.getID(),
                jobVertex.getName(),
                parallelismInfo.getParallelism(),
                parallelismInfo.getMaxParallelism(),
                jobVertex.getInvokableClassName(),
                jobVertex.getConfiguration());
    }

    @Override
    public ExecutionState getAggregateState() {
        int[] num = new int[ExecutionState.values().length];
        for (ExecutionVertex vertex : getTaskVertices()) {
            num[vertex.getExecutionState().ordinal()]++;
        }

        return getAggregateJobVertexState(num, this.parallelismInfo.getParallelism());
    }

    // ---------------------------------------------------------------------------------------------
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 设置顶点链接
    */
    public void connectToPredecessors(
            Map<IntermediateDataSetID, IntermediateResult> intermediateDataSets)
            throws JobException {
        /**
         * 它会检查一个状态，确保某个对象或系统已经被初始化。如果未初始化，则可能会抛出异常。
         */
        checkState(isInitialized());
        /**
         * 从jobVertex对象中获取所有输入，存储在一个JobEdge的列表中。JobEdge表示任务顶点之间的连接关系。
         */
        List<JobEdge> inputs = jobVertex.getInputs();
        /**
         * 如果日志的调试级别被启用，那么它会记录一些关于连接任务顶点到其前置任务的信息。
         */
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    String.format(
                            "Connecting ExecutionJobVertex %s (%s) to %d predecessors.",
                            jobVertex.getID(), jobVertex.getName(), inputs.size()));
        }

        /** 代码遍历所有输入，并对每一个JobEdge执行操作。 */
        for (int num = 0; num < inputs.size(); num++) {
            /** 获取JobEdge */
            JobEdge edge = inputs.get(num);
            /**
             * 打印日志
             */
            if (LOG.isDebugEnabled()) {
                if (edge.getSource() == null) {
                    LOG.debug(
                            String.format(
                                    "Connecting input %d of vertex %s (%s) to intermediate result referenced via ID %s.",
                                    num,
                                    jobVertex.getID(),
                                    jobVertex.getName(),
                                    edge.getSourceId()));
                } else {
                    LOG.debug(
                            String.format(
                                    "Connecting input %d of vertex %s (%s) to intermediate result referenced via predecessor %s (%s).",
                                    num,
                                    jobVertex.getID(),
                                    jobVertex.getName(),
                                    edge.getSource().getProducer().getID(),
                                    edge.getSource().getProducer().getName()));
                }
            }

            // fetch the intermediate result via ID. if it does not exist, then it either has not
            // been created, or the order
            // in which this method is called for the job vertices is not a topological order
            /**
             * 通过ID获取中间结果。如果它不存在，那么它要么没有
             * 已创建，或者为作业顶点调用此方法的顺序不是拓扑顺序
             */
            /**
             * 获取上游的IntermediateResult(对应JobGraph IndermediateDataset)
             */
            IntermediateResult ires = intermediateDataSets.get(edge.getSourceId());
            if (ires == null) {
                throw new JobException(
                        "Cannot connect this job graph to the previous graph. No previous intermediate result found for ID "
                                + edge.getSourceId());
            }
            /**
             * 找到的IntermediateResult对象ires添加到当前对象的inputs列表中
             */
            this.inputs.add(ires);
            /**
             * 将当前顶点与找到的中间结果ires连接起来
             */
            EdgeManagerBuildUtil.connectVertexToResult(this, ires);
        }
    }

    // ---------------------------------------------------------------------------------------------
    //  Actions
    // ---------------------------------------------------------------------------------------------

    /** Cancels all currently running vertex executions. */
    public void cancel() {
        for (ExecutionVertex ev : getTaskVertices()) {
            ev.cancel();
        }
    }

    /**
     * Cancels all currently running vertex executions.
     *
     * @return A future that is complete once all tasks have canceled.
     */
    public CompletableFuture<Void> cancelWithFuture() {
        return FutureUtils.waitForAll(mapExecutionVertices(ExecutionVertex::cancel));
    }

    public CompletableFuture<Void> suspend() {
        return FutureUtils.waitForAll(mapExecutionVertices(ExecutionVertex::suspend));
    }

    @Nonnull
    private Collection<CompletableFuture<?>> mapExecutionVertices(
            final Function<ExecutionVertex, CompletableFuture<?>> mapFunction) {
        return Arrays.stream(getTaskVertices()).map(mapFunction).collect(Collectors.toList());
    }

    public void fail(Throwable t) {
        for (ExecutionVertex ev : getTaskVertices()) {
            ev.fail(t);
        }
    }

    void executionVertexFinished() {
        checkState(isInitialized());
        numExecutionVertexFinished++;
        if (numExecutionVertexFinished == parallelismInfo.getParallelism()) {
            getGraph().jobVertexFinished();
        }
    }

    void executionVertexUnFinished() {
        checkState(isInitialized());
        if (numExecutionVertexFinished == parallelismInfo.getParallelism()) {
            getGraph().jobVertexUnFinished();
        }
        numExecutionVertexFinished--;
    }

    public boolean isFinished() {
        return isParallelismDecided()
                && numExecutionVertexFinished == parallelismInfo.getParallelism();
    }

    // --------------------------------------------------------------------------------------------
    //  Accumulators / Metrics
    // --------------------------------------------------------------------------------------------

    public StringifiedAccumulatorResult[] getAggregatedUserAccumulatorsStringified() {
        Map<String, OptionalFailure<Accumulator<?, ?>>> userAccumulators = new HashMap<>();

        for (ExecutionVertex vertex : getTaskVertices()) {
            Map<String, Accumulator<?, ?>> next =
                    vertex.getCurrentExecutionAttempt().getUserAccumulators();
            if (next != null) {
                AccumulatorHelper.mergeInto(userAccumulators, next);
            }
        }

        return StringifiedAccumulatorResult.stringifyAccumulatorResults(userAccumulators);
    }

    // --------------------------------------------------------------------------------------------
    //  Archiving
    // --------------------------------------------------------------------------------------------

    @Override
    public ArchivedExecutionJobVertex archive() {
        return new ArchivedExecutionJobVertex(this);
    }

    // ------------------------------------------------------------------------
    //  Static Utilities
    // ------------------------------------------------------------------------

    /**
     * A utility function that computes an "aggregated" state for the vertex.
     *
     * <p>This state is not used anywhere in the coordination, but can be used for display in
     * dashboards to as a summary for how the particular parallel operation represented by this
     * ExecutionJobVertex is currently behaving.
     *
     * <p>For example, if at least one parallel task is failed, the aggregate state is failed. If
     * not, and at least one parallel task is cancelling (or cancelled), the aggregate state is
     * cancelling (or cancelled). If all tasks are finished, the aggregate state is finished, and so
     * on.
     *
     * @param verticesPerState The number of vertices in each state (indexed by the ordinal of the
     *     ExecutionState values).
     * @param parallelism The parallelism of the ExecutionJobVertex
     * @return The aggregate state of this ExecutionJobVertex.
     */
    public static ExecutionState getAggregateJobVertexState(
            int[] verticesPerState, int parallelism) {
        if (verticesPerState == null || verticesPerState.length != ExecutionState.values().length) {
            throw new IllegalArgumentException(
                    "Must provide an array as large as there are execution states.");
        }

        if (verticesPerState[ExecutionState.FAILED.ordinal()] > 0) {
            return ExecutionState.FAILED;
        }
        if (verticesPerState[ExecutionState.CANCELING.ordinal()] > 0) {
            return ExecutionState.CANCELING;
        } else if (verticesPerState[ExecutionState.CANCELED.ordinal()] > 0) {
            return ExecutionState.CANCELED;
        } else if (verticesPerState[ExecutionState.INITIALIZING.ordinal()] > 0) {
            return ExecutionState.INITIALIZING;
        } else if (verticesPerState[ExecutionState.RUNNING.ordinal()] > 0) {
            return ExecutionState.RUNNING;
        } else if (verticesPerState[ExecutionState.FINISHED.ordinal()] > 0) {
            return verticesPerState[ExecutionState.FINISHED.ordinal()] == parallelism
                    ? ExecutionState.FINISHED
                    : ExecutionState.RUNNING;
        } else {
            // all else collapses under created
            return ExecutionState.CREATED;
        }
    }

    /** Factory to create {@link ExecutionJobVertex}. */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建ExecutionJobVertex
    */
    public static class Factory {
        ExecutionJobVertex createExecutionJobVertex(
                InternalExecutionGraphAccessor graph,
                JobVertex jobVertex,
                VertexParallelismInformation parallelismInfo,
                CoordinatorStore coordinatorStore,
                JobManagerJobMetricGroup jobManagerJobMetricGroup)
                throws JobException {
            /**
             * 构建ExecutionJobVertex
             * 主要参数jobVertex,并行度相关信息、监控指标
             */
            return new ExecutionJobVertex(
                    graph, jobVertex, parallelismInfo, coordinatorStore, jobManagerJobMetricGroup);
        }
    }
}
