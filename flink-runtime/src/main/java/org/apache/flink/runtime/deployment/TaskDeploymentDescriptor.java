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

package org.apache.flink.runtime.deployment;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.PermanentBlobService;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorGroup;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.util.GroupCache;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.List;

/**
 * A task deployment descriptor contains all the information necessary to deploy a task on a task
 * manager.
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 任务部署描述符包含在任务管理器上部署任务所需的所有信息。
*/
public final class TaskDeploymentDescriptor implements Serializable {

    private static final long serialVersionUID = -3233562176034358530L;

    /**
     * Wrapper class for serialized values which may be offloaded to the {@link
     * org.apache.flink.runtime.blob.BlobServer} or not.
     *
     * @param <T> type of the serialized value
     */
    /**
     * 序列化值的包装器类，这些值可以卸载到BlobServer
     * @param <T>
     */
    @SuppressWarnings("unused")
    public static class MaybeOffloaded<T> implements Serializable {
        private static final long serialVersionUID = 5977104446396536907L;
    }

    /**
     * A serialized value that is not offloaded to the {@link
     * org.apache.flink.runtime.blob.BlobServer}.
     *
     * @param <T> type of the serialized value
     */
    /**
     * 未卸载到的序列化值BlobServer
     */
    public static class NonOffloaded<T> extends MaybeOffloaded<T> {
        private static final long serialVersionUID = 4246628617754862463L;

        /** The serialized value. */
        public SerializedValue<T> serializedValue;

        @SuppressWarnings("unused")
        public NonOffloaded() {}

        public NonOffloaded(SerializedValue<T> serializedValue) {
            this.serializedValue = Preconditions.checkNotNull(serializedValue);
        }
    }

    /**
     * Reference to a serialized value that was offloaded to the {@link
     * org.apache.flink.runtime.blob.BlobServer}.
     *
     * @param <T> type of the serialized value
     */
    public static class Offloaded<T> extends MaybeOffloaded<T> {
        private static final long serialVersionUID = 4544135485379071679L;

        /** The key of the offloaded value BLOB. */
        public PermanentBlobKey serializedValueKey;

        @SuppressWarnings("unused")
        public Offloaded() {}

        public Offloaded(PermanentBlobKey serializedValueKey) {
            this.serializedValueKey = Preconditions.checkNotNull(serializedValueKey);
        }
    }

    /** Serialized job information if non-offloaded or <tt>PermanentBlobKey</tt> if offloaded. */
    //序列化后的作业信息，包括作业id、作业名称、作业配置
    private final MaybeOffloaded<JobInformation> serializedJobInformation;

    /** Serialized task information if non-offloaded or <tt>PermanentBlobKey</tt> if offloaded. */
    //序列化后的任务信息，包括任务id、任务名字、任务配置
    private final MaybeOffloaded<TaskInformation> serializedTaskInformation;

    /**
     * The job information, it isn't null when serializedJobInformation is offloaded and after
     * {@link #loadBigData}.
     */
    //作业信息
    private transient JobInformation jobInformation;

    /**
     * The task information, it isn't null when serializedTaskInformation is offloaded and after
     * {@link #loadBigData}.
     */
    //任务信息
    private transient TaskInformation taskInformation;

    /**
     * The ID referencing the job this task belongs to.
     *
     * <p>NOTE: this is redundant to the information stored in {@link #serializedJobInformation} but
     * needed in order to restore offloaded data.
     */
    //作业所属id
    private final JobID jobId;

    /** The ID referencing the attempt to execute the task. */
    //尝试执行任务的ID
    private final ExecutionAttemptID executionId;

    /** The allocation ID of the slot in which the task shall be run. */
    //运行任务的插槽的分配ID。
    private final AllocationID allocationId;

    /** The list of produced intermediate result partition deployment descriptors. */
    //生成的中间结果分区部署描述符的列表。用于在任务端构造ResultPartitionWriter。
    private final List<ResultPartitionDeploymentDescriptor> producedPartitions;

    /** The list of consumed intermediate result partitions. */
    //已使用的中间结果分区的列表。负责数据的输入
    private final List<InputGateDeploymentDescriptor> inputGates;

    /** Information to restore the task. This can be null if there is no state to restore. */
    //用于恢复任务的信息。如果没有要恢复的状态，则此值可能为null。
    @Nullable private final JobManagerTaskRestore taskRestore;

    public TaskDeploymentDescriptor(
            JobID jobId,
            MaybeOffloaded<JobInformation> serializedJobInformation,
            MaybeOffloaded<TaskInformation> serializedTaskInformation,
            ExecutionAttemptID executionAttemptId,
            AllocationID allocationId,
            @Nullable JobManagerTaskRestore taskRestore,
            List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
            List<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors) {

        this.jobId = Preconditions.checkNotNull(jobId);

        this.serializedJobInformation = Preconditions.checkNotNull(serializedJobInformation);
        this.serializedTaskInformation = Preconditions.checkNotNull(serializedTaskInformation);

        this.executionId = Preconditions.checkNotNull(executionAttemptId);
        this.allocationId = Preconditions.checkNotNull(allocationId);

        this.taskRestore = taskRestore;

        this.producedPartitions = Preconditions.checkNotNull(resultPartitionDeploymentDescriptors);
        this.inputGates = Preconditions.checkNotNull(inputGateDeploymentDescriptors);
    }

    /**
     * Return the sub task's job information.
     *
     * @return job information (may throw {@link IllegalStateException} if {@link #loadBigData} is
     *     not called beforehand).
     * @throws IllegalStateException If job information is offloaded to BLOB store.
     */
    public JobInformation getJobInformation() throws IOException, ClassNotFoundException {
        if (jobInformation != null) {
            return jobInformation;
        }
        if (serializedJobInformation instanceof NonOffloaded) {
            NonOffloaded<JobInformation> jobInformation =
                    (NonOffloaded<JobInformation>) serializedJobInformation;
            return jobInformation.serializedValue.deserializeValue(getClass().getClassLoader());
        }
        throw new IllegalStateException(
                "Trying to work with offloaded serialized job information.");
    }

    /**
     * Return the sub task's task information.
     *
     * @return task information (may throw {@link IllegalStateException} if {@link #loadBigData} is
     *     not called beforehand)).
     * @throws IllegalStateException If job information is offloaded to BLOB store.
     */
    public TaskInformation getTaskInformation() throws IOException, ClassNotFoundException {
        if (taskInformation != null) {
            return taskInformation;
        }
        if (serializedTaskInformation instanceof NonOffloaded) {
            NonOffloaded<TaskInformation> taskInformation =
                    (NonOffloaded<TaskInformation>) serializedTaskInformation;
            return taskInformation.serializedValue.deserializeValue(getClass().getClassLoader());
        }
        throw new IllegalStateException(
                "Trying to work with offloaded serialized task information.");
    }

    /**
     * Returns the task's job ID.
     *
     * @return the job ID this task belongs to
     */
    public JobID getJobId() {
        return jobId;
    }

    public ExecutionAttemptID getExecutionAttemptId() {
        return executionId;
    }

    /**
     * Returns the task's index in the subtask group.
     *
     * @return the task's index in the subtask group
     */
    public int getSubtaskIndex() {
        return executionId.getSubtaskIndex();
    }

    /** Returns the attempt number of the subtask. */
    public int getAttemptNumber() {
        return executionId.getAttemptNumber();
    }

    public List<ResultPartitionDeploymentDescriptor> getProducedPartitions() {
        return producedPartitions;
    }

    public List<InputGateDeploymentDescriptor> getInputGates() {
        return inputGates;
    }

    @Nullable
    public JobManagerTaskRestore getTaskRestore() {
        return taskRestore;
    }

    public AllocationID getAllocationId() {
        return allocationId;
    }

    /**
     * Loads externalized data from the BLOB store back to the object.
     *
     * @param blobService the blob store to use (may be <tt>null</tt> if {@link
     *     #serializedJobInformation} and {@link #serializedTaskInformation} are non-<tt>null</tt>)
     * @param shuffleDescriptorsCache cache of shuffle descriptors to reduce the cost of
     *     deserialization
     * @throws IOException during errors retrieving or reading the BLOBs
     * @throws ClassNotFoundException Class of a serialized object cannot be found.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将外部化的数据从BLOB存储加载回对象。
     *
    */
    public void loadBigData(
            @Nullable PermanentBlobService blobService,
            GroupCache<JobID, PermanentBlobKey, JobInformation> jobInformationCache,
            GroupCache<JobID, PermanentBlobKey, TaskInformation> taskInformationCache,
            GroupCache<JobID, PermanentBlobKey, ShuffleDescriptorGroup> shuffleDescriptorsCache)
            throws IOException, ClassNotFoundException {

        // re-integrate offloaded job info from blob
        // here, if this fails, we need to throw the exception as there is no backup path anymore
        /**
         * 重新整合从Blob中卸载的作业信息
         *
         */
        if (serializedJobInformation instanceof Offloaded) {
            /** 获取作业信息的Blob键  */
            PermanentBlobKey jobInfoKey =
                    ((Offloaded<JobInformation>) serializedJobInformation).serializedValueKey;
            /** 确保blobService不为空，因为我们需要它来获取文件   */
            Preconditions.checkNotNull(blobService);
            /** 尝试从缓存中获取JobInformation */
            JobInformation jobInformation = jobInformationCache.get(jobId, jobInfoKey);
            /** 如果缓存中没有找到JobInformation  */
            if (jobInformation == null) {
                /** 从blobService中获取JobInformation的文件   */
                final File dataFile = blobService.getFile(jobId, jobInfoKey);
                // NOTE: Do not delete the job info BLOB since it may be needed again during
                // recovery. (it is deleted automatically on the BLOB server and cache when the job
                // enters a terminal state)
                /** 反序列化文件内容，得到JobInformation对象 */
                jobInformation =
                        InstantiationUtil.deserializeObject(
                                new BufferedInputStream(Files.newInputStream(dataFile.toPath())),
                                getClass().getClassLoader());
                /** 将反序列化的JobInformation对象存入缓存 */
                jobInformationCache.put(jobId, jobInfoKey, jobInformation);
            }
            /** 更新当前类的jobInformation字段为JobInformation的深拷贝  */
            this.jobInformation = jobInformation.deepCopy();
        }

        // re-integrate offloaded task info from blob
        /** 检查serializedTaskInformation是否是Offloaded类型的实例 */
        if (serializedTaskInformation instanceof Offloaded) {
            /** 将serializedTaskInformation强制转换为Offloaded<TaskInformation>类型，并获取其serializedValueKey   */
            PermanentBlobKey taskInfoKey =
                    ((Offloaded<TaskInformation>) serializedTaskInformation).serializedValueKey;
            /** 检查blobService是否为null，防止后续使用中出现空指针异常 */
            Preconditions.checkNotNull(blobService);
            /** 尝试从taskInformationCache中根据jobId和taskInfoKey获取TaskInformation */
            TaskInformation taskInformation = taskInformationCache.get(jobId, taskInfoKey);
            /** 如果taskInformation为null，表示缓存中不存在该信息，需要从blobService中加载 */
            if (taskInformation == null) {
                /** 使用blobService根据jobId和taskInfoKey获取对应的数据文件 */
                final File dataFile = blobService.getFile(jobId, taskInfoKey);
                // NOTE: Do not delete the task info BLOB since it may be needed again during
                // recovery. (it is deleted automatically on the BLOB server and cache when the job
                // enters a terminal state)
                /** 从数据文件中反序列化TaskInformation对象 */
                taskInformation =
                        InstantiationUtil.deserializeObject(
                                new BufferedInputStream(Files.newInputStream(dataFile.toPath())),
                                getClass().getClassLoader());
                /** 将反序列化得到的TaskInformation对象放入缓存  */
                taskInformationCache.put(jobId, taskInfoKey, taskInformation);
            }
            /** 对taskInformation进行深拷贝，并赋值给this.taskInformation  */
            this.taskInformation = taskInformation.deepCopy();
        }
        /** 遍历inputGates集合 */
        for (InputGateDeploymentDescriptor inputGate : inputGates) {
            /** 尝试加载并反序列化ShuffleDescriptors */
            inputGate.tryLoadAndDeserializeShuffleDescriptors(
                    blobService, jobId, shuffleDescriptorsCache);
        }
    }

    @Override
    public String toString() {
        return String.format(
                "TaskDeploymentDescriptor [execution id: %s, "
                        + "produced partitions: %s, input gates: %s]",
                executionId,
                collectionToString(producedPartitions),
                collectionToString(inputGates));
    }

    private static String collectionToString(Iterable<?> collection) {
        final StringBuilder strBuilder = new StringBuilder();

        strBuilder.append("[");

        for (Object elem : collection) {
            strBuilder.append(elem);
        }

        strBuilder.append("]");

        return strBuilder.toString();
    }
}
