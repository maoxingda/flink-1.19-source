/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.DefaultSlotPoolServiceSchedulerFactory;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobMasterConfiguration;
import org.apache.flink.runtime.jobmaster.JobMasterServiceLeadershipRunner;
import org.apache.flink.runtime.jobmaster.SlotPoolServiceSchedulerFactory;
import org.apache.flink.runtime.jobmaster.factories.DefaultJobMasterServiceFactory;
import org.apache.flink.runtime.jobmaster.factories.DefaultJobMasterServiceProcessFactory;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.Preconditions;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Factory which creates a {@link JobMasterServiceLeadershipRunner}. */
public enum JobMasterServiceLeadershipRunnerFactory implements JobManagerRunnerFactory {
    INSTANCE;

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建 JobManagerRunner 实例
    */
    @Override
    public JobManagerRunner createJobManagerRunner(
            JobGraph jobGraph,
            Configuration configuration,
            RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices,
            HeartbeatServices heartbeatServices,
            JobManagerSharedServices jobManagerServices,
            JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
            FatalErrorHandler fatalErrorHandler,
            Collection<FailureEnricher> failureEnrichers,
            long initializationTimestamp)
            throws Exception {
        /**
         * taskVertices.size()
         * 如果没有顶点，说明作业是空的，方法将抛出异常
         */
        checkArgument(jobGraph.getNumberOfVertices() > 0, "The given job is empty");

        /**
         * 从 configuration 中解析 JobMasterConfiguration，这通常包含作业管理器的特定配置
         * 里面更多的是超时时间
         */
        final JobMasterConfiguration jobMasterConfiguration =
                JobMasterConfiguration.fromConfiguration(configuration);
        /**
         * 从 highAvailabilityServices 中获取 JobResultStore 用于存储作业结果
         */
        final JobResultStore jobResultStore = highAvailabilityServices.getJobResultStore();
        /**
         * 从 highAvailabilityServices 中获取 JobManagerLeaderElection，进行作业管理器的领导者选举。
         */
        final LeaderElection jobManagerLeaderElection =
                highAvailabilityServices.getJobManagerLeaderElection(jobGraph.getJobID());
        /**
         * 使用 DefaultSlotPoolServiceSchedulerFactory 从配置中创建 SlotPoolServiceSchedulerFactory。
         * 这个工厂用于生成调度器，用于分配和管理作业所需的计算资源（称为槽位）。
         * SlotPoolServiceSchedulerFactory 是 SlotPoolService、SchedulerNG的工厂
         * SlotPoolService：JobMaster 用于管理Slot的服务。
         * SchedulerNG：用于调度Flink作业的接口。
         */
        final SlotPoolServiceSchedulerFactory slotPoolServiceSchedulerFactory =
                DefaultSlotPoolServiceSchedulerFactory.fromConfiguration(
                        configuration, jobGraph.getJobType(), jobGraph.isDynamic());

        /**
         * 如果作业管理器的配置中指定的调度器模式是 REACTIVE（响应式），
         * 则检查 slotPoolServiceSchedulerFactory 生成的调度器类型是否为 Adaptive。
         * 响应式模式需要自适应调度器，如果不是，方法将抛出状态异常。
         */
        if (jobMasterConfiguration.getConfiguration().get(JobManagerOptions.SCHEDULER_MODE)
                == SchedulerExecutionMode.REACTIVE) {
            Preconditions.checkState(
                    slotPoolServiceSchedulerFactory.getSchedulerType()
                            == JobManagerOptions.SchedulerType.Adaptive,
                    "Adaptive Scheduler is required for reactive mode");
        }

        /**
         * 从 jobManagerServices 中获取 LibraryCacheManager，并为其注册一个类加载器租约。
         * 类加载器租约用于管理作业执行期间使用的类加载器，以确保作业可以正确加载所需的库和依赖项。
         */
        final LibraryCacheManager.ClassLoaderLease classLoaderLease =
                jobManagerServices
                        .getLibraryCacheManager()
                        .registerClassLoaderLease(jobGraph.getJobID());
        /**
         * 使用 classLoaderLease 对象来获取或解析用户代码的类加载器。
         * jobGraph.getUserJarBlobKeys() 和 jobGraph.getClasspaths() 是指向用户作业 JAR 文件和类路径的键或标识符。
         * getOrResolveClassLoader 方法利用这些信息来构造或获取一个类加载器，该类加载器用于加载用户作业中的类。
         */
        final ClassLoader userCodeClassLoader =
                classLoaderLease
                        .getOrResolveClassLoader(
                                jobGraph.getUserJarBlobKeys(), jobGraph.getClasspaths())
                        .asClassLoader();
        /**
         * 创建了一个 DefaultJobMasterServiceFactory 实例，该工厂用于创建 JobMasterService 对象。
         * JobMasterService 是作业管理器的一个核心组件，它负责协调和管理作业的执行。
         */
        final DefaultJobMasterServiceFactory jobMasterServiceFactory =
                new DefaultJobMasterServiceFactory(
                        jobManagerServices.getIoExecutor(),
                        rpcService,
                        jobMasterConfiguration,
                        jobGraph,
                        highAvailabilityServices,
                        slotPoolServiceSchedulerFactory,
                        jobManagerServices,
                        heartbeatServices,
                        jobManagerJobMetricGroupFactory,
                        fatalErrorHandler,
                        userCodeClassLoader,
                        failureEnrichers,
                        initializationTimestamp);
        /**
         * 创建了一个 DefaultJobMasterServiceProcessFactory 实例，它用于创建 JobMasterServiceProcess 对象。
         * JobMasterServiceProcess 封装了作业主服务的执行逻辑，并提供了对外部事件的响应。
         */
        final DefaultJobMasterServiceProcessFactory jobMasterServiceProcessFactory =
                new DefaultJobMasterServiceProcessFactory(
                        jobGraph.getJobID(),
                        jobGraph.getName(),
                        jobGraph.getCheckpointingSettings(),
                        initializationTimestamp,
                        jobMasterServiceFactory);
        /**
         * 代码创建并返回了一个 JobMasterServiceLeadershipRunner 实例。这个类负责处理作业主服务的领导者选举和领导逻辑。
         */
        return new JobMasterServiceLeadershipRunner(
                jobMasterServiceProcessFactory,
                jobManagerLeaderElection,
                jobResultStore,
                classLoaderLease,
                fatalErrorHandler);
    }
}
