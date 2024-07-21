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

package org.apache.flink.yarn;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.TaskManagerOptionsInternal;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.externalresource.ExternalResourceUtils;
import org.apache.flink.runtime.resourcemanager.active.AbstractResourceManagerDriver;
import org.apache.flink.runtime.resourcemanager.active.ResourceManagerDriver;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.util.ResourceManagerUtils;
import org.apache.flink.runtime.webmonitor.history.HistoryServerUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnResourceManagerDriverConfiguration;

import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Phaser;
import java.util.stream.Collectors;

/** Implementation of {@link ResourceManagerDriver} for Yarn deployment. */
public class YarnResourceManagerDriver extends AbstractResourceManagerDriver<YarnWorkerNode> {

    /**
     * Environment variable name of the hostname given by YARN. In task executor we use the
     * hostnames given by YARN consistently throughout pekko
     */
    static final String ENV_FLINK_NODE_ID = "_FLINK_NODE_ID";

    static final String ERROR_MESSAGE_ON_SHUTDOWN_REQUEST =
            "Received shutdown request from YARN ResourceManager.";

    private final YarnConfiguration yarnConfig;

    /** The process environment variables. */
    private final YarnResourceManagerDriverConfiguration configuration;

    /** Default heartbeat interval between this resource manager and the YARN ResourceManager. */
    private final int yarnHeartbeatIntervalMillis;

    /** The heartbeat interval while the resource master is waiting for containers. */
    private final int containerRequestHeartbeatIntervalMillis;

    /** Request resource futures, keyed by container's TaskExecutorProcessSpec. */
    private final Map<TaskExecutorProcessSpec, Queue<CompletableFuture<YarnWorkerNode>>>
            requestResourceFutures;

    private final RegisterApplicationMasterResponseReflector
            registerApplicationMasterResponseReflector;

    private final YarnResourceManagerClientFactory yarnResourceManagerClientFactory;

    private final YarnNodeManagerClientFactory yarnNodeManagerClientFactory;

    /** Client to communicate with the Resource Manager (YARN's master). */
    /** 客户端与资源管理器（YARN的主机）通信 */
    private AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient;

    /** Client to communicate with the Node manager and launch TaskExecutor processes. */
    /** 客户端与节点管理器通信并启动TaskExecutor流程 */
    private NMClientAsync nodeManagerClient;

    private TaskExecutorProcessSpecContainerResourcePriorityAdapter
            taskExecutorProcessSpecContainerResourcePriorityAdapter;

    private String taskManagerNodeLabel;

    private final Phaser trackerOfReleasedResources;

    private final Set<String> lastBlockedNodes = new HashSet<>();

    private volatile boolean isRunning = false;

    public YarnResourceManagerDriver(
            Configuration flinkConfig,
            YarnResourceManagerDriverConfiguration configuration,
            YarnResourceManagerClientFactory yarnResourceManagerClientFactory,
            YarnNodeManagerClientFactory yarnNodeManagerClientFactory) {
        super(flinkConfig, GlobalConfiguration.loadConfiguration(configuration.getCurrentDir()));

        this.yarnConfig = Utils.getYarnAndHadoopConfiguration(flinkConfig);
        this.requestResourceFutures = new HashMap<>();
        this.configuration = configuration;

        final int yarnHeartbeatIntervalMS =
                flinkConfig.get(YarnConfigOptions.HEARTBEAT_DELAY_SECONDS) * 1000;

        final long yarnExpiryIntervalMS =
                yarnConfig.getLong(
                        YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
                        YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS);

        if (yarnHeartbeatIntervalMS >= yarnExpiryIntervalMS) {
            log.warn(
                    "The heartbeat interval of the Flink Application master ({}) is greater "
                            + "than YARN's expiry interval ({}). The application is likely to be killed by YARN.",
                    yarnHeartbeatIntervalMS,
                    yarnExpiryIntervalMS);
        }
        yarnHeartbeatIntervalMillis = yarnHeartbeatIntervalMS;
        containerRequestHeartbeatIntervalMillis =
                flinkConfig.get(
                        YarnConfigOptions.CONTAINER_REQUEST_HEARTBEAT_INTERVAL_MILLISECONDS);

        this.taskManagerNodeLabel = flinkConfig.get(YarnConfigOptions.TASK_MANAGER_NODE_LABEL);

        this.registerApplicationMasterResponseReflector =
                new RegisterApplicationMasterResponseReflector(log);

        this.yarnResourceManagerClientFactory = yarnResourceManagerClientFactory;
        this.yarnNodeManagerClientFactory = yarnNodeManagerClientFactory;
        this.trackerOfReleasedResources = new Phaser();
    }

    // ------------------------------------------------------------------------
    //  ResourceManagerDriver
    // ------------------------------------------------------------------------

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 初始化ResourceManagerDriver
    */
    @Override
    protected void initializeInternal() throws Exception {
        // 标记资源管理器为正在运行状态
        isRunning = true;
        // 创建一个YarnContainerEventHandler对象，用于处理与YARN容器相关的事件
        final YarnContainerEventHandler yarnContainerEventHandler = new YarnContainerEventHandler();
        try {
            // 创建一个ResourceManagerClient对象，该对象负责与YARN资源管理器进行通信
            // 参数包括心跳间隔、事件处理器等
            resourceManagerClient =
                    yarnResourceManagerClientFactory.createResourceManagerClient(
                            yarnHeartbeatIntervalMillis, yarnContainerEventHandler);
            // 使用YARN配置初始化ResourceManagerClient
            resourceManagerClient.init(yarnConfig);
            // 启动ResourceManagerClient
            resourceManagerClient.start();
            // 向YARN注册应用程序主节点(Application Master)，并获取响应
            final RegisterApplicationMasterResponse registerApplicationMasterResponse =
                    registerApplicationMaster();
            getContainersFromPreviousAttempts(registerApplicationMasterResponse);
            // 创建一个适配器，用于将YARN的资源限制转换为Flink任务执行器的配置
            // 该适配器使用注册响应中的最大资源能力，并获取Flink配置中定义的外部资源配置键
            taskExecutorProcessSpecContainerResourcePriorityAdapter =
                    new TaskExecutorProcessSpecContainerResourcePriorityAdapter(
                            registerApplicationMasterResponse.getMaximumResourceCapability(),
                            ExternalResourceUtils.getExternalResourceConfigurationKeys(
                                    flinkConfig,
                                    YarnConfigOptions.EXTERNAL_RESOURCE_YARN_CONFIG_KEY_SUFFIX));
            // 如果在上面的过程中出现异常，则捕获并抛出ResourceManagerException异常
        } catch (Exception e) {
            throw new ResourceManagerException("Could not start resource manager client.", e);
        }

        // 创建一个NodeManagerClient对象，该对象负责与YARN节点管理器进行通信
        nodeManagerClient =
                yarnNodeManagerClientFactory.createNodeManagerClient(yarnContainerEventHandler);
        // 使用YARN配置初始化NodeManagerClient
        nodeManagerClient.init(yarnConfig);
        // 启动NodeManagerClient
        nodeManagerClient.start();
    }

    @Override
    public void terminate() throws Exception {
        isRunning = false;
        // wait for all containers to stop
        trackerOfReleasedResources.register();
        trackerOfReleasedResources.arriveAndAwaitAdvance();

        // shut down all components
        Exception exception = null;

        if (resourceManagerClient != null) {
            try {
                resourceManagerClient.stop();
            } catch (Exception e) {
                exception = e;
            }
        }

        if (nodeManagerClient != null) {
            try {
                nodeManagerClient.stop();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    @Override
    public void deregisterApplication(
            ApplicationStatus finalStatus, @Nullable String optionalDiagnostics) {
        // first, de-register from YARN
        final FinalApplicationStatus yarnStatus = getYarnStatus(finalStatus);
        log.info(
                "Unregister application from the YARN Resource Manager with final status {}.",
                yarnStatus);

        final Optional<URL> historyServerURL = HistoryServerUtils.getHistoryServerURL(flinkConfig);

        final String appTrackingUrl = historyServerURL.map(URL::toString).orElse("");

        try {
            resourceManagerClient.unregisterApplicationMaster(
                    yarnStatus, optionalDiagnostics, appTrackingUrl);
        } catch (YarnException | IOException e) {
            log.error("Could not unregister the application master.", e);
        }

        Utils.deleteApplicationFiles(configuration.getYarnFiles());
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 请求资源
     */
    @Override
    public CompletableFuture<YarnWorkerNode> requestResource(
            TaskExecutorProcessSpec taskExecutorProcessSpec) {
        // 检查是否已经初始化，如果未初始化则可能抛出异常或返回错误
        checkInitialized();
        // 创建一个CompletableFuture对象，用于异步地处理资源请求的结果
        final CompletableFuture<YarnWorkerNode> requestResourceFuture = new CompletableFuture<>();
        // 尝试将TaskExecutorProcessSpec转换为Yarn可以理解的优先级和资源规范
        final Optional<TaskExecutorProcessSpecContainerResourcePriorityAdapter.PriorityAndResource>
                priorityAndResourceOpt =
                        taskExecutorProcessSpecContainerResourcePriorityAdapter
                                .getPriorityAndResource(taskExecutorProcessSpec);
        // 如果没有找到合适的优先级和资源规范
        if (!priorityAndResourceOpt.isPresent()) {
            // 使用CompletableFuture的completeExceptionally方法将异常作为结果，表示请求失败
            requestResourceFuture.completeExceptionally(
                    new ResourceManagerException(
                            String.format(
                                    "Could not compute the container Resource from the given TaskExecutorProcessSpec %s. "
                                            + "This usually indicates the requested resource is larger than Yarn's max container resource limit.",
                                    taskExecutorProcessSpec)));
        } else {
            // 提取出优先级和资源规范
            final Priority priority = priorityAndResourceOpt.get().getPriority();
            final Resource resource = priorityAndResourceOpt.get().getResource();

            FutureUtils.assertNoException(
                    requestResourceFuture.handle(
                            (ignore, t) -> {
                                if (t == null) {
                                    return null;
                                }
                                // 如果捕获到的是CancellationException实例，即某个请求被取消
                                if (t instanceof CancellationException) {
                                    // 从requestResourceFutures映射中获取与当前任务执行器进程规范（taskExecutorProcessSpec）相关联的待处理资源请求Future队列
                                    // 如果映射中不存在该规范，则默认返回一个空的LinkedList
                                    final Queue<CompletableFuture<YarnWorkerNode>>
                                            pendingRequestResourceFutures =
                                                    requestResourceFutures.getOrDefault(
                                                            taskExecutorProcessSpec,
                                                            new LinkedList<>());
                                    // 检查并从队列中移除指定的资源请求Future
                                    // Preconditions.checkState确保操作后队列中不包含该Future，如果还包含则抛出异常
                                    Preconditions.checkState(
                                            pendingRequestResourceFutures.remove(
                                                    requestResourceFuture));
                                    // 记录日志信息，说明取消了具有特定优先级的挂起请求，并显示剩余挂起容器请求的数量
                                    log.info(
                                            "cancelling pending request with priority {}, remaining {} pending container requests.",
                                            priority,
                                            pendingRequestResourceFutures.size());
                                    // 计算取消前的挂起请求数量
                                    int pendingRequestsSizeBeforeCancel =
                                            pendingRequestResourceFutures.size() + 1;
                                    // 获取并检查与指定优先级和资源相关的挂起容器请求的一致性，然后获取其迭代器
                                    final Iterator<AMRMClient.ContainerRequest>
                                            pendingContainerRequestIterator =
                                                    getPendingRequestsAndCheckConsistency(
                                                                    priority,
                                                                    resource,
                                                                    pendingRequestsSizeBeforeCancel)
                                                            .iterator();
                                    // Preconditions.checkState确保迭代器中至少有一个元素，即至少存在一个挂起的容器请求
                                    // 这在逻辑上是一种检查，确保系统状态与预期一致
                                    Preconditions.checkState(
                                            pendingContainerRequestIterator.hasNext());
                                    // 从迭代器中获取下一个挂起的容器请求
                                    final AMRMClient.ContainerRequest pendingRequest =
                                            pendingContainerRequestIterator.next();
                                    // 从内部数据结构中移除这个挂起的容器请求
                                    removeContainerRequest(pendingRequest);
                                    // 如果与当前任务执行器进程规范相关的所有挂起资源请求Future队列为空
                                    if (pendingRequestResourceFutures.isEmpty()) {
                                        // 则从requestResourceFutures映射中移除该任务执行器进程规范的条目
                                        requestResourceFutures.remove(taskExecutorProcessSpec);
                                    }
                                    // 如果当前请求但尚未分配的工作节点数量小于等于0
                                    if (getNumRequestedNotAllocatedWorkers() <= 0) {
                                        // 调整ResourceManager客户端的心跳间隔为正常的YARN心跳间隔
                                        resourceManagerClient.setHeartbeatInterval(
                                                yarnHeartbeatIntervalMillis);
                                    }
                                    // 如果不是CancellationException异常，则记录错误并重新抛出异常
                                } else {
                                    log.error("Error completing resource request.", t);
                                    ExceptionUtils.rethrow(t);
                                }
                                return null;
                            }));
            // todo 向ResourceManager添加一个新的容器请求
            addContainerRequest(resource, priority);

            // make sure we transmit the request fast and receive fast news of granted allocations
            // 通过调整心跳间隔来实现
            resourceManagerClient.setHeartbeatInterval(containerRequestHeartbeatIntervalMillis);
           // 尝试从requestResourceFutures映射中获取与任务执行器进程规范相关联的Future队列
            requestResourceFutures
                    .computeIfAbsent(taskExecutorProcessSpec, ignore -> new LinkedList<>())
                    .add(requestResourceFuture);
            // 记录日志信息，说明正在请求新的任务执行器容器及其资源和优先级
            log.info(
                    "Requesting new TaskExecutor container with resource {}, priority {}.",
                    taskExecutorProcessSpec,
                    priority);
        }
        // 方法返回请求资源的Future，以便调用者可以异步获取结果
        return requestResourceFuture;
    }

    private void tryUpdateApplicationBlockList() {
        Set<String> currentBlockedNodes = getBlockedNodeRetriever().getAllBlockedNodeIds();
        if (!currentBlockedNodes.equals(lastBlockedNodes)) {
            AMRMClientAsyncReflector.INSTANCE.tryUpdateBlockList(
                    resourceManagerClient,
                    new ArrayList<>(getDifference(currentBlockedNodes, lastBlockedNodes)),
                    new ArrayList<>(getDifference(lastBlockedNodes, currentBlockedNodes)));
            this.lastBlockedNodes.clear();
            this.lastBlockedNodes.addAll(currentBlockedNodes);
        }
    }

    private static Set<String> getDifference(Set<String> setA, Set<String> setB) {
        Set<String> difference = new HashSet<>(setA);
        difference.removeAll(setB);
        return difference;
    }

    @Override
    public void releaseResource(YarnWorkerNode workerNode) {
        final Container container = workerNode.getContainer();
        log.info("Stopping container {}.", workerNode.getResourceID().getStringWithMetadata());
        trackerOfReleasedResources.register();
        nodeManagerClient.stopContainerAsync(container.getId(), container.getNodeId());
        resourceManagerClient.releaseAssignedContainer(container.getId());
    }

    // ------------------------------------------------------------------------
    //  Internal
    // ------------------------------------------------------------------------

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 按优先级处理已分配的容器
     */
    private void onContainersOfPriorityAllocated(Priority priority, List<Container> containers) {
        // 尝试通过优先级获取任务执行器进程规范和资源适配器
        final Optional<
                        TaskExecutorProcessSpecContainerResourcePriorityAdapter
                                .TaskExecutorProcessSpecAndResource>
                taskExecutorProcessSpecAndResourceOpt =
                        taskExecutorProcessSpecContainerResourcePriorityAdapter
                                .getTaskExecutorProcessSpecAndResource(priority);
         // 验证是否成功获取到了对应优先级的任务执行器进程规范和资源
        // 如果没有，则抛出异常，表示接收到了未识别的优先级容器
        Preconditions.checkState(
                taskExecutorProcessSpecAndResourceOpt.isPresent(),
                "Receive %s containers with unrecognized priority %s. This should not happen.",
                containers.size(),
                priority.getPriority());
        // 从Optional中获取任务执行器进程规范和资源
        final TaskExecutorProcessSpec taskExecutorProcessSpec =
                taskExecutorProcessSpecAndResourceOpt.get().getTaskExecutorProcessSpec();
        final Resource resource = taskExecutorProcessSpecAndResourceOpt.get().getResource();
        // 根据任务执行器进程规范获取或初始化一个待处理的资源请求队列
        final Queue<CompletableFuture<YarnWorkerNode>> pendingRequestResourceFutures =
                requestResourceFutures.getOrDefault(taskExecutorProcessSpec, new LinkedList<>());
        // 记录日志，说明接收到的容器数量、优先级以及待处理的容器请求数量
        log.info(
                "Received {} containers with priority {}, {} pending container requests.",
                containers.size(),
                priority,
                pendingRequestResourceFutures.size());
        // 获取容器的迭代器
        final Iterator<Container> containerIterator = containers.iterator();
        // 获取并检查待处理的容器请求，确保与接收到的容器数量和资源一致性的迭代器
        final Iterator<AMRMClient.ContainerRequest> pendingContainerRequestIterator =
                getPendingRequestsAndCheckConsistency(
                                priority, resource, pendingRequestResourceFutures.size())
                        .iterator();
        // 计数器，记录接受的容器数量
        int numAccepted = 0;
        // 循环处理每个已分配的容器和对应的待处理请求，直到任一迭代器耗尽
        while (containerIterator.hasNext() && pendingContainerRequestIterator.hasNext()) {
            // 从容器迭代器中获取下一个容器
            final Container container = containerIterator.next();
            // 从待处理请求迭代器中获取对应的请求
            final AMRMClient.ContainerRequest pendingRequest =
                    pendingContainerRequestIterator.next();
            // 获取容器的资源ID
            final ResourceID resourceId = getContainerResourceId(container);
            // 从待处理资源请求队列中移除并获取对应的CompletableFuture
            final CompletableFuture<YarnWorkerNode> requestResourceFuture =
                    pendingRequestResourceFutures.poll();
            // 确保成功获取到CompletableFuture，否则抛出异常
            Preconditions.checkState(requestResourceFuture != null);
            // 如果队列为空，则从全局映射中移除该任务执行器进程规范的条目
            if (pendingRequestResourceFutures.isEmpty()) {
                requestResourceFutures.remove(taskExecutorProcessSpec);
            }
            // 完成CompletableFuture，表示资源请求已成功处理
            requestResourceFuture.complete(new YarnWorkerNode(container, resourceId));
            // todo 异步启动任务执行器
            startTaskExecutorInContainerAsync(container, taskExecutorProcessSpec, resourceId);
            // 从待处理请求中移除已处理的请求
            removeContainerRequest(pendingRequest);
            // 增加接受的容器数量计数器
            numAccepted++;
        }
        // 计数器，记录超出预期的容器数量
        int numExcess = 0;
        // 如果还有剩余的容器未被处理（即超出预期的容器），则处理它们
        while (containerIterator.hasNext()) {
            // 返回或处理超出的容器
            returnExcessContainer(containerIterator.next());
            // 增加超出预期的容器数量计数器
            numExcess++;
        }
        //打印日志
        log.info(
                "Accepted {} requested containers, returned {} excess containers, {} pending container requests of resource {}.",
                numAccepted,
                numExcess,
                pendingRequestResourceFutures.size(),
                resource);
    }

    private int getNumRequestedNotAllocatedWorkers() {
        return requestResourceFutures.values().stream().mapToInt(Queue::size).sum();
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 添加容器请求
     */
    private void addContainerRequest(Resource resource, Priority priority) {
        // update blocklist
        // 更新黑名单，这里可能是指更新一些不适合部署任务节点的黑名单信息
        tryUpdateApplicationBlockList();
        // 使用ContainerRequestReflector（可能是一个工具类或单例，用于反射或封装创建ContainerRequest的细节）
        // 来根据给定的资源、优先级和任务管理器节点标签创建一个ContainerRequest对象
        AMRMClient.ContainerRequest containerRequest =
                ContainerRequestReflector.INSTANCE.getContainerRequest(
                        resource, priority, taskManagerNodeLabel);
        // 向ResourceManager客户端添加这个容器请求
        // ResourceManager客户端负责与YARN ResourceManager通信，提交资源请求
        resourceManagerClient.addContainerRequest(containerRequest);
    }

    private void removeContainerRequest(AMRMClient.ContainerRequest pendingContainerRequest) {
        log.info("Removing container request {}.", pendingContainerRequest);
        resourceManagerClient.removeContainerRequest(pendingContainerRequest);
    }

    private void returnExcessContainer(Container excessContainer) {
        log.info("Returning excess container {}.", excessContainer.getId());
        resourceManagerClient.releaseAssignedContainer(excessContainer.getId());
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 异步启动任务执行器在指定的容器中。
     *
     * @param container         要启动任务执行器的容器。
     * @param taskExecutorProcessSpec 任务执行器的进程规范，包含启动所需的配置和参数。
     * @param resourceId        资源ID，用于标识任务执行器所在的资源。
     */
    private void startTaskExecutorInContainerAsync(
            Container container,
            TaskExecutorProcessSpec taskExecutorProcessSpec,
            ResourceID resourceId) {
        // 异步创建一个包含任务执行器启动信息的容器启动上下文
        final CompletableFuture<ContainerLaunchContext> containerLaunchContextFuture =
                FutureUtils.supplyAsync(
                        () ->
                                // todo 使用lambda表达式提供任务，该任务根据资源ID、容器的主机名和任务执行器进程规范来创建启动上下文
                                createTaskExecutorLaunchContext(
                                        resourceId,
                                        container.getNodeId().getHost(),
                                        taskExecutorProcessSpec),
                        getIoExecutor());
        // 处理容器启动上下文的异步结果
        FutureUtils.assertNoException(
                containerLaunchContextFuture.handleAsync(
                        (context, exception) -> {
                            if (exception == null) {
                                nodeManagerClient.startContainerAsync(container, context);
                            } else {
                                getResourceEventHandler()
                                        .onWorkerTerminated(resourceId, exception.getMessage());
                            }
                            return null;
                        },
                        getMainThreadExecutor()));
    }

    private Collection<AMRMClient.ContainerRequest> getPendingRequestsAndCheckConsistency(
            Priority priority, Resource resource, int expectedNum) {
        final List<AMRMClient.ContainerRequest> matchingRequests =
                resourceManagerClient.getMatchingRequests(priority, ResourceRequest.ANY, resource)
                        .stream()
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList());

        Preconditions.checkState(
                matchingRequests.size() == expectedNum,
                "The RMClient's and YarnResourceManagers internal state about the number of pending container requests for priority %s has diverged. "
                        + "Number client's pending container requests %s != Number RM's pending container requests %s.",
                priority.getPriority(),
                matchingRequests.size(),
                expectedNum);

        return matchingRequests;
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 创建一个任务执行器的启动上下文。
     *
     * @param containerId       资源ID，用于标识容器和任务执行器。
     * @param host              容器所在的主机名。
     * @param taskExecutorProcessSpec 任务执行器的进程规范，包含启动所需的配置和参数。
     * @return 返回一个配置好的任务执行器启动上下文。
     */
    private ContainerLaunchContext createTaskExecutorLaunchContext(
            ResourceID containerId, String host, TaskExecutorProcessSpec taskExecutorProcessSpec)
            throws Exception {

        // init the ContainerLaunchContext
        // 初始化当前工作目录
        final String currDir = configuration.getCurrentDir();
        // 根据Flink配置和任务执行器进程规范创建ContaineredTaskManagerParameters
        final ContaineredTaskManagerParameters taskManagerParameters =
                ContaineredTaskManagerParameters.create(flinkConfig, taskExecutorProcessSpec);
        // 记录日志，表明任务执行器将在哪个主机上以什么规范启动
        log.info(
                "TaskExecutor {} will be started on {} with {}.",
                containerId.getStringWithMetadata(),
                host,
                taskExecutorProcessSpec);
        // 克隆Flink配置，并设置任务管理器的资源ID及其元数据
        final Configuration taskManagerConfig = BootstrapTools.cloneConfiguration(flinkConfig);
        taskManagerConfig.set(
                TaskManagerOptions.TASK_MANAGER_RESOURCE_ID, containerId.getResourceIdString());
        taskManagerConfig.set(
                TaskManagerOptionsInternal.TASK_MANAGER_RESOURCE_ID_METADATA,
                containerId.getMetadata());
        // 获取动态属性并将其转换为字符串，这些动态属性将作为启动参数传递给任务管理器
        final String taskManagerDynamicProperties =
                BootstrapTools.getDynamicPropertiesAsString(flinkClientConfig, taskManagerConfig);
        // 记录任务管理器的配置
        log.debug("TaskManager configuration: {}", taskManagerConfig);
        //   todo 使用各种配置和参数创建任务执行器的启动上下文
        //YarnTaskExecutorRunner
        final ContainerLaunchContext taskExecutorLaunchContext =
                Utils.createTaskExecutorContext(
                        flinkConfig,
                        yarnConfig,
                        configuration,
                        taskManagerParameters,
                        taskManagerDynamicProperties,
                        currDir,
                        YarnTaskExecutorRunner.class,
                        log);
        // 在启动上下文的环境变量中添加Flink节点ID（即主机名）
        taskExecutorLaunchContext.getEnvironment().put(ENV_FLINK_NODE_ID, host);
        // 返回配置好的任务执行器启动上下文
        return taskExecutorLaunchContext;
    }

    @VisibleForTesting
    Optional<Resource> getContainerResource(TaskExecutorProcessSpec taskExecutorProcessSpec) {
        Optional<TaskExecutorProcessSpecContainerResourcePriorityAdapter.PriorityAndResource> opt =
                taskExecutorProcessSpecContainerResourcePriorityAdapter.getPriorityAndResource(
                        taskExecutorProcessSpec);

        if (!opt.isPresent()) {
            return Optional.empty();
        }

        return Optional.of(opt.get().getResource());
    }

    private RegisterApplicationMasterResponse registerApplicationMaster() throws Exception {
        return resourceManagerClient.registerApplicationMaster(
                configuration.getRpcAddress(),
                ResourceManagerUtils.parseRestBindPortFromWebInterfaceUrl(
                        configuration.getWebInterfaceUrl()),
                configuration.getWebInterfaceUrl());
    }

    private void getContainersFromPreviousAttempts(
            final RegisterApplicationMasterResponse registerApplicationMasterResponse) {
        final List<Container> containersFromPreviousAttempts =
                registerApplicationMasterResponseReflector.getContainersFromPreviousAttempts(
                        registerApplicationMasterResponse);
        final List<YarnWorkerNode> recoveredWorkers = new ArrayList<>();

        log.info(
                "Recovered {} containers from previous attempts ({}).",
                containersFromPreviousAttempts.size(),
                containersFromPreviousAttempts);

        for (Container container : containersFromPreviousAttempts) {
            final YarnWorkerNode worker =
                    new YarnWorkerNode(container, getContainerResourceId(container));
            recoveredWorkers.add(worker);
        }

        getResourceEventHandler().onPreviousAttemptWorkersRecovered(recoveredWorkers);
    }

    // ------------------------------------------------------------------------
    //  Utility methods
    // ------------------------------------------------------------------------

    /**
     * Converts a Flink application status enum to a YARN application status enum.
     *
     * @param status The Flink application status.
     * @return The corresponding YARN application status.
     */
    private FinalApplicationStatus getYarnStatus(ApplicationStatus status) {
        if (status == null) {
            return FinalApplicationStatus.UNDEFINED;
        } else {
            switch (status) {
                case SUCCEEDED:
                    return FinalApplicationStatus.SUCCEEDED;
                case FAILED:
                    return FinalApplicationStatus.FAILED;
                case CANCELED:
                    return FinalApplicationStatus.KILLED;
                default:
                    return FinalApplicationStatus.UNDEFINED;
            }
        }
    }

    @VisibleForTesting
    private static ResourceID getContainerResourceId(Container container) {
        return new ResourceID(container.getId().toString(), container.getNodeId().toString());
    }

    private Map<Priority, List<Container>> groupContainerByPriority(List<Container> containers) {
        return containers.stream().collect(Collectors.groupingBy(Container::getPriority));
    }

    private void checkInitialized() {
        Preconditions.checkState(
                taskExecutorProcessSpecContainerResourcePriorityAdapter != null,
                "Driver not initialized.");
    }

    // ------------------------------------------------------------------------
    //  Event handlers
    // ------------------------------------------------------------------------

    class YarnContainerEventHandler
            implements AMRMClientAsync.CallbackHandler, NMClientAsync.CallbackHandler {

        @Override
        public void onContainersCompleted(List<ContainerStatus> statuses) {
            runAsyncWithFatalHandler(
                    () -> {
                        checkInitialized();
                        log.debug(
                                "YARN ResourceManager reported the following containers completed: {}.",
                                statuses);
                        for (final ContainerStatus containerStatus : statuses) {

                            final String containerId = containerStatus.getContainerId().toString();
                            getResourceEventHandler()
                                    .onWorkerTerminated(
                                            new ResourceID(containerId),
                                            getContainerCompletedCause(containerStatus));
                        }
                    });
        }

        /**
         * @授课老师: 码界探索
         * @微信: 252810631
         * @版权所有: 请尊重劳动成果
         * 当容器分配完成时调用此方法
         */
        @Override
        public void onContainersAllocated(List<Container> containers) {
            // 使用异步方式执行，并配备错误处理机制
            runAsyncWithFatalHandler(
                    () -> {
                        // 检查系统是否已经初始化
                        checkInitialized();
                        // 记录日志，说明接收到的容器数量
                        log.info("Received {} containers.", containers.size());
                        // 根据优先级对容器进行分组
                        for (Map.Entry<Priority, List<Container>> entry :
                                groupContainerByPriority(containers).entrySet()) {
                            // 对每个优先级的容器集合执行特定的处理
                            onContainersOfPriorityAllocated(entry.getKey(), entry.getValue());
                        }

                        // if we are waiting for no further containers, we can go to the
                        // regular heartbeat interval
                        // 检查是否还有未分配的请求中的工作器（或容器）
                        // 如果没有更多待分配的请求，则可以将心跳间隔设置为常规的心跳间隔
                        if (getNumRequestedNotAllocatedWorkers() <= 0) {
                            resourceManagerClient.setHeartbeatInterval(yarnHeartbeatIntervalMillis);
                        }
                    });
        }

        private void runAsyncWithFatalHandler(Runnable runnable) {
            getMainThreadExecutor()
                    .execute(
                            () -> {
                                try {
                                    runnable.run();
                                } catch (Throwable t) {
                                    onError(t);
                                }
                            });
        }

        @Override
        public void onShutdownRequest() {
            getResourceEventHandler()
                    .onError(new ResourceManagerException(ERROR_MESSAGE_ON_SHUTDOWN_REQUEST));
        }

        @Override
        public void onNodesUpdated(List<NodeReport> list) {
            // We are not interested in node updates
        }

        @Override
        public float getProgress() {
            // Temporarily need not record the total size of asked and allocated containers
            return 1;
        }

        @Override
        public void onError(Throwable throwable) {
            if (isRunning) {
                getResourceEventHandler().onError(throwable);
            }
        }

        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> map) {
            log.debug("Succeeded to call YARN Node Manager to start container {}.", containerId);
        }

        @Override
        public void onContainerStatusReceived(
                ContainerId containerId, ContainerStatus containerStatus) {
            // We are not interested in getting container status
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            log.debug("Succeeded to call YARN Node Manager to stop container {}.", containerId);
            trackerOfReleasedResources.arriveAndDeregister();
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable throwable) {
            runAsyncWithFatalHandler(
                    () -> {
                        resourceManagerClient.releaseAssignedContainer(containerId);
                        getResourceEventHandler()
                                .onWorkerTerminated(
                                        new ResourceID(containerId.toString()),
                                        throwable.getMessage());
                    });
        }

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {
            // We are not interested in getting container status
        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable throwable) {
            trackerOfReleasedResources.arriveAndDeregister();
            log.warn(
                    "Error while calling YARN Node Manager to stop container {}.",
                    containerId,
                    throwable);
        }
    }

    public static String getContainerCompletedCause(ContainerStatus containerStatus) {
        final String completeContainerMessage;
        switch (containerStatus.getExitStatus()) {
            case ContainerExitStatus.SUCCESS:
                completeContainerMessage =
                        String.format(
                                "Container %s exited normally. Diagnostics: %s",
                                containerStatus.getContainerId().toString(),
                                containerStatus.getDiagnostics());
                break;
            case ContainerExitStatus.PREEMPTED:
                completeContainerMessage =
                        String.format(
                                "Container %s was preempted by yarn. Diagnostics: %s",
                                containerStatus.getContainerId().toString(),
                                containerStatus.getDiagnostics());
                break;
            case ContainerExitStatus.INVALID:
                completeContainerMessage =
                        String.format(
                                "Container %s was invalid. Diagnostics: %s",
                                containerStatus.getContainerId().toString(),
                                containerStatus.getDiagnostics());
                break;
            case ContainerExitStatus.ABORTED:
                completeContainerMessage =
                        String.format(
                                "Container %s killed by YARN, either due to being released by the application or being 'lost' due to node failures etc. Diagnostics: %s",
                                containerStatus.getContainerId().toString(),
                                containerStatus.getDiagnostics());
                break;
            case ContainerExitStatus.DISKS_FAILED:
                completeContainerMessage =
                        String.format(
                                "Container %s is failed because threshold number of the nodemanager-local-directories or"
                                        + " threshold number of the nodemanager-log-directories have become bad. Diagnostics: %s",
                                containerStatus.getContainerId().toString(),
                                containerStatus.getDiagnostics());
                break;
            default:
                completeContainerMessage =
                        String.format(
                                "Container %s marked as failed.\n Exit code:%s.\n Diagnostics:%s",
                                containerStatus.getContainerId().toString(),
                                containerStatus.getExitStatus(),
                                containerStatus.getDiagnostics());
        }
        return completeContainerMessage;
    }
}
