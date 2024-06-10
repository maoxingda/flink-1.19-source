/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

/**
 * A class to execute a {@link SnapshotStrategy}. It can execute a strategy either synchronously or
 * asynchronously. It takes care of common logging and resource cleaning.
 *
 * @param <T> type of the snapshot result.
 */
public final class SnapshotStrategyRunner<T extends StateObject, SR extends SnapshotResources> {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotStrategyRunner.class);

    private static final String LOG_SYNC_COMPLETED_TEMPLATE =
            "{} ({}, synchronous part) in thread {} took {} ms.";
    private static final String LOG_ASYNC_COMPLETED_TEMPLATE =
            "{} ({}, asynchronous part) in thread {} took {} ms.";

    /**
     * Descriptive name of the snapshot strategy that will appear in the log outputs and {@link
     * #toString()}.
     */
    @Nonnull private final String description;

    @Nonnull private final SnapshotStrategy<T, SR> snapshotStrategy;
    @Nonnull private final CloseableRegistry cancelStreamRegistry;

    @Nonnull private final SnapshotExecutionType executionType;

    public SnapshotStrategyRunner(
            @Nonnull String description,
            @Nonnull SnapshotStrategy<T, SR> snapshotStrategy,
            @Nonnull CloseableRegistry cancelStreamRegistry,
            @Nonnull SnapshotExecutionType executionType) {
        this.description = description;
        this.snapshotStrategy = snapshotStrategy;
        this.cancelStreamRegistry = cancelStreamRegistry;
        this.executionType = executionType;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建一个异步快照任务，该任务将执行快照并返回一个包含快照结果的RunnableFuture对象。
     *
     * @param checkpointId 检查点的唯一标识符
     * @param timestamp 时间戳，表示快照的时间点
     * @param streamFactory 用于创建检查点流的工厂
     * @param checkpointOptions 检查点的选项配置
     * @param <T> 快照结果的类型
     * @return 一个RunnableFuture对象，表示异步快照任务，其执行结果将是SnapshotResult<T>类型
     * @throws Exception 如果在准备资源或执行快照时发生异常
    */
    @Nonnull
    public final RunnableFuture<SnapshotResult<T>> snapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {
        // 记录快照开始的时间
        long startTime = System.currentTimeMillis();
        // 使用快照策略同步准备资源
        SR snapshotResources = snapshotStrategy.syncPrepareResources(checkpointId);
        // 记录同步准备资源完成的信息
        logCompletedInternal(LOG_SYNC_COMPLETED_TEMPLATE, streamFactory, startTime);
        // 调用快照策略的异步快照方法，获取一个SnapshotResultSupplier对象
        SnapshotStrategy.SnapshotResultSupplier<T> asyncSnapshot =
                snapshotStrategy.asyncSnapshot(
                        snapshotResources,
                        checkpointId,
                        timestamp,
                        streamFactory,
                        checkpointOptions);
        // 创建一个FutureTask，它封装了一个Callable任务，该任务会调用SnapshotResultSupplier的get方法
        // 来获取快照结果，并在完成后释放资源
        FutureTask<SnapshotResult<T>> asyncSnapshotTask =
                new AsyncSnapshotCallable<SnapshotResult<T>>() {
                    // 调用SnapshotResultSupplier的get方法获取快照结果
                    @Override
                    protected SnapshotResult<T> callInternal() throws Exception {
                        return asyncSnapshot.get(snapshotCloseableRegistry);
                    }

                    // 释放之前同步准备的快照资源
                    @Override
                    protected void cleanupProvidedResources() {
                        if (snapshotResources != null) {
                            snapshotResources.release();
                        }
                    }

                    // 记录异步快照完成的信息
                    @Override
                    protected void logAsyncSnapshotComplete(long startTime) {
                        logCompletedInternal(
                                LOG_ASYNC_COMPLETED_TEMPLATE, streamFactory, startTime);
                    }
                }.toAsyncSnapshotFutureTask(cancelStreamRegistry);

        // 如果执行类型为SYNCHRONOUS，则立即执行快照任务
        if (executionType == SnapshotExecutionType.SYNCHRONOUS) {
            asyncSnapshotTask.run();
        }
        // 返回快照任务的Future，用于等待任务完成并获取快照结果
        return asyncSnapshotTask;
    }

    private void logCompletedInternal(
            @Nonnull String template, @Nonnull Object checkpointOutDescription, long startTime) {

        long duration = (System.currentTimeMillis() - startTime);

        LOG.debug(
                template, description, checkpointOutDescription, Thread.currentThread(), duration);
    }

    @Override
    public String toString() {
        return "SnapshotStrategy {" + description + "}";
    }
}
