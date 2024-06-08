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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.Disposable;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * This class implements the logic that creates (and potentially restores) a state backend. The
 * restore logic considers multiple, prioritized options of snapshots to restore from, where all of
 * the options should recreate the same state for the backend. When we fail to restore from the
 * snapshot with the highest priority (typically the "fastest" to restore), we fallback to the next
 * snapshot with the next highest priority. We also take care of cleaning up from failed restore
 * attempts. We only reattempt when the problem occurs during the restore call and will only stop
 * after all snapshot alternatives are exhausted and all failed.
 *
 * @param <T> type of the restored backend.
 * @param <S> type of the supplied snapshots from which the backend restores.
 */
public class BackendRestorerProcedure<T extends Closeable & Disposable, S extends StateObject> {

    /** Logger for this class. */
    private static final Logger LOG = LoggerFactory.getLogger(BackendRestorerProcedure.class);

    /** Factory for new, fresh backends without state. */
    private final FunctionWithException<Collection<S>, T, Exception> instanceSupplier;

    /**
     * This registry is used so that recovery can participate in the task lifecycle, i.e. can be
     * canceled.
     */
    private final CloseableRegistry backendCloseableRegistry;

    /** Description of this instance for logging. */
    private final String logDescription;

    /**
     * Creates a new backend restorer using the given backend supplier and the closeable registry.
     *
     * @param instanceSupplier factory function for new, empty backend instances.
     * @param backendCloseableRegistry registry to allow participation in task lifecycle, e.g. react
     *     to cancel.
     */
    public BackendRestorerProcedure(
            @Nonnull FunctionWithException<Collection<S>, T, Exception> instanceSupplier,
            @Nonnull CloseableRegistry backendCloseableRegistry,
            @Nonnull String logDescription) {

        this.instanceSupplier = Preconditions.checkNotNull(instanceSupplier);
        this.backendCloseableRegistry = Preconditions.checkNotNull(backendCloseableRegistry);
        this.logDescription = logDescription;
    }

    /**
     * Creates a new state backend and restores it from the provided set of state snapshot
     * alternatives.
     *
     * @param restoreOptions list of prioritized state snapshot alternatives for recovery.
     * @return the created (and restored) state backend.
     * @throws Exception if the backend could not be created or restored.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建一个新的状态后端，并从提供的一组状态快照备选方案中恢复它。
     * @param restoreOptions 恢复选项的列表，每个选项包含一组状态
     * @param stats 状态对象大小统计收集器
     * @param <T> 返回的状态对象的类型
     * @param <S> 状态对象的元素类型
     * @return 创建并恢复后的状态对象
     * @throws Exception 如果在创建或恢复过程中发生异常
    */
    @Nonnull
    public T createAndRestore(
            @Nonnull List<? extends Collection<S>> restoreOptions,
            @Nonnull StateObject.StateObjectSizeStatsCollector stats)
            throws Exception {

        if (restoreOptions.isEmpty()) {
            // 如果恢复选项列表为空，则设置一个只包含一个空集合的列表
            restoreOptions = Collections.singletonList(Collections.emptyList());
        }
        // 当前尝试的恢复选项的索引
        int alternativeIdx = 0;
        // 用于收集可能发生的异常
        Exception collectedException = null;

        while (alternativeIdx < restoreOptions.size()) {
            // 获取当前恢复选项中的状态集合
            Collection<S> restoreState = restoreOptions.get(alternativeIdx);
            // 索引自增，以便下次循环尝试下一个恢复选项
            ++alternativeIdx;

            // IMPORTANT: please be careful when modifying the log statements because they are used
            // for validation in
            // the automatic end-to-end tests. Those tests might fail if they are not aligned with
            // the log message!

            if (restoreState.isEmpty()) {
                // 如果状态集合为空，则记录调试日志，表示正在使用空状态创建对象
                LOG.debug("Creating {} with empty state.", logDescription);
            } else {
                if (LOG.isTraceEnabled()) {
                    LOG.trace(
                            "Creating {} and restoring with state {} from alternative ({}/{}).",
                            logDescription,
                            restoreState,
                            alternativeIdx,
                            restoreOptions.size());
                } else {
                    LOG.debug(
                            "Creating {} and restoring with state from alternative ({}/{}).",
                            logDescription,
                            alternativeIdx,
                            restoreOptions.size());
                }
            }

            try {
                // 尝试创建并恢复状态对象
                //这段代码就会调用lamdb表达是
                T successfullyRestored = attemptCreateAndRestore(restoreState);
                // Obtain and report stats for the state objects used in our successful restore
                // 对成功恢复中使用的状态对象获取并报告统计信息
                restoreState.forEach(handle -> handle.collectSizeStats(stats));
                // 返回成功恢复的状态对象
                return successfullyRestored;
            } catch (Exception ex) {
                // 收集并记录异常，如果已有收集到的异常，则保留第一个或更重要的异常
                collectedException = ExceptionUtils.firstOrSuppressed(ex, collectedException);

                if (backendCloseableRegistry.isClosed()) {
                    throw new FlinkException(
                            "Stopping restore attempts for already cancelled task.",
                            collectedException);
                }

                LOG.warn(
                        "Exception while restoring {} from alternative ({}/{}), will retry while more "
                                + "alternatives are available.",
                        logDescription,
                        alternativeIdx,
                        restoreOptions.size(),
                        ex);
            }
        }
        //抛出异常
        throw new FlinkException(
                "Could not restore "
                        + logDescription
                        + " from any of the "
                        + restoreOptions.size()
                        + " provided restore options.",
                collectedException);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建并恢复
    */
    private T attemptCreateAndRestore(Collection<S> restoreState) throws Exception {

        // create a new backend with necessary initialization.
        // 使用提供的初始化信息创建一个新的后端实例
        final T backendInstance = instanceSupplier.apply(restoreState);

        try {
            // register the backend with the registry to participate in task lifecycle w.r.t.
            // cancellation.
            // 将后端实例注册到注册表中，以便参与任务生命周期的管理，特别是在任务取消时
            backendCloseableRegistry.registerCloseable(backendInstance);
            // 如果注册成功，返回创建的后端实例
            return backendInstance;
        } catch (Exception ex) {
            // dispose the backend, e.g. to release native resources, if failed to register it into
            // registry.
            try {
                // 如果注册失败（比如因为资源不足等原因），则尝试释放后端实例占用的资源（如本地资源）
                backendInstance.dispose();
            } catch (Exception disposeEx) {
                ex = ExceptionUtils.firstOrSuppressed(disposeEx, ex);
            }
            //抛出异常
            throw ex;
        }
    }
}
