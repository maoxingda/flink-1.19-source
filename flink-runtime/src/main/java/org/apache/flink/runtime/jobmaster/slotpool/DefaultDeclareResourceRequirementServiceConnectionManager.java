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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.util.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

/**
 * Default implementation of {@link DeclareResourceRequirementServiceConnectionManager}.
 *
 * <p>This connection manager is responsible for sending new resource requirements to the connected
 * service. In case of faults it continues retrying to send the latest resource requirements to the
 * service with an exponential backoff strategy.
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 用于声明资源需求的ServiceConnectionManager。
*/
class DefaultDeclareResourceRequirementServiceConnectionManager
        extends AbstractServiceConnectionManager<
                DeclareResourceRequirementServiceConnectionManager
                        .DeclareResourceRequirementsService>
        implements DeclareResourceRequirementServiceConnectionManager {

    private static final Logger LOG =
            LoggerFactory.getLogger(
                    DefaultDeclareResourceRequirementServiceConnectionManager.class);

    private final ScheduledExecutor scheduledExecutor;

    @Nullable
    @GuardedBy("lock")
    private ResourceRequirements currentResourceRequirements;

    private DefaultDeclareResourceRequirementServiceConnectionManager(
            ScheduledExecutor scheduledExecutor) {
        this.scheduledExecutor = scheduledExecutor;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 声明资源要求
    */
    @Override
    public void declareResourceRequirements(ResourceRequirements resourceRequirements) {
        synchronized (lock) {
            /** 检查 是否关闭*/
            checkNotClosed();
            /** 判断链接服务是否为 null 不为 null,则触发资源需求提交*/
            if (isConnected()) {
                /** 如果当前对象或服务已连接，则将传入的 resourceRequirements
                 * 赋值给 currentResourceRequirements 变量。 */
                currentResourceRequirements = resourceRequirements;
                /**
                 * 调用 triggerResourceRequirementsSubmission 方法来触发资源需求的提交。
                 */
                triggerResourceRequirementsSubmission(
                        Duration.ofMillis(1L),
                        Duration.ofMillis(10000L),
                        currentResourceRequirements);
            }
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 触发资源需求提交
    */
    @GuardedBy("lock")
    private void triggerResourceRequirementsSubmission(
            Duration sleepOnError,
            Duration maxSleepOnError,
            ResourceRequirements resourceRequirementsToSend) {
        /**
         * 使用 FutureUtils.retryWithDelay 方法来执行一个可能失败的操作，并在失败时按照指定的策略进行重试。
         */
        FutureUtils.retryWithDelay(
                /** 发送 资源需求发送到资源管理器或服务*/
                () -> sendResourceRequirements(resourceRequirementsToSend),
                new ExponentialBackoffRetryStrategy(
                        Integer.MAX_VALUE, sleepOnError, maxSleepOnError),
                throwable -> !(throwable instanceof CancellationException),
                scheduledExecutor);
    }

    private CompletableFuture<Acknowledge> sendResourceRequirements(
            ResourceRequirements resourceRequirementsToSend) {
        synchronized (lock) {
            /** 判断链接服务是否为 null*/
            if (isConnected()) {
                /**
                 * 检查要发送的资源需求是否与当前存储的 currentResourceRequirements 相同
                 */
                if (resourceRequirementsToSend == currentResourceRequirements) {
                    /**  异步地发送资源需求到ResourceManager */
                    return service.declareResourceRequirements(resourceRequirementsToSend);
                } else {
                    /** 打印日志，返回异步编程结束异常状态*/
                    LOG.debug("Newer resource requirements found. Stop sending old requirements.");
                    return FutureUtils.completedExceptionally(new CancellationException());
                }
            } else {
                /** 打印日志，返回异步编程结束异常状态*/
                LOG.debug(
                        "Stop sending resource requirements to ResourceManager because it is not connected.");
                return FutureUtils.completedExceptionally(new CancellationException());
            }
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建 DefaultDeclareResourceRequirementServiceConnectionManager
    */
    public static DeclareResourceRequirementServiceConnectionManager create(
            ScheduledExecutor scheduledExecutor) {
        /**
         * 使用传入的scheduledExecutor参数来创建一个新的DefaultDeclareResourceRequirementServiceConnectionManager对象，
         * 并返回这个新创建的对象的引用。
         */
        return new DefaultDeclareResourceRequirementServiceConnectionManager(scheduledExecutor);
    }
}
