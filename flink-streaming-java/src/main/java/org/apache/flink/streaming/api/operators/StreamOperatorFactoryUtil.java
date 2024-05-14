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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceAware;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.util.Optional;
import java.util.function.Supplier;

/** A utility to instantiate new operators with a given factory. */
public class StreamOperatorFactoryUtil {
    /**
     * Creates a new operator using a factory and makes sure that all special factory traits are
     * properly handled.
     *
     * @param operatorFactory the operator factory.
     * @param containingTask the containing task.
     * @param configuration the configuration of the operator.
     * @param output the output of the operator.
     * @param operatorEventDispatcher the operator event dispatcher for communication between
     *     operator and coordinators.
     * @return a newly created and configured operator, and the {@link ProcessingTimeService}
     *     instance it can access.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建一个新的操作符，使用工厂方法并确保所有特殊的工厂特性得到正确处理。
     *
     * @param operatorFactory 操作符工厂。
     * @param containingTask 包含操作符的任务。
     * @param configuration 操作符的配置。
     * @param output 操作符的输出。
     * @param operatorEventDispatcher 操作符事件调度器，用于操作符和协调器之间的通信。
     * @return <p>一个新创建并配置好的操作符，以及它可以访问的{@link ProcessingTimeService}实例。</p>
     *         <p>Tuple2的第一个元素是操作符OP，第二个元素是ProcessingTimeService的可选实例（如果存在的话）。</p>
     * @param <OUT> 操作符输出的数据类型。
     * @param <OP> 操作符的类型，必须是StreamOperator<OUT>的子类。
    */
    public static <OUT, OP extends StreamOperator<OUT>>
            Tuple2<OP, Optional<ProcessingTimeService>> createOperator(
                    StreamOperatorFactory<OUT> operatorFactory,
                    StreamTask<OUT, ?> containingTask,
                    StreamConfig configuration,
                    Output<StreamRecord<OUT>> output,
                    OperatorEventDispatcher operatorEventDispatcher) {
        // 获取邮箱执行器（MailboxExecutor），用于在任务之间发送和接收消息
        MailboxExecutor mailboxExecutor =
                containingTask
                        .getMailboxExecutorFactory()
                        .createExecutor(configuration.getChainIndex());
        // 如果操作符工厂是YieldingOperatorFactory的实例，则设置邮箱执行器
        if (operatorFactory instanceof YieldingOperatorFactory) {
            ((YieldingOperatorFactory<?>) operatorFactory).setMailboxExecutor(mailboxExecutor);
        }
        // 创建ProcessingTimeService，该服务用于提供处理时间服务
        final Supplier<ProcessingTimeService> processingTimeServiceFactory =
                () ->
                        containingTask
                                .getProcessingTimeServiceFactory()
                                .createProcessingTimeService(mailboxExecutor);
        // 初始化ProcessingTimeService实例
        final ProcessingTimeService processingTimeService;
        if (operatorFactory instanceof ProcessingTimeServiceAware) {
            // 如果操作符工厂是ProcessingTimeServiceAware的实例，则创建ProcessingTimeService并设置给工厂
            processingTimeService = processingTimeServiceFactory.get();
            ((ProcessingTimeServiceAware) operatorFactory)
                    .setProcessingTimeService(processingTimeService);
        } else {
            // 否则，不需要ProcessingTimeService，将其设为null
            processingTimeService = null;
        }

        // TODO: what to do with ProcessingTimeServiceAware?

        // 使用给定的参数创建操作符
        OP op =
                operatorFactory.createStreamOperator(
                        new StreamOperatorParameters<>(
                                containingTask,
                                configuration,
                                output,
                                processingTimeService != null
                                        ? () -> processingTimeService
                                        : processingTimeServiceFactory,
                                operatorEventDispatcher));
        //返回结果
        return new Tuple2<>(op, Optional.ofNullable(processingTimeService));
    }
}
