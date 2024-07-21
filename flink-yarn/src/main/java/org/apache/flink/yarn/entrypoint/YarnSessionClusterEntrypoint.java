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

package org.apache.flink.yarn.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.entrypoint.DynamicParametersConfigurationParserFactory;
import org.apache.flink.runtime.entrypoint.SessionClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.yarn.api.ApplicationConstants;

import java.io.IOException;
import java.util.Map;

/** Entry point for Yarn session clusters. */
public class YarnSessionClusterEntrypoint extends SessionClusterEntrypoint {

    public YarnSessionClusterEntrypoint(Configuration configuration) {
        super(configuration);
    }

    @Override
    protected String getRPCPortRange(Configuration configuration) {
        return configuration.get(YarnConfigOptions.APPLICATION_MASTER_PORT);
    }

    @Override
    protected DispatcherResourceManagerComponentFactory
            createDispatcherResourceManagerComponentFactory(Configuration configuration) {
        return DefaultDispatcherResourceManagerComponentFactory.createSessionComponentFactory(
                YarnResourceManagerFactory.getInstance());
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 主程序入口点。
     */
    public static void main(String[] args) {
        // startup checks and logging
        // 启动检查和日志记录
        // 记录环境信息和日志，帮助诊断问题
        EnvironmentInformation.logEnvironmentInfo(
                LOG, YarnSessionClusterEntrypoint.class.getSimpleName(), args);
        // 注册信号处理程序，以优雅地处理JVM终止信号
        SignalHandler.register(LOG);
        // 安装JVM关闭保护钩子，确保在JVM关闭时执行清理操作  JVM退出会推迟5秒
        JvmShutdownSafeguard.installAsShutdownHook(LOG);
        // 获取系统环境变量
        Map<String, String> env = System.getenv();
        // 获取工作目录，这是执行当前程序的目录
        final String workingDirectory = env.get(ApplicationConstants.Environment.PWD.key());
        // 检查工作目录是否已设置，未设置则抛出异常
        Preconditions.checkArgument(
                workingDirectory != null,
                "Working directory variable (%s) not set",
                ApplicationConstants.Environment.PWD.key());
        // 尝试记录YARN环境信息，以便调试和日志记录
        try {
            YarnEntrypointUtils.logYarnEnvironmentInformation(env, LOG);
        } catch (IOException e) {
            LOG.warn("Could not log YARN environment information.", e);
        }
        // 解析命令行参数为动态配置参数
        final Configuration dynamicParameters =
                ClusterEntrypointUtils.parseParametersOrExit(
                        args,
                        new DynamicParametersConfigurationParserFactory(),
                        YarnSessionClusterEntrypoint.class);
        // 加载配置，结合工作目录、动态参数和系统环境变量
        final Configuration configuration =
                YarnEntrypointUtils.loadConfiguration(workingDirectory, dynamicParameters, env);
        // 创建Yarn会话集群入口点实例，并传入配置
        YarnSessionClusterEntrypoint yarnSessionClusterEntrypoint =
                new YarnSessionClusterEntrypoint(configuration);
        // 运行集群入口点，启动集群
        ClusterEntrypoint.runClusterEntrypoint(yarnSessionClusterEntrypoint);
    }
}
