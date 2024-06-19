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

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.deployment.application.ApplicationClusterEntryPoint;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.DefaultPackagedProgramRetriever;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramRetriever;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.entrypoint.DynamicParametersConfigurationParserFactory;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** An {@link ApplicationClusterEntryPoint} for Yarn. */
@Internal
public final class YarnApplicationClusterEntryPoint extends ApplicationClusterEntryPoint {

    private YarnApplicationClusterEntryPoint(
            final Configuration configuration, final PackagedProgram program) {
        super(configuration, program, YarnResourceManagerFactory.getInstance());
    }

    @Override
    protected String getRPCPortRange(Configuration configuration) {
        return configuration.get(YarnConfigOptions.APPLICATION_MASTER_PORT);
    }

    public static void main(final String[] args) {
        // startup checks and logging
        // 记录环境信息
        EnvironmentInformation.logEnvironmentInfo(
                LOG, YarnApplicationClusterEntryPoint.class.getSimpleName(), args);
        // 注册信号处理器，以便在接收到如SIGTERM等信号时能够优雅地关闭应用
        SignalHandler.register(LOG);
        // 安装JVM关闭时的安全保障措施，确保在JVM关闭时执行一些清理或关闭资源的操作
        JvmShutdownSafeguard.installAsShutdownHook(LOG);
        // 获取系统环境变量
        Map<String, String> env = System.getenv();
        // 获取当前工作目录，这通常是在YARN容器中运行的应用的当前目录
        final String workingDirectory = env.get(ApplicationConstants.Environment.PWD.key());
        // 检查工作目录是否已设置，如果没有设置则抛出异常
        Preconditions.checkArgument(
                workingDirectory != null,
                "Working directory variable (%s) not set",
                ApplicationConstants.Environment.PWD.key());

        try {
            // 尝试记录YARN环境信息，这些信息对于调试和监控应用很有用
            YarnEntrypointUtils.logYarnEnvironmentInformation(env, LOG);
        } catch (IOException e) {
            // 如果在记录YARN环境信息时发生IO异常，则记录警告日志
            LOG.warn("Could not log YARN environment information.", e);
        }
        // 解析命令行参数，并生成动态参数配置对象
        // 如果参数解析失败，则通过调用System.exit退出应用
        final Configuration dynamicParameters =
                ClusterEntrypointUtils.parseParametersOrExit(
                        args,
                        new DynamicParametersConfigurationParserFactory(),
                        YarnApplicationClusterEntryPoint.class);
        // 加载配置，包括从工作目录、动态参数和环境变量中加载的配置
        final Configuration configuration =
                YarnEntrypointUtils.loadConfiguration(workingDirectory, dynamicParameters, env);
       // 定义一个PackagedProgram对象，用于存储打包的程序（可能是应用或作业）
        PackagedProgram program = null;
        try {
            // 尝试获取打包的程序，这通常涉及到从配置、文件或其他资源中加载程序
            program = getPackagedProgram(configuration);
        } catch (Exception e) {
            LOG.error("Could not create application program.", e);
            System.exit(1);
        }

        try {
            // 尝试配置执行环境，这通常包括设置程序参数、资源等
            configureExecution(configuration, program);
        } catch (Exception e) {
            // 如果在配置执行环境时发生异常，记录错误日志并退出程序
            LOG.error("Could not apply application configuration.", e);
            System.exit(1);
        }
       // 创建一个YarnApplicationClusterEntryPoint对象，该对象表示YARN应用程序集群的入口点
       // 它将使用之前获取的配置和打包的程序进行初始化
        YarnApplicationClusterEntryPoint yarnApplicationClusterEntrypoint =
                new YarnApplicationClusterEntryPoint(configuration, program);
        // 运行YARN应用程序集群的入口点，这通常意味着启动应用程序并在YARN集群上执行
        ClusterEntrypoint.runClusterEntrypoint(yarnApplicationClusterEntrypoint);
    }

    private static PackagedProgram getPackagedProgram(final Configuration configuration)
            throws FlinkException {

        final ApplicationConfiguration applicationConfiguration =
                ApplicationConfiguration.fromConfiguration(configuration);

        final PackagedProgramRetriever programRetriever =
                getPackagedProgramRetriever(
                        configuration,
                        applicationConfiguration.getProgramArguments(),
                        applicationConfiguration.getApplicationClassName());
        return programRetriever.getPackagedProgram();
    }

    private static PackagedProgramRetriever getPackagedProgramRetriever(
            final Configuration configuration,
            final String[] programArguments,
            @Nullable final String jobClassName)
            throws FlinkException {

        final File userLibDir = YarnEntrypointUtils.getUsrLibDir(configuration).orElse(null);

        // No need to do pipelineJars validation if it is a PyFlink job.
        if (!(PackagedProgramUtils.isPython(jobClassName)
                || PackagedProgramUtils.isPython(programArguments))) {
            final File userApplicationJar = getUserApplicationJar(userLibDir, configuration);
            return DefaultPackagedProgramRetriever.create(
                    userLibDir, userApplicationJar, jobClassName, programArguments, configuration);
        }

        return DefaultPackagedProgramRetriever.create(
                userLibDir, jobClassName, programArguments, configuration);
    }

    private static File getUserApplicationJar(
            final File userLibDir, final Configuration configuration) {
        final List<File> pipelineJars =
                configuration.get(PipelineOptions.JARS).stream()
                        .map(uri -> new File(userLibDir, new Path(uri).getName()))
                        .collect(Collectors.toList());

        Preconditions.checkArgument(pipelineJars.size() == 1, "Should only have one jar");
        return pipelineJars.get(0);
    }
}
