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
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.plugin.PluginConfig;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.jobmanager.JobManagerProcessSpec;
import org.apache.flink.runtime.jobmanager.JobManagerProcessUtils;
import org.apache.flink.runtime.security.token.DefaultDelegationTokenManager;
import org.apache.flink.runtime.security.token.DelegationTokenContainer;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.runtime.security.token.hadoop.HadoopDelegationTokenConverter;
import org.apache.flink.runtime.security.token.hadoop.KerberosLoginProvider;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.function.FunctionUtils;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.flink.yarn.configuration.YarnLogConfigUtil;
import org.apache.flink.yarn.entrypoint.YarnApplicationClusterEntryPoint;
import org.apache.flink.yarn.entrypoint.YarnJobClusterEntrypoint;
import org.apache.flink.yarn.entrypoint.YarnSessionClusterEntrypoint;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.client.deployment.application.ApplicationConfiguration.APPLICATION_MAIN_CLASS;
import static org.apache.flink.configuration.ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR;
import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_LIB_DIR;
import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_OPT_DIR;
import static org.apache.flink.configuration.ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX;
import static org.apache.flink.configuration.ResourceManagerOptions.CONTAINERIZED_TASK_MANAGER_ENV_PREFIX;
import static org.apache.flink.runtime.entrypoint.component.FileJobGraphRetriever.JOB_GRAPH_FILE_PATH;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.yarn.Utils.getPathFromLocalFile;
import static org.apache.flink.yarn.Utils.getPathFromLocalFilePathStr;
import static org.apache.flink.yarn.Utils.getStartCommand;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_FLINK_CLASSPATH;
import static org.apache.flink.yarn.YarnConfigKeys.LOCAL_RESOURCE_DESCRIPTOR_SEPARATOR;
import static org.apache.flink.yarn.configuration.YarnConfigOptions.YARN_CONTAINER_START_COMMAND_TEMPLATE;

/** The descriptor with deployment information for deploying a Flink cluster on Yarn. */
public class YarnClusterDescriptor implements ClusterDescriptor<ApplicationId> {
    private static final Logger LOG = LoggerFactory.getLogger(YarnClusterDescriptor.class);

    @VisibleForTesting
    static final String IGNORE_UNRECOGNIZED_VM_OPTIONS = "-XX:+IgnoreUnrecognizedVMOptions";

    private final YarnConfiguration yarnConfiguration;

    private final YarnClient yarnClient;

    private final YarnClusterInformationRetriever yarnClusterInformationRetriever;

    /** True if the descriptor must not shut down the YarnClient. */
    private final boolean sharedYarnClient;

    /**
     * Lazily initialized list of files to ship. The path string for the files which is configured
     * by {@link YarnConfigOptions#SHIP_FILES} will be converted to {@link Path} with schema and
     * absolute path.
     */
    private final List<Path> shipFiles = new LinkedList<>();

    /**
     * Lazily initialized list of archives to ship. The path string for the archives which is
     * configured by {@link YarnConfigOptions#SHIP_ARCHIVES} will be converted to {@link Path} with
     * schema and absolute path.
     */
    private final List<Path> shipArchives = new LinkedList<>();

    private final String yarnQueue;

    private Path flinkJarPath;

    private final Configuration flinkConfiguration;

    private final String customName;

    private final String nodeLabel;

    private final String applicationType;

    private YarnConfigOptions.UserJarInclusion userJarInclusion;

    public YarnClusterDescriptor(
            Configuration flinkConfiguration,
            YarnConfiguration yarnConfiguration,
            YarnClient yarnClient,
            YarnClusterInformationRetriever yarnClusterInformationRetriever,
            boolean sharedYarnClient) {

        this.yarnConfiguration = Preconditions.checkNotNull(yarnConfiguration);
        this.yarnClient = Preconditions.checkNotNull(yarnClient);
        this.yarnClusterInformationRetriever =
                Preconditions.checkNotNull(yarnClusterInformationRetriever);
        this.sharedYarnClient = sharedYarnClient;

        this.flinkConfiguration = Preconditions.checkNotNull(flinkConfiguration);
        this.userJarInclusion = getUserJarInclusionMode(flinkConfiguration);

        adaptEnvSetting(flinkConfiguration, CoreOptions.FLINK_LOG_LEVEL, "ROOT_LOG_LEVEL");
        adaptEnvSetting(flinkConfiguration, CoreOptions.FLINK_LOG_MAX, "MAX_LOG_FILE_NUMBER");

        getLocalFlinkDistPath(flinkConfiguration).ifPresent(this::setLocalJarPath);
        decodeFilesToShipToCluster(flinkConfiguration, YarnConfigOptions.SHIP_FILES)
                .ifPresent(this::addShipFiles);
        decodeFilesToShipToCluster(flinkConfiguration, YarnConfigOptions.SHIP_ARCHIVES)
                .ifPresent(this::addShipArchives);

        this.yarnQueue = flinkConfiguration.get(YarnConfigOptions.APPLICATION_QUEUE);
        this.customName = flinkConfiguration.get(YarnConfigOptions.APPLICATION_NAME);
        this.applicationType = flinkConfiguration.get(YarnConfigOptions.APPLICATION_TYPE);
        this.nodeLabel = flinkConfiguration.get(YarnConfigOptions.NODE_LABEL);
    }

    /** Adapt flink env setting. */
    private static <T> void adaptEnvSetting(
            Configuration config, ConfigOption<T> configOption, String envKey) {
        config.getOptional(configOption)
                .ifPresent(
                        value -> {
                            config.setString(
                                    CONTAINERIZED_MASTER_ENV_PREFIX + envKey,
                                    String.valueOf(value));
                            config.setString(
                                    CONTAINERIZED_TASK_MANAGER_ENV_PREFIX + envKey,
                                    String.valueOf(value));
                        });
    }

    private Optional<List<Path>> decodeFilesToShipToCluster(
            final Configuration configuration, final ConfigOption<List<String>> configOption) {
        checkNotNull(configuration);
        checkNotNull(configOption);

        List<Path> files =
                ConfigUtils.decodeListFromConfig(
                        configuration, configOption, this::createPathWithSchema);
        return files.isEmpty() ? Optional.empty() : Optional.of(files);
    }

    private Path createPathWithSchema(String path) {
        return isWithoutSchema(new Path(path)) ? getPathFromLocalFilePathStr(path) : new Path(path);
    }

    private boolean isWithoutSchema(Path path) {
        return StringUtils.isNullOrWhitespaceOnly(path.toUri().getScheme());
    }

    private Optional<Path> getLocalFlinkDistPath(final Configuration configuration) {
        final String localJarPath = configuration.get(YarnConfigOptions.FLINK_DIST_JAR);
        if (localJarPath != null) {
            return Optional.of(new Path(localJarPath));
        }

        LOG.info(
                "No path for the flink jar passed. Using the location of "
                        + getClass()
                        + " to locate the jar");

        // check whether it's actually a jar file --> when testing we execute this class without a
        // flink-dist jar
        final String decodedPath = getDecodedJarPath();
        return decodedPath.endsWith(".jar")
                ? Optional.of(getPathFromLocalFilePathStr(decodedPath))
                : Optional.empty();
    }

    private String getDecodedJarPath() {
        final String encodedJarPath =
                getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        try {
            return URLDecoder.decode(encodedJarPath, Charset.defaultCharset().name());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(
                    "Couldn't decode the encoded Flink dist jar path: "
                            + encodedJarPath
                            + " You can supply a path manually via the command line.");
        }
    }

    @VisibleForTesting
    List<Path> getShipFiles() {
        return shipFiles;
    }

    @VisibleForTesting
    List<Path> getShipArchives() {
        return shipArchives;
    }

    public YarnClient getYarnClient() {
        return yarnClient;
    }

    /**
     * The class to start the application master with. This class runs the main method in case of
     * session cluster.
     */
    protected String getYarnSessionClusterEntrypoint() {
        return YarnSessionClusterEntrypoint.class.getName();
    }

    /**
     * The class to start the application master with. This class runs the main method in case of
     * the job cluster.
     */
    protected String getYarnJobClusterEntrypoint() {
        return YarnJobClusterEntrypoint.class.getName();
    }

    public Configuration getFlinkConfiguration() {
        return flinkConfiguration;
    }

    public void setLocalJarPath(Path localJarPath) {
        if (!localJarPath.toString().endsWith("jar")) {
            throw new IllegalArgumentException(
                    "The passed jar path ('"
                            + localJarPath
                            + "') does not end with the 'jar' extension");
        }
        this.flinkJarPath = localJarPath;
    }

    /**
     * Adds the given files to the list of files to ship.
     *
     * <p>Note that any file matching "<tt>flink-dist*.jar</tt>" will be excluded from the upload by
     * {@link YarnApplicationFileUploader#registerMultipleLocalResources(Collection, String,
     * LocalResourceType)} since we upload the Flink uber jar ourselves and do not need to deploy it
     * multiple times.
     *
     * @param shipFiles files to ship
     */
    public void addShipFiles(List<Path> shipFiles) {
        checkArgument(
                !isUsrLibDirIncludedInShipFiles(shipFiles, yarnConfiguration),
                "User-shipped directories configured via : %s should not include %s.",
                YarnConfigOptions.SHIP_FILES.key(),
                ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR);
        this.shipFiles.addAll(shipFiles);
    }

    private void addShipArchives(List<Path> shipArchives) {
        checkArgument(
                isArchiveOnlyIncludedInShipArchiveFiles(shipArchives, yarnConfiguration),
                "Directories or non-archive files are included.");
        this.shipArchives.addAll(shipArchives);
    }

    private static boolean isArchiveOnlyIncludedInShipArchiveFiles(
            List<Path> shipFiles, YarnConfiguration yarnConfiguration) {
        long archivedFileCount =
                shipFiles.stream()
                        .map(
                                FunctionUtils.uncheckedFunction(
                                        path -> getFileStatus(path, yarnConfiguration)))
                        .filter(FileStatus::isFile)
                        .map(status -> status.getPath().getName().toLowerCase())
                        .filter(
                                name ->
                                        name.endsWith(".tar.gz")
                                                || name.endsWith(".tar")
                                                || name.endsWith(".tgz")
                                                || name.endsWith(".dst")
                                                || name.endsWith(".jar")
                                                || name.endsWith(".zip"))
                        .count();
        return archivedFileCount == shipFiles.size();
    }

    private static FileStatus getFileStatus(Path path, YarnConfiguration yarnConfiguration)
            throws IOException {
        return path.getFileSystem(yarnConfiguration).getFileStatus(path);
    }

    private void isReadyForDeployment(ClusterSpecification clusterSpecification) throws Exception {

        if (this.flinkJarPath == null) {
            throw new YarnDeploymentException("The Flink jar path is null");
        }
        if (this.flinkConfiguration == null) {
            throw new YarnDeploymentException("Flink configuration object has not been set");
        }

        // Check if we don't exceed YARN's maximum virtual cores.
        final int numYarnMaxVcores = yarnClusterInformationRetriever.getMaxVcores();

        int configuredAmVcores = flinkConfiguration.get(YarnConfigOptions.APP_MASTER_VCORES);
        if (configuredAmVcores > numYarnMaxVcores) {
            throw new IllegalConfigurationException(
                    String.format(
                            "The number of requested virtual cores for application master %d"
                                    + " exceeds the maximum number of virtual cores %d available in the Yarn Cluster.",
                            configuredAmVcores, numYarnMaxVcores));
        }

        int configuredVcores =
                flinkConfiguration.get(
                        YarnConfigOptions.VCORES, clusterSpecification.getSlotsPerTaskManager());
        // don't configure more than the maximum configured number of vcores
        if (configuredVcores > numYarnMaxVcores) {
            throw new IllegalConfigurationException(
                    String.format(
                            "The number of requested virtual cores per node %d"
                                    + " exceeds the maximum number of virtual cores %d available in the Yarn Cluster."
                                    + " Please note that the number of virtual cores is set to the number of task slots by default"
                                    + " unless configured in the Flink config with '%s.'",
                            configuredVcores, numYarnMaxVcores, YarnConfigOptions.VCORES.key()));
        }

        // check if required Hadoop environment variables are set. If not, warn user
        if (System.getenv("HADOOP_CONF_DIR") == null && System.getenv("YARN_CONF_DIR") == null) {
            LOG.warn(
                    "Neither the HADOOP_CONF_DIR nor the YARN_CONF_DIR environment variable is set. "
                            + "The Flink YARN Client needs one of these to be set to properly load the Hadoop "
                            + "configuration for accessing YARN.");
        }
    }

    public String getNodeLabel() {
        return nodeLabel;
    }

    // -------------------------------------------------------------
    // Lifecycle management
    // -------------------------------------------------------------

    @Override
    public void close() {
        if (!sharedYarnClient) {
            yarnClient.stop();
        }
    }

    // -------------------------------------------------------------
    // ClusterClient overrides
    // -------------------------------------------------------------

    @Override
    public ClusterClientProvider<ApplicationId> retrieve(ApplicationId applicationId)
            throws ClusterRetrieveException {

        try {
            // check if required Hadoop environment variables are set. If not, warn user
            if (System.getenv("HADOOP_CONF_DIR") == null
                    && System.getenv("YARN_CONF_DIR") == null) {
                LOG.warn(
                        "Neither the HADOOP_CONF_DIR nor the YARN_CONF_DIR environment variable is set."
                                + "The Flink YARN Client needs one of these to be set to properly load the Hadoop "
                                + "configuration for accessing YARN.");
            }

            final ApplicationReport report = yarnClient.getApplicationReport(applicationId);

            if (report.getFinalApplicationStatus() != FinalApplicationStatus.UNDEFINED) {
                // Flink cluster is not running anymore
                LOG.error(
                        "The application {} doesn't run anymore. It has previously completed with final status: {}",
                        applicationId,
                        report.getFinalApplicationStatus());
                throw new RuntimeException(
                        "The Yarn application " + applicationId + " doesn't run anymore.");
            }

            setClusterEntrypointInfoToConfig(report);

            return () -> {
                try {
                    return new RestClusterClient<>(flinkConfiguration, report.getApplicationId());
                } catch (Exception e) {
                    throw new RuntimeException("Couldn't retrieve Yarn cluster", e);
                }
            };
        } catch (Exception e) {
            throw new ClusterRetrieveException("Couldn't retrieve Yarn cluster", e);
        }
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 部署一个Flink会话集群。
     * 核心点 getYarnSessionClusterEntrypoint YarnSessionClusterEntrypoint.class.getName();
     */
    @Override
    public ClusterClientProvider<ApplicationId> deploySessionCluster(
            ClusterSpecification clusterSpecification) throws ClusterDeploymentException {
        try {
            // 调用内部部署方法，该方法负责实际的集群部署逻辑
            // todo getYarnSessionClusterEntrypoint YarnSessionClusterEntrypoint.class.getName();
            return deployInternal(
                    clusterSpecification,// 集群配置
                    "Flink session cluster",// 集群描述，这里指定为Flink会话集群
                    getYarnSessionClusterEntrypoint(),// 获取Yarn会话集群的入口点重点
                    null,
                    false);
        } catch (Exception e) {
            throw new ClusterDeploymentException("Couldn't deploy Yarn session cluster", e);
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 部署Yarn对应的Flink Application
    */
    @Override
    public ClusterClientProvider<ApplicationId> deployApplicationCluster(
            final ClusterSpecification clusterSpecification,
            final ApplicationConfiguration applicationConfiguration)
            throws ClusterDeploymentException {
        // 检查传入的集群规格和应用配置是否为空
        checkNotNull(clusterSpecification);
        checkNotNull(applicationConfiguration);
        // 从 Flink 配置中创建 YARN 部署目标
        final YarnDeploymentTarget deploymentTarget =
                YarnDeploymentTarget.fromConfig(flinkConfiguration);
        // 检查部署目标是否为 YARN 应用集群
        // 如果不是，则抛出异常
        if (YarnDeploymentTarget.APPLICATION != deploymentTarget) {
            throw new ClusterDeploymentException(
                    "Couldn't deploy Yarn Application Cluster."
                            + " Expected deployment.target="
                            + YarnDeploymentTarget.APPLICATION.getName()
                            + " but actual one was \""
                            + deploymentTarget.getName()
                            + "\"");
        }

        // 将应用配置应用到 Flink 配置中
        applicationConfiguration.applyToConfiguration(flinkConfiguration);

        // No need to do pipelineJars validation if it is a PyFlink job.
        // 如果不是 PyFlink 作业，则不需要进行 pipelineJars 验证
        // PyFlink 作业使用 Python 编写，不需要 JAR 文件
        if (!(PackagedProgramUtils.isPython(applicationConfiguration.getApplicationClassName())
                || PackagedProgramUtils.isPython(applicationConfiguration.getProgramArguments()))) {
            final List<String> pipelineJars =
                    flinkConfiguration
                            .getOptional(PipelineOptions.JARS)
                            .orElse(Collections.emptyList());
            // 验证 pipelineJars 中只能有一个 JAR 文件
            Preconditions.checkArgument(pipelineJars.size() == 1, "Should only have one jar");
        }

        try {
            // 尝试内部部署方法，创建并部署 YARN 应用集群
            // 传入集群规格、集群名称、入口类名、附加参数和是否等待集群就绪
            return deployInternal(
                    clusterSpecification,
                    "Flink Application Cluster",
                    YarnApplicationClusterEntryPoint.class.getName(),//调用的类和心中的核心
                    null,
                    false);
        } catch (Exception e) {
            // 如果部署过程中出现异常，则抛出 ClusterDeploymentException 异常
            throw new ClusterDeploymentException("Couldn't deploy Yarn Application Cluster", e);
        }
    }

    @Override
    public ClusterClientProvider<ApplicationId> deployJobCluster(
            ClusterSpecification clusterSpecification, JobGraph jobGraph, boolean detached)
            throws ClusterDeploymentException {

        LOG.warn(
                "Job Clusters are deprecated since Flink 1.15. Please use an Application Cluster/Application Mode instead.");
        try {
            return deployInternal(
                    clusterSpecification,
                    "Flink per-job cluster",
                    getYarnJobClusterEntrypoint(),
                    jobGraph,
                    detached);
        } catch (Exception e) {
            throw new ClusterDeploymentException("Could not deploy Yarn job cluster.", e);
        }
    }

    @Override
    public void killCluster(ApplicationId applicationId) throws FlinkException {
        try {
            yarnClient.killApplication(applicationId);

            try (final FileSystem fs = FileSystem.get(yarnConfiguration)) {
                final Path applicationDir =
                        YarnApplicationFileUploader.getApplicationDirPath(
                                getStagingDir(fs), applicationId);

                Utils.deleteApplicationFiles(applicationDir.toUri().toString());
            }

        } catch (YarnException | IOException e) {
            throw new FlinkException(
                    "Could not kill the Yarn Flink cluster with id " + applicationId + '.', e);
        }
    }

    /**
     * This method will block until the ApplicationMaster/JobManager have been deployed on YARN.
     *
     * @param clusterSpecification Initial cluster specification for the Flink cluster to be
     *     deployed
     * @param applicationName name of the Yarn application to start
     * @param yarnClusterEntrypoint Class name of the Yarn cluster entry point.
     * @param jobGraph A job graph which is deployed with the Flink cluster, {@code null} if none
     * @param detached True if the cluster should be started in detached mode
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 基于Yarn部署Flink集群
     */
    private ClusterClientProvider<ApplicationId> deployInternal(
            ClusterSpecification clusterSpecification,
            String applicationName,
            String yarnClusterEntrypoint,
            @Nullable JobGraph jobGraph,
            boolean detached)
            throws Exception {
        // 获取当前用户的用户组信息
        final UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        // 检查是否启用了基于 Kerberos 的 Hadoop 安全
        if (HadoopUtils.isKerberosSecurityEnabled(currentUser)) {
            // 是否使用票据缓存
            boolean useTicketCache =
                    flinkConfiguration.get(SecurityOptions.KERBEROS_LOGIN_USETICKETCACHE);
            // 检查当前用户是否具有有效的 Kerberos 凭据或委托令牌
            if (!HadoopUtils.areKerberosCredentialsValid(currentUser, useTicketCache)) {
                throw new RuntimeException(
                        "Hadoop security with Kerberos is enabled but the login user "
                                + "does not have Kerberos credentials or delegation tokens!");
            }
            // 是否从 YARN 获取委托令牌
            final boolean fetchToken =
                    flinkConfiguration.get(SecurityOptions.KERBEROS_FETCH_DELEGATION_TOKEN);
            // 是否启用了对 Hadoop 文件系统的访问
            final boolean yarnAccessFSEnabled =
                    !CollectionUtil.isNullOrEmpty(
                            flinkConfiguration.get(
                                    SecurityOptions.KERBEROS_HADOOP_FILESYSTEMS_TO_ACCESS));
            // 如果不获取委托令牌但启用了对 Hadoop 文件系统的访问，则抛出异常
            if (!fetchToken && yarnAccessFSEnabled) {
                throw new IllegalConfigurationException(
                        String.format(
                                "When %s is disabled, %s must be disabled as well.",
                                SecurityOptions.KERBEROS_FETCH_DELEGATION_TOKEN.key(),
                                SecurityOptions.KERBEROS_HADOOP_FILESYSTEMS_TO_ACCESS.key()));
            }
        }
        /**
         * 核心点1.检查基础配置
         */
        // 调用 isReadyForDeployment 方法确保集群规格已经准备好，可以进行部署
        isReadyForDeployment(clusterSpecification);

        // ------------------ Check if the specified queue exists --------------------

        // 调用 checkYarnQueues 方法来验证 YARN 集群中是否存在指定的队列。
       // 如果队列不存在，可能会抛出异常或返回错误，取决于方法的实现。
       // 这个步骤在提交作业到 YARN 之前进行，以确保作业可以被调度到正确的队列中。
        checkYarnQueues(yarnClient);

        // ------------------ Check if the YARN ClusterClient has the requested resources
        // --------------
        /**
         * 核心点2.创建Yarn客户端应用程序或者集群资源相关信息
         */
        // Create application via yarnClient
        // 创建 YARN 客户端应用程序
        final YarnClientApplication yarnApplication = yarnClient.createApplication();
        final GetNewApplicationResponse appResponse = yarnApplication.getNewApplicationResponse();

        // 获取 YARN 支持的最大资源能力
        Resource maxRes = appResponse.getMaximumResourceCapability();

        // 获取当前 YARN 集群的空闲资源
        final ClusterResourceDescription freeClusterMem;
        try {
            freeClusterMem = getCurrentFreeClusterResources(yarnClient);
        } catch (YarnException | IOException e) {
            // 如果无法获取集群空闲资源，则停止会话并抛出异常
            failSessionDuringDeployment(yarnClient, yarnApplication);
            throw new YarnDeploymentException(
                    "Could not retrieve information about free cluster resources.", e);
        }

        // 获取 YARN 集群中任务调度器的最小内存分配值
        final int yarnMinAllocationMB =
                yarnConfiguration.getInt(
                        YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
                        YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
        if (yarnMinAllocationMB <= 0) {
            // 如果最小分配内存配置不正确（小于或等于0），则抛出异常
            throw new YarnDeploymentException(
                    "The minimum allocation memory "
                            + "("
                            + yarnMinAllocationMB
                            + " MB) configured via '"
                            + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB
                            + "' should be greater than 0.");
        }

        // 1.验证集群资源是否满足部署要求
        final ClusterSpecification validClusterSpecification;
        try {
            validClusterSpecification =
                    validateClusterResources(
                            clusterSpecification, yarnMinAllocationMB, maxRes, freeClusterMem);
        } catch (YarnDeploymentException yde) {
            // 如果集群资源不满足要求，则停止会话并抛出异常
            failSessionDuringDeployment(yarnClient, yarnApplication);
            throw yde;
        }
         // 打印验证后的集群规格信息
        LOG.info("Cluster specification: {}", validClusterSpecification);

        // 设置 Flink 集群的执行模式，根据 detached 变量的值来决定是分离模式还是正常模式
        final ClusterEntrypoint.ExecutionMode executionMode =
                detached
                        ? ClusterEntrypoint.ExecutionMode.DETACHED//分离模式
                        : ClusterEntrypoint.ExecutionMode.NORMAL;//正常模式

        // 将执行模式设置到 Flink 配置中，以便 Flink 集群知道应该以何种模式运行
        flinkConfiguration.set(
                ClusterEntrypoint.INTERNAL_CLUSTER_EXECUTION_MODE, executionMode.toString());
        /**
         * 核心点3.启动ApplicationMaster
         */
        // 启动 Application Master（AM），这是 YARN 集群中负责管理 Flink 作业的组件
        ApplicationReport report =
                startAppMaster(
                        flinkConfiguration,// Flink 配置文件
                        applicationName, // 应用程序名称
                        yarnClusterEntrypoint, // YARN 集群的入口点（例如 Flink YARN Session 的主类）
                        jobGraph,// Flink 作业的图结构
                        yarnClient,// YARN 客户端
                        yarnApplication,// YARN 客户端应用程序
                        validClusterSpecification);// 验证后的集群规格


        // print the application id for user to cancel themselves.
        // 如果是在分离模式下运行，则打印应用程序 ID，以便用户后续可以手动取消作业
        if (detached) {
            final ApplicationId yarnApplicationId = report.getApplicationId();
            logDetachedClusterInformation(yarnApplicationId, LOG);
        }
         // 将 Application Master 的相关信息设置到 Flink 配置中，方便后续使用
        setClusterEntrypointInfoToConfig(report);

        return () -> {
            try {
                // 使用 Flink 配置和应用程序 ID 创建一个 RestClusterClient 对象
                return new RestClusterClient<>(flinkConfiguration, report.getApplicationId());
            } catch (Exception e) {
                throw new RuntimeException("Error while creating RestClusterClient.", e);
            }
        };
    }

    private ClusterSpecification validateClusterResources(
            ClusterSpecification clusterSpecification,
            int yarnMinAllocationMB,
            Resource maximumResourceCapability,
            ClusterResourceDescription freeClusterResources)
            throws YarnDeploymentException {

        int jobManagerMemoryMb = clusterSpecification.getMasterMemoryMB();
        final int taskManagerMemoryMb = clusterSpecification.getTaskManagerMemoryMB();

        logIfComponentMemNotIntegerMultipleOfYarnMinAllocation(
                "JobManager", jobManagerMemoryMb, yarnMinAllocationMB);
        logIfComponentMemNotIntegerMultipleOfYarnMinAllocation(
                "TaskManager", taskManagerMemoryMb, yarnMinAllocationMB);

        // set the memory to minAllocationMB to do the next checks correctly
        if (jobManagerMemoryMb < yarnMinAllocationMB) {
            jobManagerMemoryMb = yarnMinAllocationMB;
        }

        final String note =
                "Please check the 'yarn.scheduler.maximum-allocation-mb' and the 'yarn.nodemanager.resource.memory-mb' configuration values\n";
        if (jobManagerMemoryMb > maximumResourceCapability.getMemorySize()) {
            throw new YarnDeploymentException(
                    "The cluster does not have the requested resources for the JobManager available!\n"
                            + "Maximum Memory: "
                            + maximumResourceCapability.getMemorySize()
                            + "MB Requested: "
                            + jobManagerMemoryMb
                            + "MB. "
                            + note);
        }

        if (taskManagerMemoryMb > maximumResourceCapability.getMemorySize()) {
            throw new YarnDeploymentException(
                    "The cluster does not have the requested resources for the TaskManagers available!\n"
                            + "Maximum Memory: "
                            + maximumResourceCapability.getMemorySize()
                            + " Requested: "
                            + taskManagerMemoryMb
                            + "MB. "
                            + note);
        }

        final String noteRsc =
                "\nThe Flink YARN client will try to allocate the YARN session, but maybe not all TaskManagers are "
                        + "connecting from the beginning because the resources are currently not available in the cluster. "
                        + "The allocation might take more time than usual because the Flink YARN client needs to wait until "
                        + "the resources become available.";

        if (taskManagerMemoryMb > freeClusterResources.containerLimit) {
            LOG.warn(
                    "The requested amount of memory for the TaskManagers ("
                            + taskManagerMemoryMb
                            + "MB) is more than "
                            + "the largest possible YARN container: "
                            + freeClusterResources.containerLimit
                            + noteRsc);
        }
        if (jobManagerMemoryMb > freeClusterResources.containerLimit) {
            LOG.warn(
                    "The requested amount of memory for the JobManager ("
                            + jobManagerMemoryMb
                            + "MB) is more than "
                            + "the largest possible YARN container: "
                            + freeClusterResources.containerLimit
                            + noteRsc);
        }

        return new ClusterSpecification.ClusterSpecificationBuilder()
                .setMasterMemoryMB(jobManagerMemoryMb)
                .setTaskManagerMemoryMB(taskManagerMemoryMb)
                .setSlotsPerTaskManager(clusterSpecification.getSlotsPerTaskManager())
                .createClusterSpecification();
    }

    private void logIfComponentMemNotIntegerMultipleOfYarnMinAllocation(
            String componentName, int componentMemoryMB, int yarnMinAllocationMB) {
        int normalizedMemMB =
                (componentMemoryMB + (yarnMinAllocationMB - 1))
                        / yarnMinAllocationMB
                        * yarnMinAllocationMB;
        if (normalizedMemMB <= 0) {
            normalizedMemMB = yarnMinAllocationMB;
        }
        if (componentMemoryMB != normalizedMemMB) {
            LOG.info(
                    "The configured {} memory is {} MB. YARN will allocate {} MB to make up an integer multiple of its "
                            + "minimum allocation memory ({} MB, configured via 'yarn.scheduler.minimum-allocation-mb'). The extra {} MB "
                            + "may not be used by Flink.",
                    componentName,
                    componentMemoryMB,
                    normalizedMemMB,
                    yarnMinAllocationMB,
                    normalizedMemMB - componentMemoryMB);
        }
    }

    private void checkYarnQueues(YarnClient yarnClient) {
        try {
            List<QueueInfo> queues = yarnClient.getAllQueues();
            if (queues.size() > 0
                    && this.yarnQueue
                            != null) { // check only if there are queues configured in yarn and for
                // this session.
                boolean queueFound = false;
                for (QueueInfo queue : queues) {
                    if (queue.getQueueName().equals(this.yarnQueue)
                            || queue.getQueueName().equals("root." + this.yarnQueue)) {
                        queueFound = true;
                        break;
                    }
                }
                if (!queueFound) {
                    String queueNames = StringUtils.toQuotedListString(queues.toArray());
                    LOG.warn(
                            "The specified queue '"
                                    + this.yarnQueue
                                    + "' does not exist. "
                                    + "Available queues: "
                                    + queueNames);
                }
            } else {
                LOG.debug("The YARN cluster does not have any queues configured");
            }
        } catch (Throwable e) {
            LOG.warn("Error while getting queue information from YARN: " + e.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error details", e);
            }
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * @param configuration Flink 的配置对象
     * @param applicationName 应用程序的名称
     * @param yarnClusterEntrypoint YARN 集群的入口点（例如 Flink YARN Session 的主类）
     * @param jobGraph  Flink 作业的图结构
     * @param yarnClient  YARN 客户端
     * @param yarnApplication  YARN 客户端应用程序
     * @param clusterSpecification  集群规格配置
    */
    private ApplicationReport startAppMaster(
            Configuration configuration,
            String applicationName,
            String yarnClusterEntrypoint,
            JobGraph jobGraph,
            YarnClient yarnClient,
            YarnClientApplication yarnApplication,
            ClusterSpecification clusterSpecification)
            throws Exception {

        // ------------------ Initialize the file systems -------------------------
        // ------------------ 初始化文件系统 -------------------------
        /**
         * 核心点1.初始化文件系统
         */
        // 使用 Flink 配置和插件管理器来初始化文件系统
        // 这将确保文件系统相关的配置和插件被正确加载
        org.apache.flink.core.fs.FileSystem.initialize(
                configuration, PluginUtils.createPluginManagerFromRootFolder(configuration));

        // 获取与 YARN 配置关联的文件系统实例
        final FileSystem fs = FileSystem.get(yarnConfiguration);

        // hard coded check for the GoogleHDFS client because its not overriding the getScheme()
        // method.
        // 对于 GoogleHadoopFileSystem 硬编码检查，因为它没有重写 getScheme() 方法
        // 如果文件系统不是 GoogleHadoopFileSystem 并且其 scheme 以 "file" 开头，则发出警告
        // 这通常意味着指定的 Hadoop 配置路径是错误的，系统正在使用默认的 Hadoop 配置值
        // Flink YARN 客户端需要将文件存储在分布式文件系统中
        if (!fs.getClass().getSimpleName().equals("GoogleHadoopFileSystem")
                && fs.getScheme().startsWith("file")) {
            LOG.warn(
                    "The file system scheme is '"
                            + fs.getScheme()
                            + "'. This indicates that the "
                            + "specified Hadoop configuration path is wrong and the system is using the default Hadoop configuration values."
                            + "The Flink YARN client needs to store its files in a distributed file system");
        }
        // 获取 YARN 应用程序的提交上下文
        ApplicationSubmissionContext appContext = yarnApplication.getApplicationSubmissionContext();
        // 获取配置中指定的远程库目录列表比如读取jar
        final List<Path> providedLibDirs =
                Utils.getQualifiedRemoteProvidedLibDirs(configuration, yarnConfiguration);
        // 获取配置中指定的远程用户库目录（如果指定了的话）
        final Optional<Path> providedUsrLibDir =
                Utils.getQualifiedRemoteProvidedUsrLib(configuration, yarnConfiguration);

        /**
         * 核心点2.创建临时目录，创建FileUploader文件上传实例
         */
        // 获取 Flink 作业的临时目录路径，通常用于存储作业提交时需要上传到 YARN 的文件
        Path stagingDirPath = getStagingDir(fs);
        // 获取与 stagingDir 路径相关联的文件系统实例
        FileSystem stagingDirFs = stagingDirPath.getFileSystem(yarnConfiguration);
        // 创建一个 YarnApplicationFileUploader 实例，用于将必要的文件上传到 YARN 的 staging 目录
        // 该实例使用 staging 目录的文件系统、路径、提供的库目录列表、应用程序 ID
        final YarnApplicationFileUploader fileUploader =
                YarnApplicationFileUploader.from(
                        stagingDirFs,
                        stagingDirPath,
                        providedLibDirs,
                        appContext.getApplicationId(),
                        getFileReplication());

        // The files need to be shipped and added to classpath.
        // 需要传输到 YARN 集群并添加到类路径的文件集合
        // 初始化为 shipFiles（可能是一些 Flink 所需的库或配置文件）
        Set<Path> systemShipFiles = new HashSet<>(shipFiles);

        // 如果配置了日志配置文件路径，则将其添加到需要传输的文件集合中
        final String logConfigFilePath =
                configuration.get(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE);
        if (logConfigFilePath != null) {
            // 将日志配置文件路径转换为 Path 对象并添加到需要传输的文件集合中
            systemShipFiles.add(getPathFromLocalFilePathStr(logConfigFilePath));
        }

        /**
         * 核心点3.为应用程序设置提交的环境
         */
        // Set-up ApplicationSubmissionContext for the application
        // 为应用程序设置 ApplicationSubmissionContext

        // 获取应用程序 ID（在之前可能已经被 YARN 分配）
        final ApplicationId appId = appContext.getApplicationId();

        // ------------------ Add Zookeeper namespace to local flinkConfiguration ------
        setHAClusterIdIfNotSet(configuration, appId);

        // 检查是否启用了高可用性模式
        if (HighAvailabilityMode.isHighAvailabilityModeActivated(configuration)) {
            // 如果启用了高可用性模式，则激活失败应用程序的重新执行
            // 设置最大应用程序尝试次数，可以从 Flink 配置或 YARN 默认配置中获取
            appContext.setMaxAppAttempts(
                    configuration.getInteger(
                            YarnConfigOptions.APPLICATION_ATTEMPTS.key(),
                            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS));
            // 激活高可用性支持，可能包括设置 Zookeeper 配置等
            activateHighAvailabilitySupport(appContext);
        } else {
            // set number of application retries to 1 in the default case
            // 在未启用高可用性模式的默认情况下，设置应用程序重试次数为 1
            appContext.setMaxAppAttempts(
                    configuration.getInteger(YarnConfigOptions.APPLICATION_ATTEMPTS.key(), 1));
        }
        /**
         * 核心点4.构建用户存储jar等配置的路径
         */
        // 创建一个HashSet用于存储用户提供的JAR文件路径
        final Set<Path> userJarFiles = new HashSet<>();
        // 如果jobGraph（作业图）不为空
        if (jobGraph != null) {
            // 从jobGraph中获取用户JARs，将其转换为URI，然后转换为Path对象，并添加到userJarFiles集合中
            userJarFiles.addAll(
                    jobGraph.getUserJars().stream()
                            .map(f -> f.toUri())
                            .map(Path::new)
                            .collect(Collectors.toSet()));
        }
         // 从配置中获取JAR文件的URI列表
        final List<URI> jarUrls =
                ConfigUtils.decodeListFromConfig(configuration, PipelineOptions.JARS, URI::create);
        // 如果jarUrls不为空，且YARN集群的入口点类是YarnApplicationClusterEntryPoint
        if (jarUrls != null
                && YarnApplicationClusterEntryPoint.class.getName().equals(yarnClusterEntrypoint)) {
            // 将jarUrls中的URI转换为Path对象，并添加到userJarFiles集合中
            userJarFiles.addAll(jarUrls.stream().map(Path::new).collect(Collectors.toSet()));
        }
        // only for per job mode
        // 仅针对每个作业模式
        if (jobGraph != null) {
            // 遍历jobGraph中的用户Artifacts（可能是配置文件、数据文件等）
            for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry :
                    jobGraph.getUserArtifacts().entrySet()) {
                // only upload local files
                // 如果Artifact的文件路径不是远程路径
                if (!Utils.isRemotePath(entry.getValue().filePath)) {
                    // 创建一个表示本地文件路径的Path对象
                    Path localPath = new Path(entry.getValue().filePath);
                    // 使用fileUploader将本地文件上传到远程位置，并获取上传后的信息（路径和大小）
                    Tuple2<Path, Long> remoteFileInfo =
                            fileUploader.uploadLocalFileToRemote(localPath, entry.getKey());
                    // 更新jobGraph中该Artifact的远程路径
                    jobGraph.setUserArtifactRemotePath(
                            entry.getKey(), remoteFileInfo.f0.toString());
                }
            }
            // 将jobGraph中的用户Artifacts信息写入到配置中
            jobGraph.writeUserArtifactEntriesToConfiguration();
        }

        // 如果没有提供库目录或者提供的库目录为空
        if (providedLibDirs == null || providedLibDirs.isEmpty()) {
            // 将系统需要的库文件夹添加到需要传输的文件集合中
            addLibFoldersToShipFiles(systemShipFiles);
        }

        // Register all files in provided lib dirs as local resources with public visibility
        // and upload the remaining dependencies as local resources with APPLICATION visibility.

        // 注册提供的本地资源，并将这些资源的路径添加到系统类路径列表中
        final List<String> systemClassPaths = fileUploader.registerProvidedLocalResources();
        /**
         * 核心点5.注册本地/远程资源，并将其添加到本地环境中
         */
        // 注册多个本地资源（系统需要传输的文件），并将这些资源的路径添加到uploadedDependencies列表中
        final List<String> uploadedDependencies =
                fileUploader.registerMultipleLocalResources(
                        systemShipFiles, Path.CUR_DIR, LocalResourceType.FILE);
        // 将uploadedDependencies中的路径添加到系统类路径列表中
        systemClassPaths.addAll(uploadedDependencies);

        // upload and register ship-only files
        // Plugin files only need to be shipped and should not be added to classpath.
        // 上传并注册仅需要传输的文件
        // 插件文件只需要传输，不需要添加到类路径中
        if (providedLibDirs == null || providedLibDirs.isEmpty()) {
            Set<Path> shipOnlyFiles = new HashSet<>();
            addPluginsFoldersToShipFiles(shipOnlyFiles);
            // 注册多个仅需要传输的本地资源（插件文件夹）
            fileUploader.registerMultipleLocalResources(
                    shipOnlyFiles, Path.CUR_DIR, LocalResourceType.FILE);
        }
        // 如果shipArchives列表不为空
        if (!shipArchives.isEmpty()) {
            // 注册多个本地资源（归档文件），这些文件将被视为归档类型进行传输
            fileUploader.registerMultipleLocalResources(
                    shipArchives, Path.CUR_DIR, LocalResourceType.ARCHIVE);
        }

        // only for application mode
        // Python jar file only needs to be shipped and should not be added to classpath.
        // 仅针对应用程序模式
       // Python JAR文件只需要传输，不需要添加到类路径中
        if (YarnApplicationClusterEntryPoint.class.getName().equals(yarnClusterEntrypoint)
                && PackagedProgramUtils.isPython(configuration.get(APPLICATION_MAIN_CLASS))) {
            // 注册Python JAR文件作为本地资源，并指定其目标目录和类型为文件
            fileUploader.registerMultipleLocalResources(
                    Collections.singletonList(
                            new Path(PackagedProgramUtils.getPythonJar().toURI())),
                    ConfigConstants.DEFAULT_FLINK_OPT_DIR,
                    LocalResourceType.FILE);
        }

        // Upload and register user jars
        //上传并注册用户的Jar
        final List<String> userClassPaths =
                fileUploader.registerMultipleLocalResources(
                        userJarFiles,
                        userJarInclusion == YarnConfigOptions.UserJarInclusion.DISABLED
                                ? ConfigConstants.DEFAULT_FLINK_USR_LIB_DIR
                                : Path.CUR_DIR,
                        LocalResourceType.FILE);

        // usrlib in remote will be used first.
        // 检查是否提供了用户库目录（providedUsrLibDir）
        if (providedUsrLibDir.isPresent()) {
            // 如果提供了用户库目录，则注册该目录为本地资源，并获取其类路径
            final List<String> usrLibClassPaths =
                    fileUploader.registerMultipleLocalResources(
                            Collections.singletonList(providedUsrLibDir.get()),// 注册用户库目录
                            Path.CUR_DIR,// 相对于当前目录
                            LocalResourceType.FILE);// 资源类型为文件
            // 将用户库目录的类路径添加到用户类路径列表中
            userClassPaths.addAll(usrLibClassPaths);
        } else if (ClusterEntrypointUtils.tryFindUserLibDirectory().isPresent()) {
            // local usrlib will be automatically shipped if it exists and there is no remote
            // usrlib.
            // 如果没有提供用户库目录，但存在默认的用户库目录（local usrlib）
            // 并且没有远程的用户库，则自动上传本地用户库
            // 创建一个集合用于存储用户库目录下的文件
            final Set<File> usrLibShipFiles = new HashSet<>();
            // 将用户库目录下的文件添加到集合中
            addUsrLibFolderToShipFiles(usrLibShipFiles);
            // 将用户库目录下的文件注册为本地资源，并获取其类路径
            final List<String> usrLibClassPaths =
                    fileUploader.registerMultipleLocalResources(
                            usrLibShipFiles.stream()// 转换集合中的文件为Path对象
                                    .map(e -> new Path(e.toURI()))// 转换为URI，然后创建Path对象
                                    .collect(Collectors.toSet()),// 注意这里使用Collectors.toSet()是不正确的，应为Collectors.toList()
                            Path.CUR_DIR,// 相对于当前目录
                            LocalResourceType.FILE);// 资源类型为文件
            // 将用户库目录的类路径添加到用户类路径列表中
            userClassPaths.addAll(usrLibClassPaths);
        }
        // 根据用户配置决定系统类路径和用户类路径的合并顺序
        if (userJarInclusion == YarnConfigOptions.UserJarInclusion.ORDER) {
            // 如果配置为用户jar包含在后，则将用户类路径添加到系统类路径之后
            systemClassPaths.addAll(userClassPaths);
        }

        // normalize classpath by sorting
        // 对类路径进行排序，以规范化类路径
        // 注意：排序类路径通常不是必须的，除非有特定的需求
        Collections.sort(systemClassPaths);
        Collections.sort(userClassPaths);

        // classpath assembler
        // 组装类路径字符串
        StringBuilder classPathBuilder = new StringBuilder();
        // 如果配置为用户jar包含在前，则首先添加用户类路径
        if (userJarInclusion == YarnConfigOptions.UserJarInclusion.FIRST) {
            for (String userClassPath : userClassPaths) {
                classPathBuilder.append(userClassPath).append(File.pathSeparator);
            }
        }
        // 添加系统类路径到类路径字符串
        for (String classPath : systemClassPaths) {
            classPathBuilder.append(classPath).append(File.pathSeparator);
        }
        /**
         * 核心点6.上传Flink dist jar等并获取本地资源
         */
        // Setup jar for ApplicationMaster
        // 使用fileUploader上传Flink的jar包，并获取其本地资源描述符
        final YarnLocalResourceDescriptor localResourceDescFlinkJar =
                fileUploader.uploadFlinkDist(flinkJarPath);
        // 将Flink的jar包的资源键（即其在YARN中的唯一标识）添加到类路径构建器中
        classPathBuilder
                .append(localResourceDescFlinkJar.getResourceKey())
                .append(File.pathSeparator);
        /**
         * 核心点7.上传job.graph
         */
        // write job graph to tmp file and add it to local resource
        // TODO: server use user main method to generate job graph
        // 将作业图写入临时文件，并将其添加到本地资源中
       // TODO: 服务器使用用户的main方法生成作业图
       // 注释：这里假设jobGraph是一个已经生成的作业图对象
        if (jobGraph != null) {
            File tmpJobGraphFile = null;
            try {
                // 创建一个以appId为前缀的临时文件，用于存储序列化后的作业图
                tmpJobGraphFile = File.createTempFile(appId.toString(), null);
                // 使用文件输出流和对象输出流将作业图对象序列化到临时文件中
                try (FileOutputStream output = new FileOutputStream(tmpJobGraphFile);
                        ObjectOutputStream obOutput = new ObjectOutputStream(output)) {
                    obOutput.writeObject(jobGraph);
                }
                // 定义一个作业图文件名，这里使用"job.graph"作为默认文件名
                final String jobGraphFilename = "job.graph";
                // 将作业图文件名存储到配置中，以便后续引用
                configuration.set(JOB_GRAPH_FILE_PATH, jobGraphFilename);

                // 使用fileUploader注册临时文件作为本地资源，并设置相关参数
                // 包括文件名、文件路径、权限、资源类型、是否可执行和是否归档
                fileUploader.registerSingleLocalResource(
                        jobGraphFilename,// 资源名称（即文件名）
                        new Path(tmpJobGraphFile.toURI()),// 资源的本地路径
                        "",//权限控制默认为空
                        LocalResourceType.FILE,// 资源类型为文件
                        true,// 是否可执行（这里设为true，但作业图文件通常不需要执行）
                        false);// 是否归档（这里设为false，表示不归档）
                // 将作业图的资源名（文件名）添加到类路径构建器中
                classPathBuilder.append(jobGraphFilename).append(File.pathSeparator);
            } catch (Exception e) {
                // 如果在添加作业图到本地资源过程中发生异常，记录警告并抛出异常
                LOG.warn("Add job graph to local resource fail.");
                throw e;
            } finally {
                // 无论是否发生异常，都尝试删除临时文件
                if (tmpJobGraphFile != null && !tmpJobGraphFile.delete()) {
                    LOG.warn("Fail to delete temporary file {}.", tmpJobGraphFile.toPath());
                }
            }
        }
        /**
         * 核心点7.创建上传存储Flink配置文件的临时文件
         */
        // Upload the flink configuration
        // write out configuration file
        // todo 创建一个用于存储Flink配置文件的临时文件
        File tmpConfigurationFile = null;
        try {
            // 获取Flink配置文件的名称
            String flinkConfigFileName = GlobalConfiguration.getFlinkConfFilename();
            // 根据应用ID和Flink配置文件名创建一个临时文件
            tmpConfigurationFile = File.createTempFile(appId + "-" + flinkConfigFileName, null);

            // remove localhost bind hosts as they render production clusters unusable
            // 移除可能存在的绑定到localhost的主机设置，因为这会使得生产集群无法使用
            // 移除JobManager的BIND_HOST设置
            removeLocalhostBindHostSetting(configuration, JobManagerOptions.BIND_HOST);
            // 移除TaskManager的BIND_HOST设置
            removeLocalhostBindHostSetting(configuration, TaskManagerOptions.BIND_HOST);
            // this setting is unconditionally overridden anyway, so we remove it for clarity
            // 由于这个设置总是会被无条件地覆盖，为了清晰起见，我们将其从配置中移除
            configuration.removeConfig(TaskManagerOptions.HOST);

            // 将修改后的配置写入到临时文件中
            BootstrapTools.writeConfiguration(configuration, tmpConfigurationFile);
            // 注册临时文件作为本地资源，以便在YARN上执行时使用
            fileUploader.registerSingleLocalResource(
                    flinkConfigFileName,// 资源名称（即文件名）
                    new Path(tmpConfigurationFile.getAbsolutePath()),// 资源的本地路径
                    "",// 权限设置默认为空字符串
                    LocalResourceType.FILE,// 资源类型为文件
                    true, // 是否可执行
                    true);// 是否归档
            // 将Flink配置文件的资源名（文件名）添加到类路径构建器中
            classPathBuilder.append(flinkConfigFileName).append(File.pathSeparator);
        } finally {
            // 无论是否发生异常，都尝试删除临时文件
            if (tmpConfigurationFile != null && !tmpConfigurationFile.delete()) {
                LOG.warn("Fail to delete temporary file {}.", tmpConfigurationFile.toPath());
            }
        }

        // 如果用户指定了用户JAR包的包含顺序为"最后"（LAST）
        if (userJarInclusion == YarnConfigOptions.UserJarInclusion.LAST) {
            // 遍历用户指定的类路径列表
            for (String userClassPath : userClassPaths) {
                // 将用户指定的类路径添加到类路径构建器中，用文件分隔符分隔
                classPathBuilder.append(userClassPath).append(File.pathSeparator);
            }
        }

        // To support Yarn Secure Integration Test Scenario
        // In Integration test setup, the Yarn containers created by YarnMiniCluster does not have
        // the Yarn site XML
        // and KRB5 configuration files. We are adding these files as container local resources for
        // the container
        // applications (JM/TMs) to have proper secure cluster setup
        // 为了支持YARN安全集成测试场景
        // 在集成测试设置中，YarnMiniCluster创建的YARN容器并不包含YARN的site XML文件和KRB5配置文件。
        // 我们将这些文件作为容器本地资源添加到容器中，
        // 以便JobManager和TaskManagers能够拥有正确的安全集群设置。
        Path remoteYarnSiteXmlPath = null;
        // 如果环境变量"IN_TESTS"已设置，则表示正在进行测试
        if (System.getenv("IN_TESTS") != null) {
            // 在YARN配置文件目录下创建一个YARN配置文件（yarn-site.xml）的File对象
            File f = new File(System.getenv("YARN_CONF_DIR"), Utils.YARN_SITE_FILE_NAME);
            // 记录日志，显示将YARN配置文件添加到AM容器本地资源中
            LOG.info(
                    "Adding Yarn configuration {} to the AM container local resource bucket",
                    f.getAbsolutePath());
            // 创建YARN配置文件的Path对象
            Path yarnSitePath = new Path(f.getAbsolutePath());
            // 将YARN配置文件注册为本地资源，并获取其远程路径
            remoteYarnSiteXmlPath =
                    fileUploader
                            .registerSingleLocalResource(
                                    Utils.YARN_SITE_FILE_NAME,
                                    yarnSitePath,
                                    "",
                                    LocalResourceType.FILE,
                                    false,
                                    false)
                            .getPath();
            // 如果系统属性"java.security.krb5.conf"已设置（表示KRB5配置文件的位置）
            if (System.getProperty("java.security.krb5.conf") != null) {
                // 将KRB5配置文件的路径设置到Flink配置中
                configuration.set(
                        SecurityOptions.KERBEROS_KRB5_PATH,
                        System.getProperty("java.security.krb5.conf"));
            }
        }
        /**
         * 核心点8.创建上传kerberos相关配置
         */
        // 声明KRB5配置文件的远程路径变量
        Path remoteKrb5Path = null;
        // 标记是否存在KRB5配置文件
        boolean hasKrb5 = false;
        // 从配置中获取KRB5配置文件的路径
        String krb5Config = configuration.get(SecurityOptions.KERBEROS_KRB5_PATH);
        // 如果KRB5配置文件路径不为空或仅包含空白字符
        if (!StringUtils.isNullOrWhitespaceOnly(krb5Config)) {
            // 创建KRB5配置文件的File对象
            final File krb5 = new File(krb5Config);
            // 记录日志，显示将KRB5配置文件添加到AM容器的本地资源中
            LOG.info(
                    "Adding KRB5 configuration {} to the AM container local resource bucket",
                    krb5.getAbsolutePath());
            // 创建KRB5配置文件的Path对象
            final Path krb5ConfPath = new Path(krb5.getAbsolutePath());
            // 将KRB5配置文件注册为本地资源，并获取其远程路径
            remoteKrb5Path =
                    fileUploader
                            .registerSingleLocalResource(
                                    Utils.KRB5_FILE_NAME,
                                    krb5ConfPath,
                                    "",
                                    LocalResourceType.FILE,
                                    false,
                                    false)
                            .getPath();
            hasKrb5 = true;
        }
         // 声明keytab文件的远程路径变量
        Path remotePathKeytab = null;
        // 声明本地化的keytab文件路径变量
        String localizedKeytabPath = null;
        // 从配置中获取keytab文件的路径
        String keytab = configuration.get(SecurityOptions.KERBEROS_LOGIN_KEYTAB);
        if (keytab != null) {
            // 获取是否应该本地化keytab文件的配置
            boolean localizeKeytab = flinkConfiguration.get(YarnConfigOptions.SHIP_LOCAL_KEYTAB);
            // 获取本地化keytab文件的目标路径
            localizedKeytabPath = flinkConfiguration.get(YarnConfigOptions.LOCALIZED_KEYTAB_PATH);
            // 如果应该本地化keytab文件
            if (localizeKeytab) {
                // Localize the keytab to YARN containers via local resource.
                LOG.info("Adding keytab {} to the AM container local resource bucket", keytab);
                // 将keytab文件注册为本地资源
                remotePathKeytab =
                        fileUploader
                                .registerSingleLocalResource(
                                        localizedKeytabPath,
                                        new Path(keytab),
                                        "",
                                        LocalResourceType.FILE,
                                        false,
                                        false)
                                .getPath();
            } else {
                // // Assume Keytab is pre-installed in the container.
                // 假设keytab文件已经预装在容器中
                // 使用配置中指定的本地化keytab文件路径
                localizedKeytabPath =
                        flinkConfiguration.get(YarnConfigOptions.LOCALIZED_KEYTAB_PATH);
            }
        }
        // 从配置中获取JobManager进程配置
        final JobManagerProcessSpec processSpec =
                JobManagerProcessUtils.processSpecFromConfigWithNewOptionToInterpretLegacyHeap(
                        flinkConfiguration, JobManagerOptions.TOTAL_PROCESS_MEMORY);
        /**
         * 核心点9.设置Application Master容器的启动上下文
         */
        // todo 核心 设置Application Master容器的启动上下文
        final ContainerLaunchContext amContainer =
                setupApplicationMasterContainer(yarnClusterEntrypoint, hasKrb5, processSpec);

        // 检查是否启用委托令牌
        boolean fetchToken = configuration.get(SecurityOptions.DELEGATION_TOKENS_ENABLED);
        // 创建Kerberos登录提供器
        KerberosLoginProvider kerberosLoginProvider = new KerberosLoginProvider(configuration);
        if (kerberosLoginProvider.isLoginPossible(true)) {
            // 如果Kerberos登录是可能的，则设置令牌
            setTokensFor(amContainer, fetchToken);
        } else {
            LOG.info(
                    "Cannot use kerberos delegation token manager, no valid kerberos credentials provided.");
        }
        // 将已注册的本地资源设置到Application Master容器中
        amContainer.setLocalResources(fileUploader.getRegisteredLocalResources());
        // 关闭文件上传器
        fileUploader.close();
        // 为Application Master容器设置访问控制列表（ACLs）
        Utils.setAclsFor(amContainer, flinkConfiguration);
        /**
         * 核心点10.生成 ApplicationMaster 所需的环境变量映射
         */
        // Setup CLASSPATH and environment variables for ApplicationMaster
        // todo 核心 生成 ApplicationMaster 所需的环境变量映射
        final Map<String, String> appMasterEnv =
                generateApplicationMasterEnv(
                        fileUploader,// 文件上传器
                        classPathBuilder.toString(), // 类路径的字符串表示
                        localResourceDescFlinkJar.toString(),// Flink JAR 文件作为本地资源的字符串表示
                        appId.toString());// 应用ID的字符串表示
        // 如果已经本地化了keytab文件
        if (localizedKeytabPath != null) {
            // 设置本地化的keytab文件路径到环境变量
            appMasterEnv.put(YarnConfigKeys.LOCAL_KEYTAB_PATH, localizedKeytabPath);
            // 获取Kerberos登录主体（用户名）
            String principal = configuration.get(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL);
            // 设置Kerberos登录主体到环境变量
            appMasterEnv.put(YarnConfigKeys.KEYTAB_PRINCIPAL, principal);
            // 如果远程keytab文件路径不为空
            if (remotePathKeytab != null) {
                // 设置远程keytab文件路径到环境变量
                appMasterEnv.put(YarnConfigKeys.REMOTE_KEYTAB_PATH, remotePathKeytab.toString());
            }
        }

        // To support Yarn Secure Integration Test Scenario
        // 为了支持 Yarn 安全集成测试场景
        // 如果远程的 yarn-site.xml 路径不为空
        if (remoteYarnSiteXmlPath != null) {
            // 设置远程 yarn-site.xml 文件的路径到环境变量
            appMasterEnv.put(
                    YarnConfigKeys.ENV_YARN_SITE_XML_PATH, remoteYarnSiteXmlPath.toString());
        }
        // 如果远程的 krb5.conf 路径不为空
        if (remoteKrb5Path != null) {
            // 设置远程 krb5.conf 文件的路径到环境变量
            appMasterEnv.put(YarnConfigKeys.ENV_KRB5_PATH, remoteKrb5Path.toString());
        }
        // 将生成的环境变量设置到 ApplicationMaster 容器中
        amContainer.setEnvironment(appMasterEnv);

        // Set up resource type requirements for ApplicationMaster
        // 为 ApplicationMaster 设置资源需求
        // 创建一个 Resource 对象
        Resource capability = Records.newRecord(Resource.class);
        // 设置内存大小为集群规格中指定的主节点内存大小（以MB为单位）
        capability.setMemorySize(clusterSpecification.getMasterMemoryMB());
        // 设置虚拟核心数为 Flink 配置中指定的 ApplicationMaster 虚拟核心数
        capability.setVirtualCores(flinkConfiguration.get(YarnConfigOptions.APP_MASTER_VCORES));

        // 优先使用自定义的应用名称，如果没有提供则使用默认的应用名称
        final String customApplicationName = customName != null ? customName : applicationName;
        appContext.setApplicationName(customApplicationName);
        // 优先使用自定义的应用类型，如果没有提供则默认为 "Apache Flink"
        appContext.setApplicationType(applicationType != null ? applicationType : "Apache Flink");
        // 设置 ApplicationMaster 容器配置
        appContext.setAMContainerSpec(amContainer);
        // 设置应用所需的资源
        appContext.setResource(capability);

        // Set priority for application
        // 设置应用的优先级
        int priorityNum = flinkConfiguration.get(YarnConfigOptions.APPLICATION_PRIORITY);
        if (priorityNum >= 0) {
            Priority priority = Priority.newInstance(priorityNum);
            appContext.setPriority(priority);
        }

        // 如果指定了 YARN 队列，则设置应用的队列
        if (yarnQueue != null) {
            appContext.setQueue(yarnQueue);
        }
        // 设置应用的节点标签（Node Label）
        setApplicationNodeLabel(appContext);
        // 设置应用的标签（Tags）
        setApplicationTags(appContext);

        // add a hook to clean up in case deployment fails
        // 添加一个钩子，用于在部署失败时执行清理操作
        Thread deploymentFailureHook =
                new DeploymentFailureHook(yarnApplication, fileUploader.getApplicationDir());
        Runtime.getRuntime().addShutdownHook(deploymentFailureHook);
        /**
         * 核心点10.提交 ApplicationMaster 运行ApplicationMaster
         */
        // 日志记录：提交 ApplicationMaster 运行ApplicationMaster
        LOG.info("Submitting application master " + appId);
        yarnClient.submitApplication(appContext);

        // 日志记录：等待集群分配资源
        LOG.info("Waiting for the cluster to be allocated");
        final long startTime = System.currentTimeMillis();
        long lastLogTime = System.currentTimeMillis();
        // 定义一个 ApplicationReport 对象，用于存储 YARN 应用程序的详细信息
        ApplicationReport report;
        // 定义一个变量 lastAppState 来存储上一次检查到的 YARN 应用程序状态
        YarnApplicationState lastAppState = YarnApplicationState.NEW;
        loop:// 使用标签 loop 来标记这个无限循环，方便后续使用 break loop 跳出循环
        while (true) {
            try {
                // 调用 yarnClient 的 getApplicationReport 方法，通过 appId 获取应用程序的详细信息
                report = yarnClient.getApplicationReport(appId);
            } catch (IOException e) {
                // 如果在获取应用程序报告时发生 IO 异常，则抛出 YarnDeploymentException 异常
                throw new YarnDeploymentException("Failed to deploy the cluster.", e);
            }
            // 获取当前应用程序的状态
            YarnApplicationState appState = report.getYarnApplicationState();
            // 在日志中输出当前应用程序的状态
            LOG.debug("Application State: {}", appState);
            // 使用 switch 语句根据应用程序的状态进行不同的处理
            switch (appState) {
                // 如果应用程序状态是 FAILED 或 KILLED
                case FAILED:
                case KILLED:
                    // 抛出 YarnDeploymentException 异常，并附带详细的错误信息
                    throw new YarnDeploymentException(
                            "The YARN application unexpectedly switched to state "
                                    + appState
                                    + " during deployment. \n"
                                    + "Diagnostics from YARN: "
                                    + report.getDiagnostics()
                                    + "\n"
                                    + "If log aggregation is enabled on your cluster, use this command to further investigate the issue:\n"
                                    + "yarn logs -applicationId "
                                    + appId);
                    // break ..
                case RUNNING:// 如果应用程序状态是 RUNNING
                    // 在日志中输出应用程序已成功部署的信息
                    LOG.info("YARN application has been deployed successfully.");
                    break loop; // 使用 break loop 跳出循环
                // 如果应用程序状态是 FINISHED
                case FINISHED:
                    LOG.info("YARN application has been finished successfully.");
                    break loop; // 使用 break loop 跳出循环
                // 如果应用程序状态是其他值
                default:
                    // 如果当前状态与上一次状态不同，则在日志中输出正在部署集群的信息以及当前状态
                    if (appState != lastAppState) {
                        LOG.info("Deploying cluster, current state " + appState);
                    }
                    if (System.currentTimeMillis() - lastLogTime > 60000) {
                        lastLogTime = System.currentTimeMillis();
                        LOG.info(
                                "Deployment took more than {} seconds. Please check if the requested resources are available in the YARN cluster",
                                (lastLogTime - startTime) / 1000);
                    }
            }
            // 更新 lastAppState 为当前状态
            lastAppState = appState;
            Thread.sleep(250);
        }

        // since deployment was successful, remove the hook
        ShutdownHookUtil.removeShutdownHook(deploymentFailureHook, getClass().getSimpleName(), LOG);
        return report;
    }

    private void removeLocalhostBindHostSetting(
            Configuration configuration, ConfigOption<?> option) {
        configuration
                .getOptional(option)
                .filter(bindHost -> bindHost.equals("localhost"))
                .ifPresent(
                        bindHost -> {
                            LOG.info(
                                    "Removing 'localhost' {} setting from effective configuration; using '0.0.0.0' instead.",
                                    option);
                            configuration.removeConfig(option);
                        });
    }

    private void setTokensFor(ContainerLaunchContext containerLaunchContext, boolean fetchToken)
            throws Exception {
        Credentials credentials = new Credentials();

        LOG.info("Loading delegation tokens available locally to add to the AM container");
        // for user
        UserGroupInformation currUsr = UserGroupInformation.getCurrentUser();

        Collection<Token<? extends TokenIdentifier>> usrTok =
                currUsr.getCredentials().getAllTokens();
        for (Token<? extends TokenIdentifier> token : usrTok) {
            LOG.info("Adding user token " + token.getService() + " with " + token);
            credentials.addToken(token.getService(), token);
        }

        if (fetchToken) {
            LOG.info("Fetching delegation tokens to add to the AM container.");
            DelegationTokenManager delegationTokenManager =
                    new DefaultDelegationTokenManager(flinkConfiguration, null, null, null);
            DelegationTokenContainer container = new DelegationTokenContainer();
            delegationTokenManager.obtainDelegationTokens(container);

            // This is here for backward compatibility to make log aggregation work
            for (Map.Entry<String, byte[]> e : container.getTokens().entrySet()) {
                if (e.getKey().equals("hadoopfs")) {
                    credentials.addAll(HadoopDelegationTokenConverter.deserialize(e.getValue()));
                }
            }
        }

        ByteBuffer tokens = ByteBuffer.wrap(HadoopDelegationTokenConverter.serialize(credentials));
        containerLaunchContext.setTokens(tokens);

        LOG.info("Delegation tokens added to the AM container.");
    }

    /**
     * Returns the configured remote target home directory if set, otherwise returns the default
     * home directory.
     *
     * @param defaultFileSystem default file system used
     * @return the remote target home directory
     */
    @VisibleForTesting
    Path getStagingDir(FileSystem defaultFileSystem) throws IOException {
        final String configuredStagingDir =
                flinkConfiguration.get(YarnConfigOptions.STAGING_DIRECTORY);
        if (configuredStagingDir == null) {
            return defaultFileSystem.getHomeDirectory();
        }
        FileSystem stagingDirFs =
                new Path(configuredStagingDir).getFileSystem(defaultFileSystem.getConf());
        return stagingDirFs.makeQualified(new Path(configuredStagingDir));
    }

    private int getFileReplication() {
        final int yarnFileReplication =
                yarnConfiguration.getInt(
                        DFSConfigKeys.DFS_REPLICATION_KEY, DFSConfigKeys.DFS_REPLICATION_DEFAULT);
        final int fileReplication = flinkConfiguration.get(YarnConfigOptions.FILE_REPLICATION);
        return fileReplication > 0 ? fileReplication : yarnFileReplication;
    }

    private static String encodeYarnLocalResourceDescriptorListToString(
            List<YarnLocalResourceDescriptor> resources) {
        return String.join(
                LOCAL_RESOURCE_DESCRIPTOR_SEPARATOR,
                resources.stream()
                        .map(YarnLocalResourceDescriptor::toString)
                        .collect(Collectors.toList()));
    }

    /**
     * Kills YARN application and stops YARN client.
     *
     * <p>Use this method to kill the App before it has been properly deployed
     */
    private void failSessionDuringDeployment(
            YarnClient yarnClient, YarnClientApplication yarnApplication) {
        LOG.info("Killing YARN application");

        try {
            yarnClient.killApplication(
                    yarnApplication.getNewApplicationResponse().getApplicationId());
        } catch (Exception e) {
            // we only log a debug message here because the "killApplication" call is a best-effort
            // call (we don't know if the application has been deployed when the error occurred).
            LOG.debug("Error while killing YARN application", e);
        }
    }

    private static class ClusterResourceDescription {
        public final long totalFreeMemory;
        public final long containerLimit;
        public final long[] nodeManagersFree;

        public ClusterResourceDescription(
                long totalFreeMemory, long containerLimit, long[] nodeManagersFree) {
            this.totalFreeMemory = totalFreeMemory;
            this.containerLimit = containerLimit;
            this.nodeManagersFree = nodeManagersFree;
        }
    }

    private ClusterResourceDescription getCurrentFreeClusterResources(YarnClient yarnClient)
            throws YarnException, IOException {
        List<NodeReport> nodes = yarnClient.getNodeReports(NodeState.RUNNING);

        int totalFreeMemory = 0;
        long containerLimit = 0;
        long[] nodeManagersFree = new long[nodes.size()];

        for (int i = 0; i < nodes.size(); i++) {
            NodeReport rep = nodes.get(i);
            long free =
                    rep.getCapability().getMemorySize()
                            - (rep.getUsed() != null ? rep.getUsed().getMemorySize() : 0);
            nodeManagersFree[i] = free;
            totalFreeMemory += free;
            if (free > containerLimit) {
                containerLimit = free;
            }
        }
        return new ClusterResourceDescription(totalFreeMemory, containerLimit, nodeManagersFree);
    }

    @Override
    public String getClusterDescription() {

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);

            YarnClusterMetrics metrics = yarnClient.getYarnClusterMetrics();

            ps.append("NodeManagers in the ClusterClient " + metrics.getNumNodeManagers());
            List<NodeReport> nodes = yarnClient.getNodeReports(NodeState.RUNNING);
            final String format = "|%-16s |%-16s %n";
            ps.printf("|Property         |Value          %n");
            ps.println("+---------------------------------------+");
            long totalMemory = 0;
            int totalCores = 0;
            for (NodeReport rep : nodes) {
                final Resource res = rep.getCapability();
                totalMemory += res.getMemorySize();
                totalCores += res.getVirtualCores();
                ps.format(format, "NodeID", rep.getNodeId());
                ps.format(format, "Memory", getDisplayMemory(res.getMemorySize()));
                ps.format(format, "vCores", res.getVirtualCores());
                ps.format(format, "HealthReport", rep.getHealthReport());
                ps.format(format, "Containers", rep.getNumContainers());
                ps.println("+---------------------------------------+");
            }
            ps.println(
                    "Summary: totalMemory "
                            + getDisplayMemory(totalMemory)
                            + " totalCores "
                            + totalCores);
            List<QueueInfo> qInfo = yarnClient.getAllQueues();
            for (QueueInfo q : qInfo) {
                ps.println(
                        "Queue: "
                                + q.getQueueName()
                                + ", Current Capacity: "
                                + q.getCurrentCapacity()
                                + " Max Capacity: "
                                + q.getMaximumCapacity()
                                + " Applications: "
                                + q.getApplications().size());
            }
            return baos.toString();
        } catch (Exception e) {
            throw new RuntimeException("Couldn't get cluster description", e);
        }
    }

    private void activateHighAvailabilitySupport(ApplicationSubmissionContext appContext)
            throws InvocationTargetException, IllegalAccessException {

        ApplicationSubmissionContextReflector reflector =
                ApplicationSubmissionContextReflector.getInstance();

        reflector.setKeepContainersAcrossApplicationAttempts(appContext, true);

        reflector.setAttemptFailuresValidityInterval(
                appContext,
                flinkConfiguration.get(
                        YarnConfigOptions.APPLICATION_ATTEMPT_FAILURE_VALIDITY_INTERVAL));
    }

    private void setApplicationTags(final ApplicationSubmissionContext appContext)
            throws InvocationTargetException, IllegalAccessException {

        final ApplicationSubmissionContextReflector reflector =
                ApplicationSubmissionContextReflector.getInstance();
        final String tagsString = flinkConfiguration.get(YarnConfigOptions.APPLICATION_TAGS);

        final Set<String> applicationTags = new HashSet<>();

        // Trim whitespace and cull empty tags
        for (final String tag : tagsString.split(",")) {
            final String trimmedTag = tag.trim();
            if (!trimmedTag.isEmpty()) {
                applicationTags.add(trimmedTag);
            }
        }

        reflector.setApplicationTags(appContext, applicationTags);
    }

    private void setApplicationNodeLabel(final ApplicationSubmissionContext appContext)
            throws InvocationTargetException, IllegalAccessException {

        if (nodeLabel != null) {
            final ApplicationSubmissionContextReflector reflector =
                    ApplicationSubmissionContextReflector.getInstance();
            reflector.setApplicationNodeLabel(appContext, nodeLabel);
        }
    }

    /**
     * Singleton object which uses reflection to determine whether the {@link
     * ApplicationSubmissionContext} supports various methods which, depending on the Hadoop
     * version, may or may not be supported.
     *
     * <p>If an unsupported method is invoked, nothing happens.
     *
     * <p>Currently three methods are proxied: - setApplicationTags (>= 2.4.0) -
     * setAttemptFailuresValidityInterval (>= 2.6.0) - setKeepContainersAcrossApplicationAttempts
     * (>= 2.4.0) - setNodeLabelExpression (>= 2.6.0)
     */
    private static class ApplicationSubmissionContextReflector {
        private static final Logger LOG =
                LoggerFactory.getLogger(ApplicationSubmissionContextReflector.class);

        private static final ApplicationSubmissionContextReflector instance =
                new ApplicationSubmissionContextReflector(ApplicationSubmissionContext.class);

        public static ApplicationSubmissionContextReflector getInstance() {
            return instance;
        }

        private static final String APPLICATION_TAGS_METHOD_NAME = "setApplicationTags";
        private static final String ATTEMPT_FAILURES_METHOD_NAME =
                "setAttemptFailuresValidityInterval";
        private static final String KEEP_CONTAINERS_METHOD_NAME =
                "setKeepContainersAcrossApplicationAttempts";
        private static final String NODE_LABEL_EXPRESSION_NAME = "setNodeLabelExpression";

        private final Method applicationTagsMethod;
        private final Method attemptFailuresValidityIntervalMethod;
        private final Method keepContainersMethod;
        @Nullable private final Method nodeLabelExpressionMethod;

        private ApplicationSubmissionContextReflector(Class<ApplicationSubmissionContext> clazz) {
            Method applicationTagsMethod;
            Method attemptFailuresValidityIntervalMethod;
            Method keepContainersMethod;
            Method nodeLabelExpressionMethod;

            try {
                // this method is only supported by Hadoop 2.4.0 onwards
                applicationTagsMethod = clazz.getMethod(APPLICATION_TAGS_METHOD_NAME, Set.class);
                LOG.debug(
                        "{} supports method {}.",
                        clazz.getCanonicalName(),
                        APPLICATION_TAGS_METHOD_NAME);
            } catch (NoSuchMethodException e) {
                LOG.debug(
                        "{} does not support method {}.",
                        clazz.getCanonicalName(),
                        APPLICATION_TAGS_METHOD_NAME);
                // assign null because the Hadoop version apparently does not support this call.
                applicationTagsMethod = null;
            }

            this.applicationTagsMethod = applicationTagsMethod;

            try {
                // this method is only supported by Hadoop 2.6.0 onwards
                attemptFailuresValidityIntervalMethod =
                        clazz.getMethod(ATTEMPT_FAILURES_METHOD_NAME, long.class);
                LOG.debug(
                        "{} supports method {}.",
                        clazz.getCanonicalName(),
                        ATTEMPT_FAILURES_METHOD_NAME);
            } catch (NoSuchMethodException e) {
                LOG.debug(
                        "{} does not support method {}.",
                        clazz.getCanonicalName(),
                        ATTEMPT_FAILURES_METHOD_NAME);
                // assign null because the Hadoop version apparently does not support this call.
                attemptFailuresValidityIntervalMethod = null;
            }

            this.attemptFailuresValidityIntervalMethod = attemptFailuresValidityIntervalMethod;

            try {
                // this method is only supported by Hadoop 2.4.0 onwards
                keepContainersMethod = clazz.getMethod(KEEP_CONTAINERS_METHOD_NAME, boolean.class);
                LOG.debug(
                        "{} supports method {}.",
                        clazz.getCanonicalName(),
                        KEEP_CONTAINERS_METHOD_NAME);
            } catch (NoSuchMethodException e) {
                LOG.debug(
                        "{} does not support method {}.",
                        clazz.getCanonicalName(),
                        KEEP_CONTAINERS_METHOD_NAME);
                // assign null because the Hadoop version apparently does not support this call.
                keepContainersMethod = null;
            }

            this.keepContainersMethod = keepContainersMethod;

            try {
                nodeLabelExpressionMethod =
                        clazz.getMethod(NODE_LABEL_EXPRESSION_NAME, String.class);
                LOG.debug(
                        "{} supports method {}.",
                        clazz.getCanonicalName(),
                        NODE_LABEL_EXPRESSION_NAME);
            } catch (NoSuchMethodException e) {
                LOG.debug(
                        "{} does not support method {}.",
                        clazz.getCanonicalName(),
                        NODE_LABEL_EXPRESSION_NAME);
                nodeLabelExpressionMethod = null;
            }

            this.nodeLabelExpressionMethod = nodeLabelExpressionMethod;
        }

        public void setApplicationTags(
                ApplicationSubmissionContext appContext, Set<String> applicationTags)
                throws InvocationTargetException, IllegalAccessException {
            if (applicationTagsMethod != null) {
                LOG.debug(
                        "Calling method {} of {}.",
                        applicationTagsMethod.getName(),
                        appContext.getClass().getCanonicalName());
                applicationTagsMethod.invoke(appContext, applicationTags);
            } else {
                LOG.debug(
                        "{} does not support method {}. Doing nothing.",
                        appContext.getClass().getCanonicalName(),
                        APPLICATION_TAGS_METHOD_NAME);
            }
        }

        public void setApplicationNodeLabel(
                ApplicationSubmissionContext appContext, String nodeLabel)
                throws InvocationTargetException, IllegalAccessException {
            if (nodeLabelExpressionMethod != null) {
                LOG.debug(
                        "Calling method {} of {}.",
                        nodeLabelExpressionMethod.getName(),
                        appContext.getClass().getCanonicalName());
                nodeLabelExpressionMethod.invoke(appContext, nodeLabel);
            } else {
                LOG.debug(
                        "{} does not support method {}. Doing nothing.",
                        appContext.getClass().getCanonicalName(),
                        NODE_LABEL_EXPRESSION_NAME);
            }
        }

        public void setAttemptFailuresValidityInterval(
                ApplicationSubmissionContext appContext, long validityInterval)
                throws InvocationTargetException, IllegalAccessException {
            if (attemptFailuresValidityIntervalMethod != null) {
                LOG.debug(
                        "Calling method {} of {}.",
                        attemptFailuresValidityIntervalMethod.getName(),
                        appContext.getClass().getCanonicalName());
                attemptFailuresValidityIntervalMethod.invoke(appContext, validityInterval);
            } else {
                LOG.debug(
                        "{} does not support method {}. Doing nothing.",
                        appContext.getClass().getCanonicalName(),
                        ATTEMPT_FAILURES_METHOD_NAME);
            }
        }

        public void setKeepContainersAcrossApplicationAttempts(
                ApplicationSubmissionContext appContext, boolean keepContainers)
                throws InvocationTargetException, IllegalAccessException {

            if (keepContainersMethod != null) {
                LOG.debug(
                        "Calling method {} of {}.",
                        keepContainersMethod.getName(),
                        appContext.getClass().getCanonicalName());
                keepContainersMethod.invoke(appContext, keepContainers);
            } else {
                LOG.debug(
                        "{} does not support method {}. Doing nothing.",
                        appContext.getClass().getCanonicalName(),
                        KEEP_CONTAINERS_METHOD_NAME);
            }
        }
    }

    private static class YarnDeploymentException extends RuntimeException {
        private static final long serialVersionUID = -812040641215388943L;

        public YarnDeploymentException(String message) {
            super(message);
        }

        public YarnDeploymentException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private class DeploymentFailureHook extends Thread {

        private final YarnClient yarnClient;
        private final YarnClientApplication yarnApplication;
        private final Path yarnFilesDir;

        DeploymentFailureHook(YarnClientApplication yarnApplication, Path yarnFilesDir) {
            this.yarnApplication = Preconditions.checkNotNull(yarnApplication);
            this.yarnFilesDir = Preconditions.checkNotNull(yarnFilesDir);

            // A new yarn client need to be created in shutdown hook in order to avoid
            // the yarn client has been closed by YarnClusterDescriptor.
            this.yarnClient = YarnClient.createYarnClient();
            this.yarnClient.init(yarnConfiguration);
        }

        @Override
        public void run() {
            LOG.info("Cancelling deployment from Deployment Failure Hook");
            yarnClient.start();
            failSessionDuringDeployment(yarnClient, yarnApplication);
            yarnClient.stop();
            LOG.info("Deleting files in {}.", yarnFilesDir);
            try {
                FileSystem fs = FileSystem.get(yarnConfiguration);

                if (!fs.delete(yarnFilesDir, true)) {
                    throw new IOException(
                            "Deleting files in " + yarnFilesDir + " was unsuccessful");
                }

                fs.close();
            } catch (IOException e) {
                LOG.error("Failed to delete Flink Jar and configuration files in HDFS", e);
            }
        }
    }

    @VisibleForTesting
    void addLibFoldersToShipFiles(Collection<Path> effectiveShipFiles) {
        // Add lib folder to the ship files if the environment variable is set.
        // This is for convenience when running from the command-line.
        // (for other files users explicitly set the ship files)
        String libDir = System.getenv().get(ENV_FLINK_LIB_DIR);
        if (libDir != null) {
            File directoryFile = new File(libDir);
            if (directoryFile.isDirectory()) {
                effectiveShipFiles.add(getPathFromLocalFile(directoryFile));
            } else {
                throw new YarnDeploymentException(
                        "The environment variable '"
                                + ENV_FLINK_LIB_DIR
                                + "' is set to '"
                                + libDir
                                + "' but the directory doesn't exist.");
            }
        } else if (shipFiles.isEmpty()) {
            LOG.warn(
                    "Environment variable '{}' not set and ship files have not been provided manually. "
                            + "Not shipping any library files.",
                    ENV_FLINK_LIB_DIR);
        }
    }

    @VisibleForTesting
    void addUsrLibFolderToShipFiles(Collection<File> effectiveShipFiles) {
        // Add usrlib folder to the ship files if it exists
        // Classes in the folder will be loaded by UserClassLoader if CLASSPATH_INCLUDE_USER_JAR is
        // DISABLED.
        ClusterEntrypointUtils.tryFindUserLibDirectory()
                .ifPresent(
                        usrLibDirFile -> {
                            effectiveShipFiles.add(usrLibDirFile);
                            LOG.info(
                                    "usrlib: {} will be shipped automatically.",
                                    usrLibDirFile.getAbsolutePath());
                        });
    }

    @VisibleForTesting
    void addPluginsFoldersToShipFiles(Collection<Path> effectiveShipFiles) {
        final Optional<File> pluginsDir = PluginConfig.getPluginsDir();
        pluginsDir.ifPresent(dir -> effectiveShipFiles.add(getPathFromLocalFile(dir)));
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 设置应用管理器容器
     * @param yarnClusterEntrypoint  YARN集群的入口点，即启动JobManager的类名
     * @param hasKrb5 是否包含Kerberos认证（krb5）
     * @param processSpec JobManager进程规格
    */
    ContainerLaunchContext setupApplicationMasterContainer(
            String yarnClusterEntrypoint, boolean hasKrb5, JobManagerProcessSpec processSpec) {
        // ------------------ Prepare Application Master Container  ------------------------------

        // respect custom JVM options in the YAML file
        // 尊重YAML文件中自定义的JVM选项
        List<ConfigOption<String>> jvmOptions =
                Arrays.asList(
                        CoreOptions.FLINK_DEFAULT_JVM_OPTIONS,// Flink默认JVM选项
                        CoreOptions.FLINK_JVM_OPTIONS,// Flink自定义JVM选项
                        CoreOptions.FLINK_DEFAULT_JM_JVM_OPTIONS,// Flink JobManager默认JVM选项
                        CoreOptions.FLINK_JM_JVM_OPTIONS);// Flink JobManager自定义JVM选项
        // 根据配置和Kerberos认证状态生成JVM选项字符串
        String javaOpts = Utils.generateJvmOptsString(flinkConfiguration, jvmOptions, hasKrb5);

        // Set up the container launch context for the application master
        // 设置应用管理器的容器启动上下文
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
        // 启动命令的值，这些值将用于构建最终的启动命令
        final Map<String, String> startCommandValues = new HashMap<>();
        // 设置Java执行命令的路径
        startCommandValues.put("java", "$JAVA_HOME/bin/java");
        // 根据进程规格和Flink配置生成JVM堆内存参数
        String jvmHeapMem =
                JobManagerProcessUtils.generateJvmParametersStr(processSpec, flinkConfiguration);
        // 将JVM堆内存参数添加到启动命令中
        startCommandValues.put("jvmmem", jvmHeapMem);
        // 将JVM选项添加到启动命令中
        startCommandValues.put("jvmopts", javaOpts);
        // 设置日志配置
        // 获取YARN日志配置命令
        startCommandValues.put(
                "logging", YarnLogConfigUtil.getLoggingYarnCommand(flinkConfiguration));
        // 设置要运行的类名（即YARN集群的入口点）
        startCommandValues.put("class", yarnClusterEntrypoint);
        // 设置重定向输出和错误日志到YARN的日志目录
        // 将标准输出和错误输出重定向到YARN的日志目录
        startCommandValues.put(
                "redirects",
                "1> " // 标准输出
                        + ApplicationConstants.LOG_DIR_EXPANSION_VAR// YARN日志目录变量
                        + "/jobmanager.out "// 输出到jobmanager.out文件
                        + "2> "// 标准错误输出
                        + ApplicationConstants.LOG_DIR_EXPANSION_VAR// YARN日志目录变量
                        + "/jobmanager.err");// 输出到jobmanager.err文件
        // 生成动态配置参数的字符串，这些参数可能包括额外的JVM参数、系统属性等
        String dynamicParameterListStr =
                JobManagerProcessUtils.generateDynamicConfigsStr(processSpec);
        // 将动态参数添加到启动命令值中，通常这些参数会作为应用程序的命令行参数
        startCommandValues.put("args", dynamicParameterListStr);
        // 从Flink配置中获取YARN容器启动命令的模板
        final String commandTemplate =
                flinkConfiguration.get(YARN_CONTAINER_START_COMMAND_TEMPLATE);
        // 使用命令模板和之前构建的启动命令值来生成最终的启动命令
        final String amCommand = getStartCommand(commandTemplate, startCommandValues);

        // 将最终的启动命令设置为容器启动上下文的唯一命令
        amContainer.setCommands(Collections.singletonList(amCommand));
        // 打印调试日志，显示应用管理器的启动命令
        LOG.debug("Application Master start command: " + amCommand);
        // 返回配置好的应用管理器容器启动上下文
        return amContainer;
    }

    private static YarnConfigOptions.UserJarInclusion getUserJarInclusionMode(
            org.apache.flink.configuration.Configuration config) {
        return config.get(YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR);
    }

    private static boolean isUsrLibDirIncludedInShipFiles(
            List<Path> shipFiles, YarnConfiguration yarnConfig) {
        return shipFiles.stream()
                .map(FunctionUtils.uncheckedFunction(path -> getFileStatus(path, yarnConfig)))
                .filter(FileStatus::isDirectory)
                .map(status -> status.getPath().getName().toLowerCase())
                .anyMatch(name -> name.equals(DEFAULT_FLINK_USR_LIB_DIR));
    }

    private void setClusterEntrypointInfoToConfig(final ApplicationReport report) {
        checkNotNull(report);

        final ApplicationId appId = report.getApplicationId();
        final String host = report.getHost();
        final int port = report.getRpcPort();

        LOG.info("Found Web Interface {}:{} of application '{}'.", host, port, appId);

        flinkConfiguration.set(JobManagerOptions.ADDRESS, host);
        flinkConfiguration.set(JobManagerOptions.PORT, port);

        flinkConfiguration.set(RestOptions.ADDRESS, host);
        flinkConfiguration.set(RestOptions.PORT, port);

        flinkConfiguration.set(YarnConfigOptions.APPLICATION_ID, ConverterUtils.toString(appId));

        setHAClusterIdIfNotSet(flinkConfiguration, appId);
    }

    private void setHAClusterIdIfNotSet(Configuration configuration, ApplicationId appId) {
        // set cluster-id to app id if not specified
        if (!configuration.contains(HighAvailabilityOptions.HA_CLUSTER_ID)) {
            configuration.set(
                    HighAvailabilityOptions.HA_CLUSTER_ID, ConverterUtils.toString(appId));
        }
    }

    public static void logDetachedClusterInformation(
            ApplicationId yarnApplicationId, Logger logger) {
        logger.info(
                "The Flink YARN session cluster has been started in detached mode. In order to "
                        + "stop Flink gracefully, use the following command:\n"
                        + "$ echo \"stop\" | ./bin/yarn-session.sh -id {}\n"
                        + "If this should not be possible, then you can also kill Flink via YARN's web interface or via:\n"
                        + "$ yarn application -kill {}\n"
                        + "Note that killing Flink might not clean up all job artifacts and temporary files.",
                yarnApplicationId,
                yarnApplicationId);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 设置ApplicationMaster 的执行环境
    */
    @VisibleForTesting
    Map<String, String> generateApplicationMasterEnv(
            final YarnApplicationFileUploader fileUploader,
            final String classPathStr,
            final String localFlinkJarStr,
            final String appIdStr)
            throws IOException {
        final Map<String, String> env = new HashMap<>();
        // set user specified app master environment variables
        // 设置用户指定的Application Master环境变量
        // 通过指定的前缀从Flink配置中检索键值对
        // ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX 是环境变量前缀
        env.putAll(
                ConfigurationUtils.getPrefixedKeyValuePairs(
                        ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX,
                        this.flinkConfiguration));
        // set Flink app class path
        // 设置Flink应用类路径
        env.put(ENV_FLINK_CLASSPATH, classPathStr);
        // Set FLINK_LIB_DIR to `lib` folder under working dir in container
        // 设置FLINK_LIB_DIR为容器工作目录下的'lib'文件夹
        env.put(ENV_FLINK_LIB_DIR, Path.CUR_DIR + "/" + ConfigConstants.DEFAULT_FLINK_LIB_DIR);
        // Set FLINK_OPT_DIR to `opt` folder under working dir in container
        // 设置FLINK_OPT_DIR为容器工作目录下的'opt'文件夹
        env.put(ENV_FLINK_OPT_DIR, Path.CUR_DIR + "/" + ConfigConstants.DEFAULT_FLINK_OPT_DIR);
        // set Flink on YARN internal configuration values

        // 设置Flink on YARN内部配置值
        // 设置Flink分发JAR包的位置
        env.put(YarnConfigKeys.FLINK_DIST_JAR, localFlinkJarStr);
        // 设置应用的ID
        env.put(YarnConfigKeys.ENV_APP_ID, appIdStr);
        // 设置客户端的主目录
        env.put(YarnConfigKeys.ENV_CLIENT_HOME_DIR, fileUploader.getHomeDir().toString());
        // 编码并设置需要上传的资源列表
        env.put(
                YarnConfigKeys.ENV_CLIENT_SHIP_FILES,
                encodeYarnLocalResourceDescriptorListToString(
                        fileUploader.getEnvShipResourceList()));
        // 设置Flink应用目录的URI
        env.put(
                YarnConfigKeys.FLINK_YARN_FILES,
                fileUploader.getApplicationDir().toUri().toString());
        // https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-site/src/site/markdown/YarnApplicationSecurity.md#identity-on-an-insecure-cluster-hadoop_user_name
        // 设置Hadoop的用户名（用于在不安全的集群上标识身份）
        // UserGroupInformation.getCurrentUser().getUserName() 获取当前Hadoop用户的用户名
        env.put(
                YarnConfigKeys.ENV_HADOOP_USER_NAME,
                UserGroupInformation.getCurrentUser().getUserName());
        // set classpath from YARN configuration
        //设置Yarn的ClassPath
        Utils.setupYarnClassPath(this.yarnConfiguration, env);
        // 返回设置好的环境变量Map
        return env;
    }

    private String getDisplayMemory(long memoryMB) {
        return MemorySize.ofMebiBytes(memoryMB).toHumanReadableString();
    }
}
