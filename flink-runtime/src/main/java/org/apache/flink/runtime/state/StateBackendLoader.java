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

package org.apache.flink.runtime.state;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle;
import org.apache.flink.runtime.state.delegate.DelegatingStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackendFactory;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackendFactory;
import org.apache.flink.util.DynamicCodeLoadingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** This class contains utility methods to load state backends from configurations. */
public class StateBackendLoader {

    private static final Logger LOG = LoggerFactory.getLogger(StateBackendLoader.class);

    /** Used for Loading ChangelogStateBackend. */
    private static final String CHANGELOG_STATE_BACKEND =
            "org.apache.flink.state.changelog.ChangelogStateBackend";

    /** Used for Loading TempChangelogStateBackend. */
    private static final String DEACTIVATED_CHANGELOG_STATE_BACKEND =
            "org.apache.flink.state.changelog.DeactivatedChangelogStateBackend";

    /** Used for loading RocksDBStateBackend. */
    private static final String ROCKSDB_STATE_BACKEND_FACTORY =
            "org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackendFactory";

    // ------------------------------------------------------------------------
    //  Configuration shortcut names
    // ------------------------------------------------------------------------
    /** The shortcut configuration name of the HashMap state backend. */
    public static final String HASHMAP_STATE_BACKEND_NAME = "hashmap";

    /**
     * The shortcut configuration name for the MemoryState backend that checkpoints to the
     * JobManager.
     */
    @Deprecated public static final String MEMORY_STATE_BACKEND_NAME = "jobmanager";

    /** The shortcut configuration name for the FileSystem State backend. */
    @Deprecated public static final String FS_STATE_BACKEND_NAME = "filesystem";

    /** The shortcut configuration name for the RocksDB State Backend. */
    public static final String ROCKSDB_STATE_BACKEND_NAME = "rocksdb";

    // ------------------------------------------------------------------------
    //  Loading the state backend from a configuration
    // ------------------------------------------------------------------------

    /**
     * Loads the unwrapped state backend from the configuration, from the parameter 'state.backend',
     * as defined in {@link StateBackendOptions#STATE_BACKEND}.
     *
     * <p>The state backends can be specified either via their shortcut name, or via the class name
     * of a {@link StateBackendFactory}. If a StateBackendFactory class name is specified, the
     * factory is instantiated (via its zero-argument constructor) and its {@link
     * StateBackendFactory#createFromConfig(ReadableConfig, ClassLoader)} method is called.
     *
     * <p>Recognized shortcut names are '{@value StateBackendLoader#HASHMAP_STATE_BACKEND_NAME}',
     * '{@value StateBackendLoader#ROCKSDB_STATE_BACKEND_NAME}' '{@value
     * StateBackendLoader#MEMORY_STATE_BACKEND_NAME}' (Deprecated), and '{@value
     * StateBackendLoader#FS_STATE_BACKEND_NAME}' (Deprecated).
     *
     * @param config The configuration to load the state backend from
     * @param classLoader The class loader that should be used to load the state backend
     * @param logger Optionally, a logger to log actions to (may be null)
     * @return The instantiated state backend.
     * @throws DynamicCodeLoadingException Thrown if a state backend factory is configured and the
     *     factory class was not found or the factory could not be instantiated
     * @throws IllegalConfigurationException May be thrown by the StateBackendFactory when creating
     *     / configuring the state backend in the factory
     * @throws IOException May be thrown by the StateBackendFactory when instantiating the state
     *     backend
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 从配置中加载 StateBackend。
     *
     * @param config Flink 配置对象，其中包含状态后端的配置信息
     * @param classLoader 用于加载用户代码的类加载器
     * @param logger 可选的日志记录器，用于记录日志信息（如果为null，则不记录日志）
     * @return 加载的 StateBackend 实例
     * @throws IllegalConfigurationException 如果在加载过程中遇到配置错误
     * @throws DynamicCodeLoadingException 如果在动态加载代码时发生错误
     * @throws IOException 如果在读取配置文件或进行I/O操作时发生错误
    */
    @Nonnull
    public static StateBackend loadStateBackendFromConfig(
            ReadableConfig config, ClassLoader classLoader, @Nullable Logger logger)
            throws IllegalConfigurationException, DynamicCodeLoadingException, IOException {
        // 检查配置对象是否为空
        checkNotNull(config, "config");
        checkNotNull(classLoader, "classLoader");
        // 基于类型从配置中获取状态后端的名称
        final String backendName = config.get(StateBackendOptions.STATE_BACKEND);

        // by default the factory class is the backend name
        // 默认情况下，工厂类的名称与状态后端的名称相同
        String factoryClassName = backendName;

        // 根据状态后端的名称选择具体的实现
        switch (backendName.toLowerCase()) {
            case MEMORY_STATE_BACKEND_NAME:
                // 创建并配置MemoryStateBackend实例
                MemoryStateBackend backend =
                        new MemoryStateBackendFactory().createFromConfig(config, classLoader);
                //打印日志
                if (logger != null) {
                    logger.warn(
                            "MemoryStateBackend has been deprecated. Please use 'hashmap' state "
                                    + "backend instead with JobManagerCheckpointStorage for equivalent "
                                    + "functionality");

                    logger.info("State backend is set to job manager {}", backend);
                }
                // 返回创建的MemoryStateBackend实例
                return backend;
                //如果类型是HASH、FSData
            case FS_STATE_BACKEND_NAME:
                if (logger != null) {
                    logger.warn(
                            "{} state backend has been deprecated. Please use 'hashmap' state "
                                    + "backend instead.",
                            backendName.toLowerCase());
                }
                // fall through and use the HashMapStateBackend instead which
                // utilizes the same HeapKeyedStateBackend runtime implementation.
                //创建HashMapStateBackend
            case HASHMAP_STATE_BACKEND_NAME:
                HashMapStateBackend hashMapStateBackend =
                        new HashMapStateBackendFactory().createFromConfig(config, classLoader);
                if (logger != null) {
                    logger.info("State backend is set to heap memory {}", hashMapStateBackend);
                }
                //返回
                return hashMapStateBackend;
            //如果是RockDb 则设置org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackendFactory
            case ROCKSDB_STATE_BACKEND_NAME:
                factoryClassName = ROCKSDB_STATE_BACKEND_FACTORY;

                // fall through to the 'default' case that uses reflection to load the backend
                // that way we can keep RocksDB in a separate module

            default:
                if (logger != null) {
                    logger.info("Loading state backend via factory {}", factoryClassName);
                }
                //反射创建org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackendFactory
                StateBackendFactory<?> factory;
                try {
                    @SuppressWarnings("rawtypes")
                    Class<? extends StateBackendFactory> clazz =
                            Class.forName(factoryClassName, false, classLoader)
                                    .asSubclass(StateBackendFactory.class);

                    factory = clazz.newInstance();
                } catch (ClassNotFoundException e) {
                    throw new DynamicCodeLoadingException(
                            "Cannot find configured state backend factory class: " + backendName,
                            e);
                } catch (ClassCastException | InstantiationException | IllegalAccessException e) {
                    throw new DynamicCodeLoadingException(
                            "The class configured under '"
                                    + StateBackendOptions.STATE_BACKEND.key()
                                    + "' is not a valid state backend factory ("
                                    + backendName
                                    + ')',
                            e);
                }
                //创建EmbeddedRocksDBStateBackend并返回
                return factory.createFromConfig(config, classLoader);
        }
    }

    /**
     * Checks if an application-defined state backend is given, and if not, loads the state backend
     * from the configuration, from the parameter 'state.backend', as defined in {@link
     * CheckpointingOptions#STATE_BACKEND}. If no state backend is configured, this instantiates the
     * default state backend (the {@link HashMapStateBackend}).
     *
     * <p>If an application-defined state backend is found, and the state backend is a {@link
     * ConfigurableStateBackend}, this methods calls {@link
     * ConfigurableStateBackend#configure(ReadableConfig, ClassLoader)} on the state backend.
     *
     * <p>Refer to {@link #loadStateBackendFromConfig(ReadableConfig, ClassLoader, Logger)} for
     * details on how the state backend is loaded from the configuration.
     *
     * @param jobConfig The job configuration to load the state backend from
     * @param clusterConfig The cluster configuration to load the state backend from
     * @param classLoader The class loader that should be used to load the state backend
     * @param logger Optionally, a logger to log actions to (may be null)
     * @return The instantiated state backend.
     * @throws DynamicCodeLoadingException Thrown if a state backend factory is configured and the
     *     factory class was not found or the factory could not be instantiated
     * @throws IllegalConfigurationException May be thrown by the StateBackendFactory when creating
     *     / configuring the state backend in the factory
     * @throws IOException May be thrown by the StateBackendFactory when instantiating the state
     *     backend
     */
    private static StateBackend loadFromApplicationOrConfigOrDefaultInternal(
            @Nullable StateBackend fromApplication,
            Configuration jobConfig,
            Configuration clusterConfig,
            ClassLoader classLoader,
            @Nullable Logger logger)
            throws IllegalConfigurationException, DynamicCodeLoadingException, IOException {

        checkNotNull(jobConfig, "jobConfig");
        checkNotNull(clusterConfig, "clusterConfig");
        checkNotNull(classLoader, "classLoader");

        // Job level config can override the cluster level config.
        Configuration mergedConfig = new Configuration(clusterConfig);
        mergedConfig.addAll(jobConfig);

        final StateBackend backend;

        // In the FLINK-2.0, the state backend from application will be not supported anymore.
        // (1) the application defined state backend has precedence
        if (fromApplication != null) {
            // see if this is supposed to pick up additional configuration parameters
            if (fromApplication instanceof ConfigurableStateBackend) {
                // needs to pick up configuration
                if (logger != null) {
                    logger.info(
                            "Using job/cluster config to configure application-defined state backend: {}",
                            fromApplication);
                }

                backend =
                        ((ConfigurableStateBackend) fromApplication)
                                // Use cluster config for backwards compatibility.
                                .configure(clusterConfig, classLoader);
            } else {
                // keep as is!
                backend = fromApplication;
            }

            if (logger != null) {
                logger.info("Using application-defined state backend: {}", backend);
            }
        } else {
            // (2) check if the config defines a state backend
            backend = loadStateBackendFromConfig(mergedConfig, classLoader, logger);
        }

        return backend;
    }

    /**
     * This is the state backend loader that loads a {@link DelegatingStateBackend} wrapping the
     * state backend loaded from {@link
     * StateBackendLoader#loadFromApplicationOrConfigOrDefaultInternal} when delegation is enabled.
     * If delegation is not enabled, the underlying wrapped state backend is returned instead.
     *
     * @param fromApplication StateBackend defined from application
     * @param jobConfig The job level configuration to load the state backend from
     * @param clusterConfig The cluster level configuration to load the state backend from
     * @param classLoader The class loader that should be used to load the state backend
     * @param logger Optionally, a logger to log actions to (may be null)
     * @return The instantiated state backend.
     * @throws DynamicCodeLoadingException Thrown if a state backend (factory) is configured and the
     *     (factory) class was not found or could not be instantiated
     * @throws IllegalConfigurationException May be thrown by the StateBackendFactory when creating
     *     / configuring the state backend in the factory
     * @throws IOException May be thrown by the StateBackendFactory when instantiating the state
     *     backend
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 从应用程序配置、作业配置或默认配置中加载 StateBackend。
     * @param fromApplication 从应用程序配置中获取的状态后端，如果为null，则尝试从其他配置源加载
     * @param jobConfig Flink 作业的配置对象
     * @param clusterConfig Flink 集群的配置对象
     * @param classLoader 用于加载用户代码的类加载器
     * @param logger 可选的日志记录器，用于记录日志信息（如果为null，则不记录日志）
     * @return 加载的 StateBackend 实例
     * @throws IllegalConfigurationException 如果在加载过程中遇到配置错误
     * @throws DynamicCodeLoadingException 如果在动态加载代码时发生错误
     * @throws IOException 如果在读取配置文件或进行I/O操作时发生错误
     */
    public static StateBackend fromApplicationOrConfigOrDefault(
            @Nullable StateBackend fromApplication,
            Configuration jobConfig,
            Configuration clusterConfig,
            ClassLoader classLoader,
            @Nullable Logger logger)
            throws IllegalConfigurationException, DynamicCodeLoadingException, IOException {
        // 内部方法，用于从应用程序配置、作业配置或默认配置中加载 StateBackend
        StateBackend rootBackend =
                loadFromApplicationOrConfigOrDefaultInternal(
                        fromApplication, jobConfig, clusterConfig, classLoader, logger);
        // 检查是否启用状态变更日志
        boolean enableChangeLog =
                jobConfig
                        .getOptional(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)
                        .orElse(clusterConfig.get(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG));

        StateBackend backend;
        // 如果启用了状态变更日志
        if (enableChangeLog) {
            // 包装原始 StateBackend，以便添加状态变更日志功能
            backend = wrapStateBackend(rootBackend, classLoader, CHANGELOG_STATE_BACKEND);
            LOG.info(
                    "State backend loader loads {} to delegate {}",
                    backend.getClass().getSimpleName(),
                    rootBackend.getClass().getSimpleName());
        } else {
            backend = rootBackend;
            LOG.info(
                    "State backend loader loads the state backend as {}",
                    backend.getClass().getSimpleName());
        }
        // 返回加载的 StateBackend
        return backend;
    }

    /**
     * Checks whether state backend uses managed memory, without having to deserialize or load the
     * state backend.
     *
     * @param config configuration to load the state backend from.
     * @param stateBackendFromApplicationUsesManagedMemory Whether the application-defined backend
     *     uses Flink's managed memory. Empty if application has not defined a backend.
     * @param classLoader User code classloader.
     * @return Whether the state backend uses managed memory.
     */
    public static boolean stateBackendFromApplicationOrConfigOrDefaultUseManagedMemory(
            Configuration config,
            Optional<Boolean> stateBackendFromApplicationUsesManagedMemory,
            ClassLoader classLoader) {

        checkNotNull(config, "config");

        // (1) the application defined state backend has precedence
        if (stateBackendFromApplicationUsesManagedMemory.isPresent()) {
            return stateBackendFromApplicationUsesManagedMemory.get();
        }

        // (2) check if the config defines a state backend
        try {
            final StateBackend fromConfig = loadStateBackendFromConfig(config, classLoader, LOG);
            return fromConfig.useManagedMemory();
        } catch (IllegalConfigurationException | DynamicCodeLoadingException | IOException e) {
            LOG.warn(
                    "Cannot decide whether state backend uses managed memory. Will reserve managed memory by default.",
                    e);
            return true;
        }
    }

    /**
     * Load state backend which may wrap the original state backend for recovery.
     *
     * @param originalStateBackend StateBackend loaded from application or config.
     * @param classLoader User code classloader.
     * @param keyedStateHandles The state handles for restore.
     * @return Wrapped state backend for recovery.
     * @throws DynamicCodeLoadingException Thrown if keyed state handles of wrapped state backend
     *     are found and the class was not found or could not be instantiated.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 加载状态后端，它可能会包装原始状态后端以进行恢复。
     *
     * @param originalStateBackend 从应用程序或配置中加载的StateBackend。
     * @param classLoader 用户代码的类加载器。
     * @param keyedStateHandles 用于恢复的状态句柄集合。
     * @return 包装后的状态后端，用于恢复。
    */
    public static StateBackend loadStateBackendFromKeyedStateHandles(
            StateBackend originalStateBackend,
            ClassLoader classLoader,
            Collection<KeyedStateHandle> keyedStateHandles)
            throws DynamicCodeLoadingException {
        // Wrapping ChangelogStateBackend or ChangelogStateBackendHandle is not supported currently.
        // 当前不支持包装ChangelogStateBackend或ChangelogStateBackendHandle。
        // 如果原始状态后端不是ChangelogStateBackend类型，
        // 但给定的状态句柄中存在ChangelogStateBackendHandle的实例，
        // 则将其包装为禁用日志的状态后端。
        if (!isChangelogStateBackend(originalStateBackend)
                && keyedStateHandles.stream()
                        .anyMatch(
                                stateHandle ->
                                        stateHandle instanceof ChangelogStateBackendHandle)) {
            return wrapStateBackend(
                    originalStateBackend, classLoader, DEACTIVATED_CHANGELOG_STATE_BACKEND);
        }
        // 如果不满足上述条件，则返回原始状态后端。
        return originalStateBackend;
    }

    public static boolean isChangelogStateBackend(StateBackend backend) {
        return CHANGELOG_STATE_BACKEND.equals(backend.getClass().getName());
    }

    private static StateBackend wrapStateBackend(
            StateBackend backend, ClassLoader classLoader, String className)
            throws DynamicCodeLoadingException {

        // ChangelogStateBackend resides in a separate module, load it using reflection
        try {
            Constructor<? extends DelegatingStateBackend> constructor =
                    Class.forName(className, false, classLoader)
                            .asSubclass(DelegatingStateBackend.class)
                            .getDeclaredConstructor(StateBackend.class);
            constructor.setAccessible(true);
            return constructor.newInstance(backend);
        } catch (ClassNotFoundException e) {
            throw new DynamicCodeLoadingException(
                    "Cannot find DelegateStateBackend class: " + className, e);
        } catch (InstantiationException
                | IllegalAccessException
                | NoSuchMethodException
                | InvocationTargetException e) {
            throw new DynamicCodeLoadingException("Fail to initialize: " + className, e);
        }
    }

    // ------------------------------------------------------------------------

    /** This class is not meant to be instantiated. */
    private StateBackendLoader() {}
}
