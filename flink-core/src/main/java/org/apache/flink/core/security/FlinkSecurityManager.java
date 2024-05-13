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

package org.apache.flink.core.security;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Permission;

/**
 * {@code FlinkSecurityManager} to control certain behaviors that can be captured by Java system
 * security manager. It can be used to control unexpected user behaviors that potentially impact
 * cluster availability, for example, it can warn or prevent user code from terminating JVM by
 * System.exit or halt by logging or throwing an exception. This does not necessarily prevent
 * malicious users who try to tweak security manager on their own, but more for being dependable
 * against user mistakes by gracefully handling them informing users rather than causing silent
 * unavailability.
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 用于控制一些可以通过 Java 系统安全管理器捕获的行为。它可以用来控制可能影响集群可用性的意外用户行为，
 * 例如，它可以警告或阻止用户代码通过 System.exit 终止 JVM 或通过日志记录或抛出异常来阻止 JVM 停止。
 * 这并不一定能阻止那些试图自己调整安全管理器的恶意用户，但更多的是为了可靠地处理用户错误，
 * 通过优雅地处理这些错误并通知用户，而不是导致无声的不可用状态。
*/
public class FlinkSecurityManager extends SecurityManager {

    static final Logger LOG = LoggerFactory.getLogger(FlinkSecurityManager.class);

    /**
     * Security manager reference lastly set to system's security manager by public API. As system
     * security manager can be reset with another but still chain-called into this manager properly,
     * this reference may not be referenced by System.getSecurityManager, but we still need to
     * control runtime check behaviors such as monitoring exit from user code.
     */
    private static FlinkSecurityManager flinkSecurityManager;

    private final SecurityManager originalSecurityManager;
    private final ThreadLocal<Boolean> monitorUserSystemExit = new InheritableThreadLocal<>();
    private final ClusterOptions.UserSystemExitMode userSystemExitMode;

    private final boolean haltOnSystemExit;

    @VisibleForTesting
    FlinkSecurityManager(
            ClusterOptions.UserSystemExitMode userSystemExitMode, boolean haltOnSystemExit) {
        this(userSystemExitMode, haltOnSystemExit, System.getSecurityManager());
    }

    @VisibleForTesting
    FlinkSecurityManager(
            ClusterOptions.UserSystemExitMode userSystemExitMode,
            boolean haltOnSystemExit,
            SecurityManager originalSecurityManager) {
        this.userSystemExitMode = Preconditions.checkNotNull(userSystemExitMode);
        this.haltOnSystemExit = haltOnSystemExit;
        this.originalSecurityManager = originalSecurityManager;
    }

    /**
     * Instantiate FlinkUserSecurityManager from configuration. Return null if no security manager
     * check is needed, so that a caller can skip setting security manager avoiding runtime check
     * cost, if there is no security check set up already. Use {@link #setFromConfiguration} helper,
     * which handles disabled case.
     *
     * @param configuration to instantiate the security manager from
     * @return FlinkUserSecurityManager instantiated based on configuration. Return null if
     *     disabled.
     */
    @VisibleForTesting
    static FlinkSecurityManager fromConfiguration(Configuration configuration) {
        final ClusterOptions.UserSystemExitMode userSystemExitMode =
                configuration.get(ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT);

        boolean haltOnSystemExit = configuration.get(ClusterOptions.HALT_ON_FATAL_ERROR);

        // If no check is needed, return null so that caller can avoid setting security manager not
        // to incur any runtime cost.
        if (userSystemExitMode == ClusterOptions.UserSystemExitMode.DISABLED && !haltOnSystemExit) {
            return null;
        }
        LOG.info(
                "FlinkSecurityManager is created with {} user system exit mode and {} exit",
                userSystemExitMode,
                haltOnSystemExit ? "forceful" : "graceful");
        // Add more configuration parameters that need user security manager (currently only for
        // system exit).
        return new FlinkSecurityManager(userSystemExitMode, haltOnSystemExit);
    }

    public static void setFromConfiguration(Configuration configuration) {
        final FlinkSecurityManager flinkSecurityManager =
                FlinkSecurityManager.fromConfiguration(configuration);
        if (flinkSecurityManager != null) {
            try {
                System.setSecurityManager(flinkSecurityManager);
            } catch (Exception e) {
                throw new IllegalConfigurationException(
                        String.format(
                                "Could not register security manager due to no permission to "
                                        + "set a SecurityManager. Either update your existing "
                                        + "SecurityManager to allow the permission or do not use "
                                        + "security manager features (e.g., '%s: %s', '%s: %s')",
                                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT.key(),
                                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT.defaultValue(),
                                ClusterOptions.HALT_ON_FATAL_ERROR.key(),
                                ClusterOptions.HALT_ON_FATAL_ERROR.defaultValue()),
                        e);
            }
        }
        FlinkSecurityManager.flinkSecurityManager = flinkSecurityManager;
    }

    public static void monitorUserSystemExitForCurrentThread() {
        if (flinkSecurityManager != null) {
            flinkSecurityManager.monitorUserSystemExit();
        }
    }

    public static void unmonitorUserSystemExitForCurrentThread() {
        if (flinkSecurityManager != null) {
            flinkSecurityManager.unmonitorUserSystemExit();
        }
    }

    @Override
    public void checkPermission(Permission perm) {
        if (originalSecurityManager != null) {
            originalSecurityManager.checkPermission(perm);
        }
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
        if (originalSecurityManager != null) {
            originalSecurityManager.checkPermission(perm, context);
        }
    }

    @Override
    public void checkExit(int status) {
        // 检查是否监控用户系统退出
        if (userSystemExitMonitored()) {
            // 根据用户系统退出模式进行不同的处理
            switch (userSystemExitMode) {
                // 禁用模式，不执行任何操作
                case DISABLED:
                    break;
                case LOG:
                    // 日志模式，记录异常追踪日志以帮助用户调试退出来源
                    // 向日志中添加退出JVM的警告信息，并附带异常信息
                    // Add exception trace log to help users to debug where exit came from.
                    LOG.warn(
                            "Exiting JVM with status {} is monitored: The system will exit due to this call.",
                            status,
                            new UserSystemExitException());
                    break;
                case THROW:
                    // 抛出异常模式，直接抛出UserSystemExitException异常
                    throw new UserSystemExitException();
                default:
                    // 不应该发生的情况，如果上面已经详尽处理了所有模式
                    // 在已经处于退出路径时记录警告
                    // Must not happen if exhaustively handling all modes above. Logging as being
                    // already at exit path.
                    LOG.warn("No valid check exit mode configured: {}", userSystemExitMode);
            }
        }
        // As this security manager is current at outer most of the chain and it has exit guard
        // option, invoke inner security manager here after passing guard checking above, if any.
        // 由于此安全管理器位于链的最外层，并且具有退出守卫选项，
        // 如果在上面的守卫检查之后存在其他安全管理器，则在此处调用它
        if (originalSecurityManager != null) {
            originalSecurityManager.checkExit(status);
        }
        // At this point, exit is determined. Halt if defined, otherwise check ended, JVM will call
        // System.exit
        // 此时，退出已经确定。如果定义了haltOnSystemExit，则立即停止JVM，
        // 否则检查结束，JVM将调用System.exit()退出
        if (haltOnSystemExit) {
            Runtime.getRuntime().halt(status);
        }
    }

    @VisibleForTesting
    void monitorUserSystemExit() {
        monitorUserSystemExit.set(true);
    }

    @VisibleForTesting
    void unmonitorUserSystemExit() {
        monitorUserSystemExit.set(false);
    }

    @VisibleForTesting
    boolean userSystemExitMonitored() {
        return Boolean.TRUE.equals(monitorUserSystemExit.get());
    }

    /**
     * Use this method to circumvent the configured {@link FlinkSecurityManager} behavior, ensuring
     * that the current JVM process will always stop via System.exit() or
     * Runtime.getRuntime().halt().
     */
    public static void forceProcessExit(int exitCode) {
        // Unset ourselves to allow exiting in any case.
        System.setSecurityManager(null);
        if (flinkSecurityManager != null && flinkSecurityManager.haltOnSystemExit) {
            Runtime.getRuntime().halt(exitCode);
        } else {
            System.exit(exitCode);
        }
    }
}
