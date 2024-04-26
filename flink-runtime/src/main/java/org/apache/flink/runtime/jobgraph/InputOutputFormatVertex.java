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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.FinalizeOnMaster.FinalizationContext;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.operators.util.TaskConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A task vertex that runs an initialization and a finalization on the master. If necessary, it
 * tries to deserialize input and output formats, and initialize and finalize them on master.
 */
public class InputOutputFormatVertex extends JobVertex {

    private static final long serialVersionUID = 1L;

    private final Map<OperatorID, String> formatDescriptions = new HashMap<>();

    public InputOutputFormatVertex(String name) {
        super(name);
    }

    public InputOutputFormatVertex(
            String name, JobVertexID id, List<OperatorIDPair> operatorIDPairs) {

        super(name, id, operatorIDPairs);
    }

    @Override
    public void initializeOnMaster(InitializeOnMasterContext context) throws Exception {
        /** 从传入的InitializeOnMasterContext对象中获取类加载器。 */
        ClassLoader loader = context.getClassLoader();
        /**
         * loader初始化一个InputOutputFormatContainer对象。
         * InputOutputFormatContainer输入和输出格式的信息和配置容器。
         */
        final InputOutputFormatContainer formatContainer = initInputOutputformatContainer(loader);
        /** 获取当前线程的上下文类加载器 */
        final ClassLoader original = Thread.currentThread().getContextClassLoader();
        try {
            // set user classloader before calling user code
            /** 在调用用户代码之前设置用户类加载器 */
            Thread.currentThread().setContextClassLoader(loader);

            // configure the input format and setup input splits
            /**
             * 从formatContainer中获取输入格式的映射，其中键是OperatorID，值是UserCodeWrapper对象，
             * 包装了用户提供的输入格式实例。
             */
            Map<OperatorID, UserCodeWrapper<? extends InputFormat<?, ?>>> inputFormats =
                    formatContainer.getInputFormats();
            /**
             * 检查输入格式的数量。如果大于1，则抛出异常，因为在这个作业顶点不支持多个输入格式。
             */
            if (inputFormats.size() > 1) {
                throw new UnsupportedOperationException(
                        "Multiple input formats are not supported in a job vertex.");
            }
            /** 遍历输入格式的映射 */
            for (Map.Entry<OperatorID, UserCodeWrapper<? extends InputFormat<?, ?>>> entry :
                    inputFormats.entrySet()) {
                final InputFormat<?, ?> inputFormat;

                try {
                    /** 获取UserCodeWrapper中的用户代码对象 */
                    inputFormat = entry.getValue().getUserCodeObject();
                    /** 使用formatContainer中的参数配置输入格式 */
                    inputFormat.configure(formatContainer.getParameters(entry.getKey()));
                } catch (Throwable t) {
                    throw new Exception(
                            "Configuring the input format ("
                                    + getFormatDescription(entry.getKey())
                                    + ") failed: "
                                    + t.getMessage(),
                            t);
                }
                /** 调用setInputSplitSource方法，可能是为了设置设置输入拆分 */
                setInputSplitSource(inputFormat);
            }

            // configure output formats and invoke initializeGlobal()
            /** formatContainer中获取输出格式的映射，其中键是OperatorID，值是UserCodeWrapper对象，
             * 用户提供的输出格式实例。
             */
            Map<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> outputFormats =
                    formatContainer.getOutputFormats();
            for (Map.Entry<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> entry :
                    outputFormats.entrySet()) {
                final OutputFormat<?> outputFormat;

                try {
                    /** 从UserCodeWrapper中获取输出格式实例。 */
                    outputFormat = entry.getValue().getUserCodeObject();
                    /** 使用formatContainer中的参数配置输出格式。*/
                    outputFormat.configure(formatContainer.getParameters(entry.getKey()));
                } catch (Throwable t) {
                    throw new Exception(
                            "Configuring the output format ("
                                    + getFormatDescription(entry.getKey())
                                    + ") failed: "
                                    + t.getMessage(),
                            t);
                }
                /**
                 * 输出格式实现了InitializeOnMaster接口，那么会调用其initializeGlobal方法。
                 * 这个方法可能用于在master节点上进行一些全局的初始化工作，比如设置并行度等。
                 */
                if (outputFormat instanceof InitializeOnMaster) {
                    int executionParallelism = context.getExecutionParallelism();
                    /** 在分布式程序执行开始之前，会在master（JobManager）上调用该方法。 */
                    ((InitializeOnMaster) outputFormat).initializeGlobal(executionParallelism);
                }
            }
        } finally {
            // restore original classloader
            /** 恢复原始类加载器 */
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Override
    public void finalizeOnMaster(FinalizeOnMasterContext context) throws Exception {
        final ClassLoader loader = context.getClassLoader();
        final InputOutputFormatContainer formatContainer = initInputOutputformatContainer(loader);

        final ClassLoader original = Thread.currentThread().getContextClassLoader();
        try {
            // set user classloader before calling user code
            Thread.currentThread().setContextClassLoader(loader);

            // configure output formats and invoke finalizeGlobal()
            Map<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> outputFormats =
                    formatContainer.getOutputFormats();
            for (Map.Entry<OperatorID, UserCodeWrapper<? extends OutputFormat<?>>> entry :
                    outputFormats.entrySet()) {
                final OutputFormat<?> outputFormat;

                try {
                    outputFormat = entry.getValue().getUserCodeObject();
                    outputFormat.configure(formatContainer.getParameters(entry.getKey()));
                } catch (Throwable t) {
                    throw new Exception(
                            "Configuring the output format ("
                                    + getFormatDescription(entry.getKey())
                                    + ") failed: "
                                    + t.getMessage(),
                            t);
                }

                if (outputFormat instanceof FinalizeOnMaster) {
                    int executionParallelism = context.getExecutionParallelism();
                    ((FinalizeOnMaster) outputFormat)
                            .finalizeGlobal(
                                    new FinalizationContext() {
                                        @Override
                                        public int getParallelism() {
                                            return executionParallelism;
                                        }

                                        @Override
                                        public int getFinishedAttempt(int subtaskIndex) {
                                            return context.getFinishedAttempt(subtaskIndex);
                                        }
                                    });
                }
            }
        } finally {
            // restore original classloader
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    public String getFormatDescription(OperatorID operatorID) {
        return formatDescriptions.get(operatorID);
    }

    public void setFormatDescription(OperatorID operatorID, String formatDescription) {
        formatDescriptions.put(checkNotNull(operatorID), formatDescription);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 根据传入的类加载器创建 InputOutputFormatContainer
    */
    private InputOutputFormatContainer initInputOutputformatContainer(ClassLoader classLoader)
            throws Exception {
        try {
            return new InputOutputFormatContainer(new TaskConfig(getConfiguration()), classLoader);
        } catch (Throwable t) {
            throw new Exception(
                    "Loading the input/output formats failed: "
                            + String.join(",", formatDescriptions.values()),
                    t);
        }
    }
}
