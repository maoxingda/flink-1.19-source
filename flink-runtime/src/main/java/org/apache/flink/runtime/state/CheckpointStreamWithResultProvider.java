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

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FileBasedStateOutputStream;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * Interface that provides access to a CheckpointStateOutputStream and a method to provide the
 * {@link SnapshotResult}. This abstracts from different ways that a result is obtained from
 * checkpoint output streams.
 */
public interface CheckpointStreamWithResultProvider extends Closeable {

    Logger LOG = LoggerFactory.getLogger(CheckpointStreamWithResultProvider.class);

    /** Closes the stream ans returns a snapshot result with the stream handle(s). */
    @Nonnull
    SnapshotResult<StreamStateHandle> closeAndFinalizeCheckpointStreamResult() throws IOException;

    /** Returns the encapsulated output stream. */
    @Nonnull
    CheckpointStateOutputStream getCheckpointOutputStream();

    @Override
    default void close() throws IOException {
        getCheckpointOutputStream().close();
    }

    /**
     * Implementation of {@link CheckpointStreamWithResultProvider} that only creates the
     * primary/remote/jm-owned state.
     */
    class PrimaryStreamOnly implements CheckpointStreamWithResultProvider {

        @Nonnull private final CheckpointStateOutputStream outputStream;

        public PrimaryStreamOnly(@Nonnull CheckpointStateOutputStream outputStream) {
            this.outputStream = outputStream;
        }

        @Nonnull
        @Override
        public SnapshotResult<StreamStateHandle> closeAndFinalizeCheckpointStreamResult()
                throws IOException {
            return SnapshotResult.of(outputStream.closeAndGetHandle());
        }

        @Nonnull
        @Override
        public CheckpointStateOutputStream getCheckpointOutputStream() {
            return outputStream;
        }
    }

    /**
     * Implementation of {@link CheckpointStreamWithResultProvider} that creates both, the
     * primary/remote/jm-owned state and the secondary/local/tm-owned state.
     */
    class PrimaryAndSecondaryStream implements CheckpointStreamWithResultProvider {

        private static final Logger LOG = LoggerFactory.getLogger(PrimaryAndSecondaryStream.class);

        @Nonnull private final DuplicatingCheckpointOutputStream outputStream;

        public PrimaryAndSecondaryStream(
                @Nonnull CheckpointStateOutputStream primaryOut,
                CheckpointStateOutputStream secondaryOut)
                throws IOException {
            this(new DuplicatingCheckpointOutputStream(primaryOut, secondaryOut));
        }

        PrimaryAndSecondaryStream(@Nonnull DuplicatingCheckpointOutputStream outputStream) {
            this.outputStream = outputStream;
        }

        @Nonnull
        @Override
        public SnapshotResult<StreamStateHandle> closeAndFinalizeCheckpointStreamResult()
                throws IOException {

            final StreamStateHandle primaryStreamStateHandle;

            try {
                primaryStreamStateHandle = outputStream.closeAndGetPrimaryHandle();
            } catch (IOException primaryEx) {
                try {
                    outputStream.close();
                } catch (IOException closeEx) {
                    primaryEx = ExceptionUtils.firstOrSuppressed(closeEx, primaryEx);
                }
                throw primaryEx;
            }

            StreamStateHandle secondaryStreamStateHandle = null;

            try {
                secondaryStreamStateHandle = outputStream.closeAndGetSecondaryHandle();
            } catch (IOException secondaryEx) {
                LOG.warn("Exception from secondary/local checkpoint stream.", secondaryEx);
            }

            if (primaryStreamStateHandle != null) {
                if (secondaryStreamStateHandle != null) {
                    return SnapshotResult.withLocalState(
                            primaryStreamStateHandle, secondaryStreamStateHandle);
                } else {
                    return SnapshotResult.of(primaryStreamStateHandle);
                }
            } else {
                return SnapshotResult.empty();
            }
        }

        @Nonnull
        @Override
        public DuplicatingCheckpointOutputStream getCheckpointOutputStream() {
            return outputStream;
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建CheckpointStateOutputStream
     * @param checkpointedStateScope 检查点状态的作用域，不能为空
     * @param primaryStreamFactory 用于创建主流的检查点流工厂，不能为空
    */
    @Nonnull
    static CheckpointStreamWithResultProvider createSimpleStream(
            @Nonnull CheckpointedStateScope checkpointedStateScope,
            @Nonnull CheckpointStreamFactory primaryStreamFactory)
            throws IOException {
        //使用主流工厂创建主流的输出流
        CheckpointStateOutputStream primaryOut =
                primaryStreamFactory.createCheckpointStateOutputStream(checkpointedStateScope);
        // 返回一个仅包含主流的CheckpointStreamWithResultProvider
        return new CheckpointStreamWithResultProvider.PrimaryStreamOnly(primaryOut);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建一个支持本地恢复的复制流，用于在检查点时将状态数据写入主流和备份流
     * @param checkpointId 检查点的ID，必须是非负数
     * @param checkpointedStateScope 检查点状态的作用域，不能为空
     * @param primaryStreamFactory 用于创建主流的检查点流工厂，不能为空
     * @param secondaryStreamDirProvider 提供本地恢复备份流目录的提供者，不能为空
    */
    @Nonnull
    static CheckpointStreamWithResultProvider createDuplicatingStream(
            @Nonnegative long checkpointId,
            @Nonnull CheckpointedStateScope checkpointedStateScope,
            @Nonnull CheckpointStreamFactory primaryStreamFactory,
            @Nonnull LocalRecoveryDirectoryProvider secondaryStreamDirProvider)
            throws IOException {
        // 使用主流工厂创建主流的输出流
        CheckpointStateOutputStream primaryOut =
                primaryStreamFactory.createCheckpointStateOutputStream(checkpointedStateScope);

        try {
            // 创建一个用于备份流的文件，文件路径基于检查点ID和随机生成的UUID
            File outFile =
                    new File(
                            secondaryStreamDirProvider.subtaskSpecificCheckpointDirectory(
                                    checkpointId),
                            String.valueOf(UUID.randomUUID()));
            // 将文件路径转换为URI路径
            Path outPath = new Path(outFile.toURI());
            // 使用文件路径创建CheckpointState输出流
            CheckpointStateOutputStream secondaryOut =
                    new FileBasedStateOutputStream(outPath.getFileSystem(), outPath);
            // 返回一个包含主流和备份流的CheckpointStreamWithResultProvider
            return new CheckpointStreamWithResultProvider.PrimaryAndSecondaryStream(
                    primaryOut, secondaryOut);
            // 如果在创建备份流时发生异常
        } catch (IOException secondaryEx) {
            LOG.warn(
                    "Exception when opening secondary/local checkpoint output stream. "
                            + "Continue only with the primary stream.",
                    secondaryEx);
        }
        // 如果备份流创建失败，返回仅包含主流的CheckpointStreamWithResultProvider
        return new CheckpointStreamWithResultProvider.PrimaryStreamOnly(primaryOut);
    }

    /**
     * Factory method for a {@link KeyedStateHandle} to be used in {@link
     * #toKeyedStateHandleSnapshotResult(SnapshotResult, KeyGroupRangeOffsets,
     * KeyedStateHandleFactory)}.
     */
    @FunctionalInterface
    interface KeyedStateHandleFactory {
        KeyedStateHandle create(
                KeyGroupRangeOffsets keyGroupRangeOffsets, StreamStateHandle streamStateHandle);
    }

    /**
     * Helper method that takes a {@link SnapshotResult<StreamStateHandle>} and a {@link
     * KeyGroupRangeOffsets} and creates a {@link SnapshotResult<KeyedStateHandle>} by combining the
     * key groups offsets with all the present stream state handles.
     */
    @Nonnull
    static SnapshotResult<KeyedStateHandle> toKeyedStateHandleSnapshotResult(
            @Nonnull SnapshotResult<StreamStateHandle> snapshotResult,
            @Nonnull KeyGroupRangeOffsets keyGroupRangeOffsets,
            @Nonnull KeyedStateHandleFactory stateHandleFactory) {

        StreamStateHandle jobManagerOwnedSnapshot = snapshotResult.getJobManagerOwnedSnapshot();

        if (jobManagerOwnedSnapshot != null) {

            KeyedStateHandle jmKeyedState =
                    stateHandleFactory.create(keyGroupRangeOffsets, jobManagerOwnedSnapshot);
            StreamStateHandle taskLocalSnapshot = snapshotResult.getTaskLocalSnapshot();

            if (taskLocalSnapshot != null) {

                KeyedStateHandle localKeyedState =
                        stateHandleFactory.create(keyGroupRangeOffsets, taskLocalSnapshot);
                return SnapshotResult.withLocalState(jmKeyedState, localKeyedState);
            } else {

                return SnapshotResult.of(jmKeyedState);
            }
        } else {

            return SnapshotResult.empty();
        }
    }
}
