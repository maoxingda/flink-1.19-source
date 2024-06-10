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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.DuplicatingFileSystem;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStateToolset;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.NotDuplicatingCheckpointStateToolset;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory.FsCheckpointStateOutputStream;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** An implementation of durable checkpoint storage to file systems. */
public class FsCheckpointStorageAccess extends AbstractFsCheckpointStorageAccess {

    protected final FileSystem fileSystem;

    protected final Path checkpointsDirectory;

    protected final Path sharedStateDirectory;

    protected final Path taskOwnedStateDirectory;

    protected final int fileSizeThreshold;

    protected final int writeBufferSize;

    private boolean baseLocationsInitialized = false;

    public FsCheckpointStorageAccess(
            Path checkpointBaseDirectory,
            @Nullable Path defaultSavepointDirectory,
            JobID jobId,
            int fileSizeThreshold,
            int writeBufferSize)
            throws IOException {

        this(
                checkpointBaseDirectory.getFileSystem(),
                checkpointBaseDirectory,
                defaultSavepointDirectory,
                true,
                jobId,
                fileSizeThreshold,
                writeBufferSize);
    }

    public FsCheckpointStorageAccess(
            Path checkpointBaseDirectory,
            @Nullable Path defaultSavepointDirectory,
            boolean createCheckpointSubDirs,
            JobID jobId,
            int fileSizeThreshold,
            int writeBufferSize)
            throws IOException {

        this(
                checkpointBaseDirectory.getFileSystem(),
                checkpointBaseDirectory,
                defaultSavepointDirectory,
                createCheckpointSubDirs,
                jobId,
                fileSizeThreshold,
                writeBufferSize);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * FsCheckpointStorageAccess 的构造函数，用于初始化基于文件系统的检查点存储访问。
     *
     * @param fs 文件系统实例，用于访问存储检查点的文件系统
     * @param checkpointBaseDirectory 检查点的基本目录，所有检查点数据都将存储在此目录或其子目录下
     * @param defaultSavepointDirectory 默认的保存点目录（可以为null），如果未指定保存点目录，则可能使用该目录
     * @param createCheckpointSubDirs 是否为每个作业在基本目录下创建子目录来存储检查点数据
     * @param jobId 作业的JobID，用于在文件系统上组织检查点数据
     * @param fileSizeThreshold 触发单个检查点文件分片的文件大小阈值（以字节为单位）
     * @param writeBufferSize 写入文件时的缓冲区大小（以字节为单位）
     * @throws IOException 如果在初始化过程中发生I/O错误
    */
    public FsCheckpointStorageAccess(
            FileSystem fs,
            Path checkpointBaseDirectory,
            @Nullable Path defaultSavepointDirectory,
            boolean createCheckpointSubDirs,
            JobID jobId,
            int fileSizeThreshold,
            int writeBufferSize)
            throws IOException {
        // 调用父类的构造函数，初始化与JobID和默认保存点目录相关的状态
        //设置目录
        super(jobId, defaultSavepointDirectory);
        // 检查fileSizeThreshold参数是否大于等于0
        // 因为文件大小阈值必须是非负的
        checkArgument(fileSizeThreshold >= 0);
        // 检查writeBufferSize参数是否大于等于0
        // 因为写入缓冲区大小也必须是非负的
        checkArgument(writeBufferSize >= 0);
        // 检查fs参数是否非空，文件系统是访问存储的关键
        this.fileSystem = checkNotNull(fs);
        // 根据createCheckpointSubDirs参数决定检查点目录的创建方式
        // 如果为true，则在基本目录下为jobId创建一个子目录
        // 如果为false，则直接使用传入的checkpointBaseDirectory作为检查点目录
        this.checkpointsDirectory =
                createCheckpointSubDirs
                        ? getCheckpointDirectoryForJob(checkpointBaseDirectory, jobId)
                        : checkpointBaseDirectory;
        // 设置共享状态的目录，这是所有任务共享的检查点数据目录
        this.sharedStateDirectory = new Path(checkpointsDirectory, CHECKPOINT_SHARED_STATE_DIR);
        // 设置任务拥有的状态目录，这是每个任务独有的检查点数据目录
        this.taskOwnedStateDirectory =
                new Path(checkpointsDirectory, CHECKPOINT_TASK_OWNED_STATE_DIR);
        // 设置文件大小阈值
        this.fileSizeThreshold = fileSizeThreshold;
        // 设置写入缓冲区大小
        this.writeBufferSize = writeBufferSize;
    }

    // ------------------------------------------------------------------------

    @VisibleForTesting
    Path getCheckpointsDirectory() {
        return checkpointsDirectory;
    }

    // ------------------------------------------------------------------------
    //  CheckpointStorage implementation
    // ------------------------------------------------------------------------

    @Override
    public boolean supportsHighlyAvailableStorage() {
        return true;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 初始化检查点所需的基础目录。
     *
     * @throws IOException 如果在创建目录时发生I/O异常
    */
    @Override
    public void initializeBaseLocationsForCheckpoint() throws IOException {
        // 尝试创建共享状态目录
        if (!fileSystem.mkdirs(sharedStateDirectory)) {
            // 如果目录创建失败（可能是已经存在或其他原因），则抛出异常
            throw new IOException(
                    "Failed to create directory for shared state: " + sharedStateDirectory);
        }
        // 尝试创建任务拥有的状态目录
        if (!fileSystem.mkdirs(taskOwnedStateDirectory)) {
            // 如果目录创建失败（可能是已经存在或其他原因），则抛出异常
            throw new IOException(
                    "Failed to create directory for task owned state: " + taskOwnedStateDirectory);
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 初始化给定检查点ID的检查点存储位置。
     *
     * @param checkpointId 检查点ID
     * @return 初始化后的检查点存储位置
     * @throws IOException 如果在初始化过程中发生I/O异常
    */
    @Override
    public CheckpointStorageLocation initializeLocationForCheckpoint(long checkpointId)
            throws IOException {
        // 检查检查点ID是否非负，如果为负则抛出异常
        checkArgument(checkpointId >= 0, "Illegal negative checkpoint id: %s.", checkpointId);

        // prepare all the paths needed for the checkpoints
        // 准备检查点所需的所有路径
        // 创建检查点目录，包括使用给定的检查点目录和检查点ID
        final Path checkpointDir = createCheckpointDirectory(checkpointsDirectory, checkpointId);

        // create the checkpoint exclusive directory
        // 创建检查点的独占目录
        // 如果目录不存在，则使用fileSystem的mkdirs方法创建它
        fileSystem.mkdirs(checkpointDir);
        // 创建一个FsCheckpointStorageLocation实例，该实例封装了文件系统和检查点目录等所需的信息
        // FsCheckpointStorageLocation是用于在文件系统中存储检查点的位置信息
        return new FsCheckpointStorageLocation(
                fileSystem,
                checkpointDir,
                sharedStateDirectory,
                taskOwnedStateDirectory,
                CheckpointStorageLocationReference.getDefault(),
                fileSizeThreshold,
                writeBufferSize);
    }

    @Override
    public CheckpointStreamFactory resolveCheckpointStorageLocation(
            long checkpointId, CheckpointStorageLocationReference reference) throws IOException {

        if (reference.isDefaultReference()) {
            // default reference, construct the default location for that particular checkpoint
            final Path checkpointDir =
                    createCheckpointDirectory(checkpointsDirectory, checkpointId);

            return new FsCheckpointStorageLocation(
                    fileSystem,
                    checkpointDir,
                    sharedStateDirectory,
                    taskOwnedStateDirectory,
                    reference,
                    fileSizeThreshold,
                    writeBufferSize);
        } else {
            // location encoded in the reference
            final Path path = decodePathFromReference(reference);

            return new FsCheckpointStorageLocation(
                    path.getFileSystem(),
                    path,
                    path,
                    path,
                    reference,
                    fileSizeThreshold,
                    writeBufferSize);
        }
    }

    @Override
    public CheckpointStateOutputStream createTaskOwnedStateStream() {
        // as the comment of CheckpointStorageWorkerView#createTaskOwnedStateStream said we may
        // change into shared state,
        // so we use CheckpointedStateScope.SHARED here.
        return new FsCheckpointStateOutputStream(
                taskOwnedStateDirectory, fileSystem, writeBufferSize, fileSizeThreshold);
    }

    @Override
    public CheckpointStateToolset createTaskOwnedCheckpointStateToolset() {
        if (fileSystem instanceof DuplicatingFileSystem) {
            return new FsCheckpointStateToolset(
                    taskOwnedStateDirectory, (DuplicatingFileSystem) fileSystem);
        } else {
            return new NotDuplicatingCheckpointStateToolset();
        }
    }

    @Override
    protected CheckpointStorageLocation createSavepointLocation(FileSystem fs, Path location) {
        final CheckpointStorageLocationReference reference = encodePathAsReference(location);
        return new FsCheckpointStorageLocation(
                fs, location, location, location, reference, fileSizeThreshold, writeBufferSize);
    }

    public FsMergingCheckpointStorageAccess toFileMergingStorage(
            FileMergingSnapshotManager mergingSnapshotManager, Environment environment)
            throws IOException {
        return new FsMergingCheckpointStorageAccess(
                fileSystem,
                checkpointsDirectory,
                getDefaultSavepointDirectory(),
                environment.getJobID(),
                fileSizeThreshold,
                writeBufferSize,
                mergingSnapshotManager,
                environment);
    }
}
