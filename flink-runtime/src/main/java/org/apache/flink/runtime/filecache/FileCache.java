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

package org.apache.flink.runtime.filecache;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.cache.DistributedCache.DistributedCacheEntry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.PermanentBlobService;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The FileCache is used to access registered cache files when a task is deployed.
 *
 * <p>Files and zipped directories are retrieved from the {@link PermanentBlobService}. The
 * life-cycle of these files is managed by the blob-service.
 *
 * <p>Retrieved directories will be expanded in "{@code <system-tmp-dir>/tmp_<jobID>/}" and deleted
 * when the task is unregistered after a 5 second delay, unless a new task requests the file in the
 * meantime.
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * FileCache用于在部署任务时访问已注册的缓存文件。
*/
public class FileCache {

    private static final Logger LOG = LoggerFactory.getLogger(FileCache.class);

    /** cache-wide lock to ensure consistency. copies are not done under this lock. */
    private final Object lock = new Object();

    private final Map<JobID, Map<String, Future<Path>>> entries;

    private final Map<JobID, Set<ExecutionAttemptID>> jobRefHolders;

    private final ScheduledExecutorService executorService;

    private final File[] storageDirectories;

    private final Thread shutdownHook;

    private int nextDirectory;

    private final PermanentBlobService blobService;

    private final long cleanupInterval; // in milliseconds

    // ------------------------------------------------------------------------

    public FileCache(String[] tempDirectories, PermanentBlobService blobService)
            throws IOException {
        this(
                tempDirectories,
                blobService,
                Executors.newScheduledThreadPool(10, new ExecutorThreadFactory("flink-file-cache")),
                5000);
    }

    @VisibleForTesting
    FileCache(
            String[] tempDirectories,
            PermanentBlobService blobService,
            ScheduledExecutorService executorService,
            long cleanupInterval)
            throws IOException {

        Preconditions.checkNotNull(tempDirectories);
        this.cleanupInterval = cleanupInterval;

        storageDirectories = new File[tempDirectories.length];

        for (int i = 0; i < tempDirectories.length; i++) {
            String cacheDirName = "flink-dist-cache-" + UUID.randomUUID().toString();
            storageDirectories[i] = new File(tempDirectories[i], cacheDirName);
            String path = storageDirectories[i].getAbsolutePath();

            if (storageDirectories[i].mkdirs()) {
                LOG.info("User file cache uses directory " + path);
            } else {
                LOG.error("User file cache cannot create directory " + path);
                // delete all other directories we created so far
                for (int k = 0; k < i; k++) {
                    if (!storageDirectories[k].delete()) {
                        LOG.warn(
                                "User file cache cannot remove prior directory "
                                        + storageDirectories[k].getAbsolutePath());
                    }
                }
                throw new IOException("File cache cannot create temp storage directory: " + path);
            }
        }

        this.shutdownHook = createShutdownHook(this, LOG);

        this.entries = new HashMap<>();
        this.jobRefHolders = new HashMap<>();
        this.executorService = executorService;
        this.blobService = blobService;
    }

    /** Shuts down the file cache by cancelling all. */
    public void shutdown() {
        synchronized (lock) {
            // first shutdown the thread pool
            ScheduledExecutorService es = this.executorService;
            if (es != null) {
                es.shutdown();
                try {
                    es.awaitTermination(cleanupInterval, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    // may happen
                }
            }

            entries.clear();
            jobRefHolders.clear();

            // clean up the all storage directories
            for (File dir : storageDirectories) {
                try {
                    FileUtils.deleteDirectory(dir);
                    LOG.info("removed file cache directory {}", dir.getAbsolutePath());
                } catch (IOException e) {
                    LOG.error(
                            "File cache could not properly clean up storage directory: {}",
                            dir.getAbsolutePath(),
                            e);
                }
            }

            // Remove shutdown hook to prevent resource leaks
            ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);
        }
    }

    // ------------------------------------------------------------------------

    /**
     * If the file doesn't exists locally, retrieve the file from the blob-service.
     *
     * @param entry The cache entry descriptor (path, executable flag)
     * @param jobID The ID of the job for which the file is copied.
     * @return The handle to the task that copies the file.
     */
    public Future<Path> createTmpFile(
            String name, DistributedCacheEntry entry, JobID jobID, ExecutionAttemptID executionId)
            throws Exception {
        synchronized (lock) {
            Map<String, Future<Path>> jobEntries =
                    entries.computeIfAbsent(jobID, k -> new HashMap<>());

            // register reference holder
            final Set<ExecutionAttemptID> refHolders =
                    jobRefHolders.computeIfAbsent(jobID, id -> new HashSet<>());
            refHolders.add(executionId);

            Future<Path> fileEntry = jobEntries.get(name);
            if (fileEntry != null) {
                // file is already in the cache. return a future that
                // immediately returns the file
                return fileEntry;
            } else {
                // need to copy the file

                // create the target path
                File tempDirToUse = new File(storageDirectories[nextDirectory++], jobID.toString());
                if (nextDirectory >= storageDirectories.length) {
                    nextDirectory = 0;
                }

                // kick off the copying
                Callable<Path> cp;
                if (entry.blobKey != null) {
                    cp =
                            new CopyFromBlobProcess(
                                    entry,
                                    jobID,
                                    blobService,
                                    new Path(tempDirToUse.getAbsolutePath()));
                } else {
                    cp = new CopyFromDFSProcess(entry, new Path(tempDirToUse.getAbsolutePath()));
                }
                FutureTask<Path> copyTask = new FutureTask<>(cp);
                executorService.submit(copyTask);

                // store our entry
                jobEntries.put(name, copyTask);

                return copyTask;
            }
        }
    }

    private static Thread createShutdownHook(final FileCache cache, final Logger logger) {

        return ShutdownHookUtil.addShutdownHook(
                cache::shutdown, FileCache.class.getSimpleName(), logger);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 释放jobid对应的文件缓存
    */
    public void releaseJob(JobID jobId, ExecutionAttemptID executionId) {
        // 检查jobId是否为空，如果为空则抛出NullPointerException异常
        checkNotNull(jobId);
        // 使用synchronized关键字对lock对象进行同步，以确保在多线程环境下对jobRefHolders的操作是线程安全的
        synchronized (lock) {
            // Map<JobID, Set<ExecutionAttemptID>> 从jobRefHolders这个Map中获取与jobId关联的ExecutionAttemptID集合
            Set<ExecutionAttemptID> jobRefCounter = jobRefHolders.get(jobId);
            // 如果jobRefCounter为空或者没有元素（即该jobId没有关联的executionId），则直接返回
            if (jobRefCounter == null || jobRefCounter.isEmpty()) {
                return;
            }
            // 从jobRefCounter集合中移除指定的executionId
            jobRefCounter.remove(executionId);
            // 使用executorService在指定时间间隔后执行DeleteProcess任务，以清理与jobId相关的资源
            // 这里假设DeleteProcess是一个实现了Runnable接口的类，负责清理工作
            if (jobRefCounter.isEmpty()) {
                executorService.schedule(
                        new DeleteProcess(jobId), cleanupInterval, TimeUnit.MILLISECONDS);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  background processes
    // ------------------------------------------------------------------------

    /** Asynchronous file copy process from blob server. */
    private static class CopyFromBlobProcess implements Callable<Path> {

        private final PermanentBlobKey blobKey;
        private final Path target;
        private final boolean isDirectory;
        private final boolean isExecutable;
        private final JobID jobID;
        private final PermanentBlobService blobService;

        CopyFromBlobProcess(
                DistributedCacheEntry e, JobID jobID, PermanentBlobService blobService, Path target)
                throws Exception {
            this.isExecutable = e.isExecutable;
            this.isDirectory = e.isZipped;
            this.jobID = jobID;
            this.blobService = blobService;
            this.blobKey =
                    InstantiationUtil.deserializeObject(
                            e.blobKey, Thread.currentThread().getContextClassLoader());
            this.target = target;
        }

        @Override
        public Path call() throws IOException {
            final File file = blobService.getFile(jobID, blobKey);

            if (isDirectory) {
                Path directory =
                        FileUtils.expandDirectory(new Path(file.getAbsolutePath()), target);
                return directory;
            } else {
                //noinspection ResultOfMethodCallIgnored
                file.setExecutable(isExecutable);
                return Path.fromLocalFile(file);
            }
        }
    }

    /** Asynchronous file copy process. */
    private static class CopyFromDFSProcess implements Callable<Path> {

        private final Path filePath;
        private final Path cachedPath;
        private final boolean executable;
        private final boolean isZipped;

        public CopyFromDFSProcess(DistributedCacheEntry e, Path cachedPath) {
            this.filePath = new Path(e.filePath);
            this.executable = e.isExecutable;
            this.isZipped = e.isZipped;

            String sourceFile = e.filePath;
            int posOfSep = sourceFile.lastIndexOf("/");
            if (posOfSep > 0) {
                sourceFile = sourceFile.substring(posOfSep + 1);
            }

            this.cachedPath = new Path(cachedPath, sourceFile);
        }

        @Override
        public Path call() throws IOException {
            // let exceptions propagate. we can retrieve them later from
            // the future and report them upon access to the result
            FileUtils.copy(filePath, cachedPath, this.executable);
            if (isZipped) {
                return FileUtils.expandDirectory(cachedPath, cachedPath.getParent());
            }
            return cachedPath;
        }
    }

    /** If no task is using this file after 5 seconds, clear it. */
    @VisibleForTesting
    class DeleteProcess implements Runnable {

        private final JobID jobID;

        DeleteProcess(JobID jobID) {
            this.jobID = jobID;
        }

        @Override
        public void run() {
            try {
                // 使用锁来保证对共享资源的线程安全访问
                synchronized (lock) {
                    // 从jobRefHolders这个Map中获取与当前jobID关联的ExecutionAttemptID集合
                    Set<ExecutionAttemptID> jobRefs = jobRefHolders.get(jobID);
                    // 如果jobRefs不为空但已经为空集（意味着所有的ExecutionAttemptID都已被处理完或移除）
                    if (jobRefs != null && jobRefs.isEmpty()) {
                        // abort the copy
                        // 取消所有与当前jobID关联的Future<Path>任务（可能是文件复制或下载任务）
                        // 传入true参数表示如果任务已经开始，则尝试中断它
                        for (Future<Path> fileFuture : entries.get(jobID).values()) {
                            fileFuture.cancel(true);
                        }

                        // remove job specific entries in maps
                        // 从entries这个Map中移除当前jobID及其相关的条目
                        entries.remove(jobID);
                        // 从jobRefHolders这个Map中移除当前jobID及其相关的ExecutionAttemptID集合
                        jobRefHolders.remove(jobID);

                        // remove the job wide temp directories
                        // 遍历所有存储目录，并删除与当前jobID相关的临时目录
                        for (File storageDirectory : storageDirectories) {
                            // 构造与当前jobID相关的临时目录路径
                            File tempDir = new File(storageDirectory, jobID.toString());
                            // 使用FileUtils工具类删除该临时目录及其所有内容
                            FileUtils.deleteDirectory(tempDir);
                        }
                    }
                }
            } catch (IOException e) {
                // 如果在删除文件或目录时发生IO异常，记录错误日志
                LOG.error("Could not delete file from local file cache.", e);
            }
        }
    }
}
