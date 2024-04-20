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

package org.apache.flink.runtime.client;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.SupplierWithException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Contains utility methods for clients. */
public enum ClientUtils {
    ;

    /**
     * Extracts all files required for the execution from the given {@link JobGraph} and uploads
     * them using the {@link BlobClient} from the given {@link Supplier}.
     *
     * @param jobGraph jobgraph requiring files
     * @param clientSupplier supplier of blob client to upload files with
     * @throws FlinkException if the upload fails
     */
    public static void extractAndUploadJobGraphFiles(
            JobGraph jobGraph, SupplierWithException<BlobClient, IOException> clientSupplier)
            throws FlinkException {
        List<Path> userJars = jobGraph.getUserJars();
        Collection<Tuple2<String, Path>> userArtifacts =
                jobGraph.getUserArtifacts().entrySet().stream()
                        .map(
                                entry ->
                                        Tuple2.of(
                                                entry.getKey(),
                                                new Path(entry.getValue().filePath)))
                        .collect(Collectors.toList());

        uploadJobGraphFiles(jobGraph, userJars, userArtifacts, clientSupplier);
    }

    /**
     * Uploads the given jars and artifacts required for the execution of the given {@link JobGraph}
     * using the {@link BlobClient} from the given {@link Supplier}.
     *
     * @param jobGraph jobgraph requiring files
     * @param userJars jars to upload
     * @param userArtifacts artifacts to upload
     * @param clientSupplier supplier of blob client to upload files with
     * @throws FlinkException if the upload fails
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 使用BlobClient上传执行给定的JobGraph所需的给定jar和工件。
     * uploadJobGraphFiles的静态方法，它的目的是上传作业图（JobGraph）的相关文件到 Flink 的 Blob 服务器
    */
    public static void uploadJobGraphFiles(
            JobGraph jobGraph,
            Collection<Path> userJars,
            Collection<Tuple2<String, org.apache.flink.core.fs.Path>> userArtifacts,
            SupplierWithException<BlobClient, IOException> clientSupplier)
            throws FlinkException {
        /**
         * 如果 userJars 或 userArtifacts 集合不为空，即存在需要上传的文件，则执行上传逻辑。
         */
        if (!userJars.isEmpty() || !userArtifacts.isEmpty()) {
            /** 创建 BlobClient 并上传文件 */
            try (BlobClient client = clientSupplier.get()) {
                /** 上传 JAR 文件 */
                uploadAndSetUserJars(jobGraph, userJars, client);
                /** 上传 缓存文件 文件 */
                uploadAndSetUserArtifacts(jobGraph, userArtifacts, client);
            } catch (IOException ioe) {
                throw new FlinkException("Could not upload job files.", ioe);
            }
        }
        /**
         * 无论是否上传了文件，都会调用 writeUserArtifactEntriesToConfiguration 方法，
         * 该方法可能是将 jobGraph 中的用户工件条目写入到其配置中，以便稍后在集群中使用。
         */
        jobGraph.writeUserArtifactEntriesToConfiguration();
    }

    /**
     * Uploads the given user jars using the given {@link BlobClient}, and sets the appropriate
     * blobkeys on the given {@link JobGraph}.
     *
     * @param jobGraph jobgraph requiring user jars
     * @param userJars jars to upload
     * @param blobClient client to upload jars with
     * @throws IOException if the upload fails
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 通过BlobClient上传用户的Jar文件
     * 上传 JAR 文件并获取 BLOB 键，以及将这些 BLOB 键设置到 Flink 的 JobGraph 对象中。
    */
    private static void uploadAndSetUserJars(
            JobGraph jobGraph, Collection<Path> userJars, BlobClient blobClient)
            throws IOException {
        /** 上传 JAR 文件并获取 BLOB 键 */
        Collection<PermanentBlobKey> blobKeys =
                uploadUserJars(jobGraph.getJobID(), userJars, blobClient);
        /** 调用了 setUserJarBlobKeys 方法，将刚刚获得的 blobKeys 集合和 jobGraph 作为参数传递。
         * 这个方法负责将这些 BLOB 键添加到 JobGraph 对象中， */
        setUserJarBlobKeys(blobKeys, jobGraph);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将用户提供的 JAR 文件集合上传到 BLOB 客户端，并返回上传后每个文件对应的永久 BLOB 键集合
    */
    private static Collection<PermanentBlobKey> uploadUserJars(
            JobID jobId, Collection<Path> userJars, BlobClient blobClient) throws IOException {
        /**
         * 创建一个新的 ArrayList 实例，其初始容量设置为 userJars 集合的大小，用于存储上传后每个 JAR 文件对应的 PermanentBlobKey。
         */
        Collection<PermanentBlobKey> blobKeys = new ArrayList<>(userJars.size());
        /** 遍历 JAR 文件集合并上传 */
        for (Path jar : userJars) {
            /**
             * 对于每个路径，调用 blobClient 的 uploadFile 方法来上传文件，并获取上传后返回的 PermanentBlobKey。
             */
            final PermanentBlobKey blobKey = blobClient.uploadFile(jobId, jar);
            /** 键添加到 blobKeys 集合中 */
            blobKeys.add(blobKey);
        }
        /** 返回 BLOB 键集合 */
        return blobKeys;
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将一组 PermanentBlobKey 添加到 JobGraph 对象中
    */
    private static void setUserJarBlobKeys(
            /**
             * 遍历 blobKeys 集合中的每个 PermanentBlobKey。对于集合中的每个元素，它调用 jobGraph 的 addUserJarBlobKey 方法，
             * 并将当前遍历到的 PermanentBlobKey 作为
             */
            Collection<PermanentBlobKey> blobKeys, JobGraph jobGraph) {
        blobKeys.forEach(jobGraph::addUserJarBlobKey);
    }

    /**
     * Uploads the given user artifacts using the given {@link BlobClient}, and sets the appropriate
     * blobkeys on the given {@link JobGraph}.
     *
     * @param jobGraph jobgraph requiring user artifacts
     * @param artifactPaths artifacts to upload
     * @param blobClient client to upload artifacts with
     * @throws IOException if the upload fails
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 上传一组用户定义的artifact文件（可能是一些程序运行时需要的文件或资源），
     * 并将上传后的Blob（二进制大对象）的键（key）设置到JobGraph对象中。
    */
    private static void uploadAndSetUserArtifacts(
            JobGraph jobGraph,
            Collection<Tuple2<String, Path>> artifactPaths,
            BlobClient blobClient)
            throws IOException {
        /**
         * 负责将artifactPaths中指定的文件上传到Blob存储，并返回一个包含每个artifact的键（key）的集合
         */
        Collection<Tuple2<String, PermanentBlobKey>> blobKeys =
                uploadUserArtifacts(jobGraph.getJobID(), artifactPaths, blobClient);
        /**
         * 设置Blob Keys到JobGraph:
         */
        setUserArtifactBlobKeys(jobGraph, blobKeys);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 上传用户提供的本地文件（非分布式文件系统上的文件）到 BLOB 客户端，并返回包含文件名和对应 BLOB 键的元组集合。
    */
    private static Collection<Tuple2<String, PermanentBlobKey>> uploadUserArtifacts(
            JobID jobID, Collection<Tuple2<String, Path>> userArtifacts, BlobClient blobClient)
            throws IOException {
        /**
         * 创建一个新的 ArrayList 来存储上传文件后得到的文件名和 BLOB 键的元组。
         * 列表的初始容量设置为 userArtifacts 集合的大小
         */
        Collection<Tuple2<String, PermanentBlobKey>> blobKeys =
                new ArrayList<>(userArtifacts.size());
        /**
         * 遍历 userArtifacts 集合中的每个元组。对于每个元组，它包含文件名（userArtifact.f0）和文件路径（userArtifact.f1）。
         */
        for (Tuple2<String, Path> userArtifact : userArtifacts) {
            // only upload local files
            /**
             * 检查文件是否位于分布式文件系统上。如果文件不在分布式文件系统上（即它是本地的），则调用 blobClient 的 uploadFile 方法来上传文件，
             * 并传递作业 ID 和文件路径作为参数。uploadFile 方法返回一个 PermanentBlobKey，代表上传文件的 BLOB 键。
             */
            if (!userArtifact.f1.getFileSystem().isDistributedFS()) {
                final PermanentBlobKey blobKey = blobClient.uploadFile(jobID, userArtifact.f1);
                blobKeys.add(Tuple2.of(userArtifact.f0, blobKey));
            }
        }
        /** 返回 BLOB 键集合 */
        return blobKeys;
    }

    private static void setUserArtifactBlobKeys(
            JobGraph jobGraph, Collection<Tuple2<String, PermanentBlobKey>> blobKeys)
            throws IOException {
        for (Tuple2<String, PermanentBlobKey> blobKey : blobKeys) {
            jobGraph.setUserArtifactBlobKey(blobKey.f0, blobKey.f1);
        }
    }
}
