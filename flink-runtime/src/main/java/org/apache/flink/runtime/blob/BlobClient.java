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

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobServerProtocol.BUFFER_SIZE;
import static org.apache.flink.runtime.blob.BlobServerProtocol.GET_OPERATION;
import static org.apache.flink.runtime.blob.BlobServerProtocol.JOB_RELATED_CONTENT;
import static org.apache.flink.runtime.blob.BlobServerProtocol.JOB_UNRELATED_CONTENT;
import static org.apache.flink.runtime.blob.BlobServerProtocol.RETURN_ERROR;
import static org.apache.flink.runtime.blob.BlobServerProtocol.RETURN_OKAY;
import static org.apache.flink.runtime.blob.BlobUtils.readExceptionFromStream;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The BLOB client can communicate with the BLOB server and either upload (PUT), download (GET), or
 * delete (DELETE) BLOBs.
 */
public final class BlobClient implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(BlobClient.class);

    /** The socket connection to the BLOB server. */
    private final Socket socket;

    /**
     * Instantiates a new BLOB client.
     *
     * @param serverAddress the network address of the BLOB server
     * @param clientConfig additional configuration like SSL parameters required to connect to the
     *     blob server
     * @throws IOException thrown if the connection to the BLOB server could not be established
     */
    public BlobClient(InetSocketAddress serverAddress, Configuration clientConfig)
            throws IOException {
        Socket socket = null;

        try {
            // create an SSL socket if configured
            if (SecurityOptions.isInternalSSLEnabled(clientConfig)
                    && clientConfig.get(BlobServerOptions.SSL_ENABLED)) {
                LOG.info("Using ssl connection to the blob server");

                socket = SSLUtils.createSSLClientSocketFactory(clientConfig).createSocket();
            } else {
                socket = new Socket();
            }

            // Establish the socket using the hostname and port. This avoids a potential issue where
            // the
            // InetSocketAddress can cache a failure in hostname resolution forever.
            socket.connect(
                    new InetSocketAddress(serverAddress.getHostName(), serverAddress.getPort()),
                    clientConfig.get(BlobServerOptions.CONNECT_TIMEOUT));
            socket.setSoTimeout(clientConfig.get(BlobServerOptions.SO_TIMEOUT));
        } catch (Exception e) {
            BlobUtils.closeSilently(socket, LOG);
            throw new IOException("Could not connect to BlobServer at address " + serverAddress, e);
        }

        this.socket = socket;
    }

    /**
     * Downloads the given BLOB from the given server and stores its contents to a (local) file.
     *
     * <p>Transient BLOB files are deleted after a successful copy of the server's data into the
     * given <tt>localJarFile</tt>.
     *
     * @param jobId job ID the BLOB belongs to or <tt>null</tt> if job-unrelated
     * @param blobKey BLOB key
     * @param localJarFile the local file to write to
     * @param serverAddress address of the server to download from
     * @param blobClientConfig client configuration for the connection
     * @param numFetchRetries number of retries before failing
     * @throws IOException if an I/O error occurs during the download
     */
    static void downloadFromBlobServer(
            @Nullable JobID jobId,
            BlobKey blobKey,
            File localJarFile,
            InetSocketAddress serverAddress,
            Configuration blobClientConfig,
            int numFetchRetries)
            throws IOException {

        final byte[] buf = new byte[BUFFER_SIZE];
        LOG.info("Downloading {}/{} from {}", jobId, blobKey, serverAddress);

        // loop over retries
        int attempt = 0;
        while (true) {
            try (final BlobClient bc = new BlobClient(serverAddress, blobClientConfig);
                    final InputStream is = bc.getInternal(jobId, blobKey);
                    final OutputStream os = new FileOutputStream(localJarFile)) {
                while (true) {
                    final int read = is.read(buf);
                    if (read < 0) {
                        break;
                    }
                    os.write(buf, 0, read);
                }

                return;
            } catch (Throwable t) {
                String message =
                        "Failed to fetch BLOB "
                                + jobId
                                + "/"
                                + blobKey
                                + " from "
                                + serverAddress
                                + " and store it under "
                                + localJarFile.getAbsolutePath();
                if (attempt < numFetchRetries) {
                    if (LOG.isDebugEnabled()) {
                        LOG.error(message + " Retrying...", t);
                    } else {
                        LOG.error(message + " Retrying...");
                    }
                } else {
                    LOG.error(message + " No retries left.", t);
                    throw new IOException(message, t);
                }

                // retry
                ++attempt;
                LOG.info(
                        "Downloading {}/{} from {} (retry {})",
                        jobId,
                        blobKey,
                        serverAddress,
                        attempt);
            }
        } // end loop over retries
    }

    @Override
    public void close() throws IOException {
        this.socket.close();
    }

    public boolean isClosed() {
        return this.socket.isClosed();
    }

    public boolean isConnected() {
        return socket.isConnected();
    }

    // --------------------------------------------------------------------------------------------
    //  GET
    // --------------------------------------------------------------------------------------------

    /**
     * Downloads the BLOB identified by the given BLOB key from the BLOB server.
     *
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param blobKey blob key associated with the requested file
     * @return an input stream to read the retrieved data from
     * @throws FileNotFoundException if there is no such file;
     * @throws IOException if an I/O error occurs during the download
     */
    InputStream getInternal(@Nullable JobID jobId, BlobKey blobKey) throws IOException {

        if (this.socket.isClosed()) {
            throw new IllegalStateException(
                    "BLOB Client is not connected. "
                            + "Client has been shut down or encountered an error before.");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("GET BLOB {}/{} from {}.", jobId, blobKey, socket.getLocalSocketAddress());
        }

        try {
            OutputStream os = this.socket.getOutputStream();
            InputStream is = this.socket.getInputStream();

            // Send GET header
            sendGetHeader(os, jobId, blobKey);
            receiveAndCheckGetResponse(is);

            return new BlobInputStream(is, blobKey, os);
        } catch (Throwable t) {
            BlobUtils.closeSilently(socket, LOG);
            throw new IOException("GET operation failed: " + t.getMessage(), t);
        }
    }

    /**
     * Constructs and writes the header data for a GET operation to the given output stream.
     *
     * @param outputStream the output stream to write the header data to
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param blobKey blob key associated with the requested file
     * @throws IOException thrown if an I/O error occurs while writing the header data to the output
     *     stream
     */
    private static void sendGetHeader(
            OutputStream outputStream, @Nullable JobID jobId, BlobKey blobKey) throws IOException {
        checkNotNull(blobKey);
        checkArgument(
                jobId != null || blobKey instanceof TransientBlobKey,
                "permanent BLOBs must be job-related");

        // Signal type of operation
        outputStream.write(GET_OPERATION);

        // Send job ID and key
        if (jobId == null) {
            outputStream.write(JOB_UNRELATED_CONTENT);
        } else {
            outputStream.write(JOB_RELATED_CONTENT);
            outputStream.write(jobId.getBytes());
        }
        blobKey.writeToOutputStream(outputStream);
    }

    /**
     * Reads the response from the input stream and throws in case of errors.
     *
     * @param is stream to read from
     * @throws IOException if the response is an error or reading the response failed
     */
    private static void receiveAndCheckGetResponse(InputStream is) throws IOException {
        int response = is.read();
        if (response < 0) {
            throw new EOFException("Premature end of response");
        }
        if (response == RETURN_ERROR) {
            Throwable cause = readExceptionFromStream(is);
            throw new IOException("Server side error: " + cause.getMessage(), cause);
        } else if (response != RETURN_OKAY) {
            throw new IOException("Unrecognized response");
        }
    }

    // --------------------------------------------------------------------------------------------
    //  PUT
    // --------------------------------------------------------------------------------------------

    /**
     * Uploads data from the given byte buffer to the BLOB server.
     *
     * @param jobId the ID of the job the BLOB belongs to (or <tt>null</tt> if job-unrelated)
     * @param value the buffer to read the data from
     * @param offset the read offset within the buffer
     * @param len the number of bytes to read from the buffer
     * @param blobType whether the BLOB should become permanent or transient
     * @return the computed BLOB key of the uploaded BLOB
     * @throws IOException thrown if an I/O error occurs while uploading the data to the BLOB server
     */
    BlobKey putBuffer(
            @Nullable JobID jobId, byte[] value, int offset, int len, BlobKey.BlobType blobType)
            throws IOException {

        if (this.socket.isClosed()) {
            throw new IllegalStateException(
                    "BLOB Client is not connected. "
                            + "Client has been shut down or encountered an error before.");
        }
        checkNotNull(value);

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "PUT BLOB buffer ("
                            + len
                            + " bytes) to "
                            + socket.getLocalSocketAddress()
                            + ".");
        }

        try (BlobOutputStream os = new BlobOutputStream(jobId, blobType, socket)) {
            os.write(value, offset, len);
            // Receive blob key and compare
            return os.finish();
        } catch (Throwable t) {
            BlobUtils.closeSilently(socket, LOG);
            throw new IOException("PUT operation failed: " + t.getMessage(), t);
        }
    }

    /**
     * Uploads data from the given input stream to the BLOB server.
     *
     * @param jobId the ID of the job the BLOB belongs to (or <tt>null</tt> if job-unrelated)
     * @param inputStream the input stream to read the data from
     * @param blobType whether the BLOB should become permanent or transient
     * @return the computed BLOB key of the uploaded BLOB
     * @throws IOException thrown if an I/O error occurs while uploading the data to the BLOB server
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将给定输入流中的数据上传到BLOB服务器。
     * 将输入流（InputStream）中的数据作为 BLOB（Binary Large Object，二进制大对象）上传到一个 BLOB 客户端
     *  JobID jobId：作业的ID，可以是null。
     *  InputStream inputStream：要上传的数据流。
     *  BlobKey.BlobType blobType：BLOB 的类型。
    */
    BlobKey putInputStream(
            @Nullable JobID jobId, InputStream inputStream, BlobKey.BlobType blobType)
            throws IOException {
        /** 如果 BLOB 客户端的 socket 已经关闭，则抛出一个 IllegalStateException，表示客户端未连接或之前已遇到错误。 */
        if (this.socket.isClosed()) {
            throw new IllegalStateException(
                    "BLOB Client is not connected. "
                            + "Client has been shut down or encountered an error before.");
        }
        /**
         * 使用 checkNotNull 方法确保 inputStream 不是 null。如果为 null，通常会抛出一个 NullPointerException。
         */
        checkNotNull(inputStream);

        if (LOG.isDebugEnabled()) {
            LOG.debug("PUT BLOB stream to {}.", socket.getLocalSocketAddress());
        }
        /**
         * 1.创建一个 `BlobOutputStream` 对象，用于将数据写入 BLOB 客户端。
         */
        try (BlobOutputStream os = new BlobOutputStream(jobId, blobType, socket)) {
            /**
             * 2.使用 `IOUtils.copyBytes` 方法将 `inputStream` 中的数据复制到 `BlobOutputStream` 中。
             * 这里使用了缓冲区（`BUFFER_SIZE`）来优化性能。
             */
            IOUtils.copyBytes(inputStream, os, BUFFER_SIZE, false);
            /**
             * 3.调用 `os.finish()` 方法完成上传操作，并返回 `BlobKey`。
             */
            return os.finish();
        } catch (Throwable t) {
            BlobUtils.closeSilently(socket, LOG);
            throw new IOException("PUT operation failed: " + t.getMessage(), t);
        }
    }

    /**
     * Uploads the JAR files to the {@link PermanentBlobService} of the {@link BlobServer} at the
     * given address with HA as configured.
     *
     * @param serverAddress Server address of the {@link BlobServer}
     * @param clientConfig Any additional configuration for the blob client
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param files List of files to upload
     * @throws IOException if the upload fails
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将JAR文件上载到指定地址的 BlobServer 的 link PermanentBlobService
    */
    public static List<PermanentBlobKey> uploadFiles(
            InetSocketAddress serverAddress,
            Configuration clientConfig,
            JobID jobId,
            List<Path> files)
            throws IOException {
        /**
         * 使用 checkNotNull 方法来确保 jobId 不为 null。如果为 null，则通常会抛出 NullPointerException。
         */
        checkNotNull(jobId);
        /**
         * 如果传入的文件列表为空，则直接返回一个空的 List，避免不必要的操作。
         */
        if (files.isEmpty()) {
            return Collections.emptyList();
        } else {
            /**
             * 创建一个新的 ArrayList 来存储每个上传文件对应的 PermanentBlobKey。
             */
            List<PermanentBlobKey> blobKeys = new ArrayList<>();
            /**
             * 创建BlobClient blobClient
             */
            try (BlobClient blobClient = new BlobClient(serverAddress, clientConfig)) {
                /** 遍历传入的文件列表 */
                for (final Path file : files) {
                    /** 使用 BlobClient 的 uploadFile 方法上传每个文件 */
                    final PermanentBlobKey key = blobClient.uploadFile(jobId, file);
                    /** 上传后得到的 PermanentBlobKey 被添加到 blobKeys 列表中 */
                    blobKeys.add(key);
                }
            }
            /** 返回 BLOB 键列表 */
            return blobKeys;
        }
    }

    /**
     * Uploads a single file to the {@link PermanentBlobService} of the given {@link BlobServer}.
     *
     * @param jobId ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
     * @param file file to upload
     * @throws IOException if the upload fails
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将单个文件上载到给定BlobServer的PermanentBlobService
    */
    public PermanentBlobKey uploadFile(JobID jobId, Path file) throws IOException {
        /**
         * 通过文件路径 file 获取其所在的文件系统 fs。这通常用于后续的文件操作，如打开文件。
         */
        final FileSystem fs = file.getFileSystem();
        /**
         * 1.使用 `fs.open(file)` 打开文件，得到一个 `InputStream`，即文件的输入流。
         *
         */
        try (InputStream is = fs.open(file)) {
            /**
             * 2.用之前定义的 `putInputStream` 方法，将文件的输入流上传到 BLOB 客户端。
             * 这里传递的 `blobType` 参数是 `PERMANENT_BLOB`，表示上传的 BLOB 是永久性的。
             * 3.将 `putInputStream` 方法返回的 `BlobKey` 强制转换为 `PermanentBlobKey` 并返回。
             */
            return (PermanentBlobKey) putInputStream(jobId, is, PERMANENT_BLOB);
        }
    }
}
