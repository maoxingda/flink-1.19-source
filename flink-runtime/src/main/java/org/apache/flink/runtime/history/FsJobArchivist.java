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

package org.apache.flink.runtime.history;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonEncoding;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;

/** Utility class for writing an archive file to a {@link FileSystem} and reading it back. */
public class FsJobArchivist {

    private static final Logger LOG = LoggerFactory.getLogger(FsJobArchivist.class);
    private static final JsonFactory jacksonFactory = new JsonFactory();
    private static final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

    private static final String ARCHIVE = "archive";
    private static final String PATH = "path";
    private static final String JSON = "json";

    private FsJobArchivist() {}

    /**
     * Writes the given {@link AccessExecutionGraph} to the {@link FileSystem} pointed to by {@link
     * JobManagerOptions#ARCHIVE_DIR}.
     *
     * @param rootPath directory to which the archive should be written to
     * @param jobId job id
     * @param jsonToArchive collection of json-path pairs to that should be archived
     * @return path to where the archive was written, or null if no archive was created
     * @throws IOException
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将给定的AccessExecutionGraph写入由JobManagerOptions#ARCHIVE_DIR指向的  FileSystem。
    */
    public static Path archiveJob(
            Path rootPath, JobID jobId, Collection<ArchivedJson> jsonToArchive) throws IOException {
        try {
            /** 获取文件系统 */
            FileSystem fs = rootPath.getFileSystem();
            /** 构建归档文件路径 */
            Path path = new Path(rootPath, jobId.toString());
            /**
             * 在fs文件系统中创建一个新的输出流，
             * 用于写入数据到path指定的路径。
             * 这里使用的是NO_OVERWRITE模式，意味着如果路径已经存在，
             * 则不会覆盖它，而是会抛出异常。
             */
            OutputStream out = fs.create(path, FileSystem.WriteMode.NO_OVERWRITE);
            /**
             * 创建一个JsonGenerator对象，并使用它来写入JSON数据。
             * 一个包含多个ArchivedJson对象的数组，每个ArchivedJson对象都是一个包含path和json字段的对象。
             */
            try (JsonGenerator gen = jacksonFactory.createGenerator(out, JsonEncoding.UTF8)) {
                gen.writeStartObject();
                gen.writeArrayFieldStart(ARCHIVE);
                /**
                 * 循环jsonToArchive集合
                 */
                for (ArchivedJson archive : jsonToArchive) {
                    gen.writeStartObject();
                    gen.writeStringField(PATH, archive.getPath());
                    gen.writeStringField(JSON, archive.getJson());
                    gen.writeEndObject();
                }
                gen.writeEndArray();
                gen.writeEndObject();
            } catch (Exception e) {
                /**
                 * 如果在写入JSON数据时发生异常，会捕获该异常，并删除已创建的归档文件（防止留下不完整的文件）。
                 * 然后重新抛出捕获的异常。
                 */
                fs.delete(path, false);
                throw e;
            }
            /** 记录日志并返回路径 */
            LOG.info("Job {} has been archived at {}.", jobId, path);
            return path;
        } catch (IOException e) {
            LOG.error("Failed to archive job.", e);
            throw e;
        }
    }

    /**
     * Reads the given archive file and returns a {@link Collection} of contained {@link
     * ArchivedJson}.
     *
     * @param file archive to extract
     * @return collection of archived jsons
     * @throws IOException if the file can't be opened, read or doesn't contain valid json
     */
    public static Collection<ArchivedJson> getArchivedJsons(Path file) throws IOException {
        try (FSDataInputStream input = file.getFileSystem().open(file);
                ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            IOUtils.copyBytes(input, output);

            try {
                JsonNode archive = mapper.readTree(output.toByteArray());

                Collection<ArchivedJson> archives = new ArrayList<>();
                for (JsonNode archivePart : archive.get(ARCHIVE)) {
                    String path = archivePart.get(PATH).asText();
                    String json = archivePart.get(JSON).asText();
                    archives.add(new ArchivedJson(path, json));
                }
                return archives;
            } catch (NullPointerException npe) {
                // occurs if the archive is empty or any of the expected fields are not present
                throw new IOException(
                        "Job archive (" + file.getPath() + ") did not conform to expected format.");
            }
        }
    }
}
