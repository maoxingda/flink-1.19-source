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

package org.apache.flink.runtime.io.disk;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.runtime.memory.MemoryManager;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link org.apache.flink.core.memory.DataOutputView} that is backed by a {@link
 * BlockChannelWriter}, making it effectively a data output stream. The view writes it data in
 * blocks to the underlying channel.
 */
public class FileChannelOutputView extends AbstractPagedOutputView {

    private final BlockChannelWriter<MemorySegment> writer; // the writer to the channel

    private final MemoryManager memManager;

    private final List<MemorySegment> memory;

    private int numBlocksWritten;

    private int bytesInLatestSegment;

    // --------------------------------------------------------------------------------------------

    public FileChannelOutputView(
            BlockChannelWriter<MemorySegment> writer,
            MemoryManager memManager,
            List<MemorySegment> memory,
            int segmentSize)
            throws IOException {
        super(segmentSize, 0);

        checkNotNull(writer);
        checkNotNull(memManager);
        checkNotNull(memory);
        checkArgument(!writer.isClosed());

        this.writer = writer;
        this.memManager = memManager;
        this.memory = memory;

        for (MemorySegment next : memory) {
            writer.getReturnQueue().add(next);
        }

        // move to the first page
        advance();
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Closes this output, writing pending data and releasing the memory.
     *
     * @throws IOException Thrown, if the pending data could not be written.
     */
    public void close() throws IOException {
        close(false);
    }

    /**
     * Closes this output, writing pending data and releasing the memory.
     *
     * @throws IOException Thrown, if the pending data could not be written.
     */
    public void closeAndDelete() throws IOException {
        close(true);
    }

    private void close(boolean delete) throws IOException {
        try {
            // send off set last segment, if we have not been closed before
            MemorySegment current = getCurrentSegment();
            if (current != null) {
                writeSegment(current, getCurrentPositionInSegment());
            }

            clear();
            if (delete) {
                writer.closeAndDelete();
            } else {
                writer.close();
            }
        } finally {
            memManager.release(memory);
        }
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Gets the number of blocks written by this output view.
     *
     * @return The number of blocks written by this output view.
     */
    public int getBlockCount() {
        return numBlocksWritten;
    }

    /**
     * Gets the number of bytes written in the latest memory segment.
     *
     * @return The number of bytes written in the latest memory segment.
     */
    public int getBytesInLatestSegment() {
        return bytesInLatestSegment;
    }

    public long getWriteOffset() {
        return ((long) numBlocksWritten) * segmentSize + getCurrentPositionInSegment();
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 重写方法，用于获取下一个内存段或磁盘上的块
     *
     * @param current 当前正在处理的内存段，可能为null
     * @param posInSegment 当前内存段中的位置（此方法内部可能根据需求决定是否使用此参数）
     * @return 下一个可用的内存段或磁盘上的块
     * @throws IOException 如果在写入或获取内存段时发生I/O错误
    */
    @Override
    protected MemorySegment nextSegment(MemorySegment current, int posInSegment)
            throws IOException {
        // 如果当前内存段不为null，则将其写入（可能是写入到磁盘或执行其他操作）
        if (current != null) {
            writeSegment(current, posInSegment);
        }
        // 从写入器（可能是磁盘写入器）中获取下一个返回的块
        // 假设writer已经在类的其他部分被正确初始化并指向了一个有效的写入器
        return writer.getNextReturnedBlock();
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将内存段写入到目标位置（可能是磁盘或其他存储介质）
     *
     * @param segment 要写入的内存段
     * @param writePosition 写入时的位置（此方法内部可能不直接使用此参数，因为它取决于具体的写入逻辑）
     * @throws IOException 如果在写入过程中发生I/O错误
    */
    private void writeSegment(MemorySegment segment, int writePosition) throws IOException {
        // 将内存段写入到写入器（可能是磁盘写入器）中
        writer.writeBlock(segment);
        // 增加已写入的块计数
        numBlocksWritten++;
        // 更新最新写入段中的字节数（这里假设bytesInLatestSegment是类的一个成员变量）
        bytesInLatestSegment = writePosition;
    }
}
