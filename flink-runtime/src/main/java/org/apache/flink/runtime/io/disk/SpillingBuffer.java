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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentSource;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.HeaderlessChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** An output view that buffers written data in memory pages and spills them when they are full. */
public class SpillingBuffer extends AbstractPagedOutputView {

    private final ArrayList<MemorySegment> fullSegments;

    private final MemorySegmentSource memorySource;

    private BlockChannelWriter<MemorySegment> writer;

    private RandomAccessInputView inMemInView;

    private HeaderlessChannelReaderInputView externalInView;

    private final IOManager ioManager;

    private int blockCount;

    private int numBytesInLastSegment;

    private int numMemorySegmentsInWriter;

    public SpillingBuffer(IOManager ioManager, MemorySegmentSource memSource, int segmentSize) {
        super(memSource.nextSegment(), segmentSize, 0);

        this.fullSegments = new ArrayList<MemorySegment>(16);
        this.memorySource = memSource;
        this.ioManager = ioManager;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 获取下一个内存段或磁盘上的块
     *
     * @param current 当前正在使用的内存段
     * @param positionInCurrent 当前内存段中的位置（此参数在此方法中可能未使用）
     * @return 下一个可用的内存段或磁盘上的块
     * @throws IOException 如果在写入或获取内存段时发生I/O错误
    */
    @Override
    protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent)
            throws IOException {
        // check if we are still in memory
        // 检查是否仍在内存中处理
        if (this.writer == null) {
            // 将当前内存段添加到已满的段列表中
            this.fullSegments.add(current);
            // 从内存源中获取下一个内存段
            final MemorySegment nextSeg = this.memorySource.nextSegment();
            if (nextSeg != null) {
                // 如果存在下一个内存段，则返回它
                return nextSeg;
            } else {
                // out of memory, need to spill: create a writer
                // 内存不足，需要溢出到磁盘：创建一个写入器
                this.writer =
                        this.ioManager.createBlockChannelWriter(this.ioManager.createChannel());

                // add all segments to the writer
                // 将所有已满的段添加到写入器中
                this.blockCount = this.fullSegments.size();
                this.numMemorySegmentsInWriter = this.blockCount;
                for (int i = 0; i < this.fullSegments.size(); i++) {
                    this.writer.writeBlock(this.fullSegments.get(i));
                }
                // 清空已满的段列表
                this.fullSegments.clear();
                // 从写入器中获取第一个返回的块
                final MemorySegment seg = this.writer.getNextReturnedBlock();
                // 更新写入器中的内存段数量
                this.numMemorySegmentsInWriter--;
                // 返回该块
                return seg;
            }
        } else {
            // spilling
            // 正在溢出到磁盘
            // 将当前内存段写入到磁盘的写入器中
            this.writer.writeBlock(current);
            // 更新总的块计数
            this.blockCount++;
            // 从写入器中获取下一个返回的块
            return this.writer.getNextReturnedBlock();
        }
    }

    public DataInputView flip() throws IOException {
        // check whether this is the first flip and we need to add the current segment to the full
        // ones
        if (getCurrentSegment() != null) {
            // first flip
            if (this.writer == null) {
                // in memory
                this.fullSegments.add(getCurrentSegment());
                this.numBytesInLastSegment = getCurrentPositionInSegment();
                this.inMemInView =
                        new RandomAccessInputView(
                                this.fullSegments, this.segmentSize, this.numBytesInLastSegment);
            } else {
                // external: write the last segment and collect the memory back
                this.writer.writeBlock(this.getCurrentSegment());
                this.numMemorySegmentsInWriter++;

                this.numBytesInLastSegment = getCurrentPositionInSegment();
                this.blockCount++;
                this.writer.close();
                for (int i = this.numMemorySegmentsInWriter; i > 0; i--) {
                    this.fullSegments.add(this.writer.getNextReturnedBlock());
                }
                this.numMemorySegmentsInWriter = 0;
            }

            // make sure we cannot write more
            clear();
        }

        if (this.writer == null) {
            // in memory
            this.inMemInView.setReadPosition(0);
            return this.inMemInView;
        } else {
            // recollect memory from a previous view
            if (this.externalInView != null) {
                this.externalInView.close();
            }

            final BlockChannelReader<MemorySegment> reader =
                    this.ioManager.createBlockChannelReader(this.writer.getChannelID());
            this.externalInView =
                    new HeaderlessChannelReaderInputView(
                            reader,
                            this.fullSegments,
                            this.blockCount,
                            this.numBytesInLastSegment,
                            false);
            return this.externalInView;
        }
    }

    /**
     * @return A list with all memory segments that have been taken from the memory segment source.
     */
    public List<MemorySegment> close() throws IOException {
        final ArrayList<MemorySegment> segments =
                new ArrayList<MemorySegment>(
                        this.fullSegments.size() + this.numMemorySegmentsInWriter);

        // if the buffer is still being written, clean that up
        if (getCurrentSegment() != null) {
            segments.add(getCurrentSegment());
            clear();
        }

        moveAll(this.fullSegments, segments);
        this.fullSegments.clear();

        // clean up the writer
        if (this.writer != null) {
            // closing before the first flip, collect the memory in the writer
            this.writer.close();
            for (int i = this.numMemorySegmentsInWriter; i > 0; i--) {
                segments.add(this.writer.getNextReturnedBlock());
            }
            this.writer.closeAndDelete();
            this.writer = null;
        }

        // clean up the views
        if (this.inMemInView != null) {
            this.inMemInView = null;
        }
        if (this.externalInView != null) {
            if (!this.externalInView.isClosed()) {
                this.externalInView.close();
            }
            this.externalInView = null;
        }
        return segments;
    }

    /**
     * Utility method that moves elements. It avoids copying the data into a dedicated array first,
     * as the {@link ArrayList#addAll(java.util.Collection)} method does.
     *
     * @param <E>
     * @param source
     * @param target
     */
    private static final <E> void moveAll(ArrayList<E> source, ArrayList<E> target) {
        target.ensureCapacity(target.size() + source.size());
        for (int i = source.size() - 1; i >= 0; i--) {
            target.add(source.remove(i));
        }
    }
}
