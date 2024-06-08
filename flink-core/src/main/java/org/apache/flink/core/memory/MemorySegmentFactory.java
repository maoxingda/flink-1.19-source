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

package org.apache.flink.core.memory;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TaskManagerExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A factory for memory segments ({@link MemorySegment}). */
@Internal
public final class MemorySegmentFactory {
    private static final Logger LOG = LoggerFactory.getLogger(MemorySegmentFactory.class);
    private static final Runnable NO_OP = () -> {};

    /**
     * Creates a new memory segment that targets the given heap memory region.
     *
     * <p>This method should be used to turn short lived byte arrays into memory segments.
     *
     * @param buffer The heap memory region.
     * @return A new memory segment that targets the given heap memory region.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建一个以给定堆内存区域为目标的新内存段
    */
    public static MemorySegment wrap(byte[] buffer) {
        return new MemorySegment(buffer, null);
    }

    /**
     * Copies the given heap memory region and creates a new memory segment wrapping it.
     *
     * @param bytes The heap memory region.
     * @param start starting position, inclusive
     * @param end end position, exclusive
     * @return A new memory segment that targets a copy of the given heap memory region.
     * @throws IllegalArgumentException if start > end or end > bytes.length
     */
    public static MemorySegment wrapCopy(byte[] bytes, int start, int end)
            throws IllegalArgumentException {
        checkArgument(end >= start);
        checkArgument(end <= bytes.length);
        MemorySegment copy = allocateUnpooledSegment(end - start);
        copy.put(0, bytes, start, copy.size());
        return copy;
    }

    /**
     * Wraps the four bytes representing the given number with a {@link MemorySegment}.
     *
     * @see ByteBuffer#putInt(int)
     */
    public static MemorySegment wrapInt(int value) {
        return wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
    }

    /**
     * Allocates some unpooled memory and creates a new memory segment that represents that memory.
     *
     * <p>This method is similar to {@link #allocateUnpooledSegment(int, Object)}, but the memory
     * segment will have null as the owner.
     *
     * @param size The size of the memory segment to allocate.
     * @return A new memory segment, backed by unpooled heap memory.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 分配一些堆外内存，并创建一个代表该内存的新内存段
    */
    public static MemorySegment allocateUnpooledSegment(int size) {
        return allocateUnpooledSegment(size, null);
    }

    /**
     * Allocates some unpooled memory and creates a new memory segment that represents that memory.
     *
     * <p>This method is similar to {@link #allocateUnpooledSegment(int)}, but additionally sets the
     * owner of the memory segment.
     *
     * @param size The size of the memory segment to allocate.
     * @param owner The owner to associate with the memory segment.
     * @return A new memory segment, backed by unpooled heap memory.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 分配一些堆外内存，并创建一个代表该内存的新内存段
    */
    public static MemorySegment allocateUnpooledSegment(int size, Object owner) {
        return new MemorySegment(new byte[size], owner);
    }

    /**
     * Allocates some unpooled off-heap memory and creates a new memory segment that represents that
     * memory.
     *
     * @param size The size of the off-heap memory segment to allocate.
     * @return A new memory segment, backed by unpooled off-heap memory.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 分配一些堆外内存，并创建一个表示该内存的新内存段。
    */
    public static MemorySegment allocateUnpooledOffHeapMemory(int size) {
        return allocateUnpooledOffHeapMemory(size, null);
    }

    /**
     * Allocates some unpooled off-heap memory and creates a new memory segment that represents that
     * memory.
     *
     * @param size The size of the off-heap memory segment to allocate.
     * @param owner The owner to associate with the off-heap memory segment.
     * @return A new memory segment, backed by unpooled off-heap memory.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 分配一些堆外内存，并创建一个表示该内存的新内存段。
    */
    public static MemorySegment allocateUnpooledOffHeapMemory(int size, Object owner) {
        // todo 申请直接内存
        ByteBuffer memory = allocateDirectMemory(size);
        // todo 构造 MemorySegment
        return new MemorySegment(memory, owner);
    }

    @VisibleForTesting
    public static MemorySegment allocateOffHeapUnsafeMemory(int size) {
        return allocateOffHeapUnsafeMemory(size, null, NO_OP);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 分配指定大小的直接内存（非JVM堆内存）并返回其对应的ByteBuffer对象。
     * @param size 需要分配的直接内存大小，以字节为单位
     * @return 分配的直接内存对应的ByteBuffer对象
     * @throws Exception 如果在分配内存时发生OutOfMemoryError异常，
    */
    private static ByteBuffer allocateDirectMemory(int size) {
        //noinspection ErrorNotRethrown
        // 使用try-catch块捕获OutOfMemoryError异常，以避免直接崩溃

        try {
            // todo 分配指定大小的直接内存，并返回其对应的ByteBuffer对象
            return ByteBuffer.allocateDirect(size);
        } catch (OutOfMemoryError outOfMemoryError) {
            //抛出异常
            // TODO: this error handling can be removed in future,
            // once we find a common way to handle OOM errors in netty threads.
            // Here we enrich it to propagate better OOM message to the receiver
            // if it happens in a netty thread.
            TaskManagerExceptionUtils.tryEnrichTaskManagerError(outOfMemoryError);
            if (ExceptionUtils.isDirectOutOfMemoryError(outOfMemoryError)) {
                LOG.error("Cannot allocate direct memory segment", outOfMemoryError);
            }

            ExceptionUtils.rethrow(outOfMemoryError);
            return null;
        }
    }

    /**
     * Allocates an off-heap unsafe memory and creates a new memory segment to represent that
     * memory.
     *
     * <p>Creation of this segment schedules its memory freeing operation when its java wrapping
     * object is about to be garbage collected, similar to {@link
     * java.nio.DirectByteBuffer#DirectByteBuffer(int)}. The difference is that this memory
     * allocation is out of option -XX:MaxDirectMemorySize limitation.
     *
     * @param size The size of the off-heap unsafe memory segment to allocate.
     * @param owner The owner to associate with the off-heap unsafe memory segment.
     * @param customCleanupAction A custom action to run upon calling GC cleaner.
     * @return A new memory segment, backed by off-heap unsafe memory.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 为非堆外不安全的内存分配空间，并创建一个新的内存段来表示这段内存。
     *
     * <p>创建这个内存段时，会安排在其Java包装对象即将被垃圾回收时释放其内存的操作，
     * 这与 {@link java.nio.DirectByteBuffer#DirectByteBuffer(int)} 类似。但区别在于，
     * 这种内存分配不受JVM启动参数 -XX:MaxDirectMemorySize 的限制。
     *
     * @param size 要分配的非堆外不安全内存段的大小。
     * @param owner 与非堆外不安全内存段关联的所有者对象。
     * @param customCleanupAction 在调用GC清理器时运行的自定义清理动作。
     * @return 一个新的内存段，由非堆外不安全内存支持。
    */
    public static MemorySegment allocateOffHeapUnsafeMemory(
            int size, Object owner, Runnable customCleanupAction) {
        // 使用非安全方式分配指定大小的内存，并返回其内存地址
        long address = MemoryUtils.allocateUnsafe(size);
        // 使用上一步获得的内存地址和大小，将其封装为一个ByteBuffer对象
        ByteBuffer offHeapBuffer = MemoryUtils.wrapUnsafeMemoryWithByteBuffer(address, size);
        // 创建一个清理器，当GC回收时，会根据提供的地址和自定义清理动作来释放内存
        Runnable cleaner = MemoryUtils.createMemoryCleaner(address, customCleanupAction);
        // 创建一个新的MemorySegment对象，该对象由offHeapBuffer支持，
        // 并关联给定的owner、指定该内存段不是可映射的，以及分配清理器
        return new MemorySegment(offHeapBuffer, owner, false, cleaner);
    }

    /**
     * Creates a memory segment that wraps the off-heap memory backing the given ByteBuffer. Note
     * that the ByteBuffer needs to be a <i>direct ByteBuffer</i>.
     *
     * <p>This method is intended to be used for components which pool memory and create memory
     * segments around long-lived memory regions.
     *
     * @param memory The byte buffer with the off-heap memory to be represented by the memory
     *     segment.
     * @return A new memory segment representing the given off-heap memory.
     */
    public static MemorySegment wrapOffHeapMemory(ByteBuffer memory) {
        return new MemorySegment(memory, null);
    }
}
