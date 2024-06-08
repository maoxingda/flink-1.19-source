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
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Utility class for memory operations. */
@Internal
public class MemoryUtils {

    /** The "unsafe", which can be used to perform native memory accesses. */
    @SuppressWarnings({"restriction", "UseOfSunClasses"})
    public static final sun.misc.Unsafe UNSAFE = getUnsafe();

    /** The native byte order of the platform on which the system currently runs. */
    public static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();

    private static final long BUFFER_ADDRESS_FIELD_OFFSET =
            getClassFieldOffset(Buffer.class, "address");
    private static final long BUFFER_CAPACITY_FIELD_OFFSET =
            getClassFieldOffset(Buffer.class, "capacity");
    private static final Class<?> DIRECT_BYTE_BUFFER_CLASS =
            getClassByName("java.nio.DirectByteBuffer");

    @SuppressWarnings("restriction")
    private static sun.misc.Unsafe getUnsafe() {
        try {
            Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            return (sun.misc.Unsafe) unsafeField.get(null);
        } catch (SecurityException e) {
            throw new Error(
                    "Could not access the sun.misc.Unsafe handle, permission denied by security manager.",
                    e);
        } catch (NoSuchFieldException e) {
            throw new Error("The static handle field in sun.misc.Unsafe was not found.", e);
        } catch (IllegalArgumentException e) {
            throw new Error("Bug: Illegal argument reflection access for static field.", e);
        } catch (IllegalAccessException e) {
            throw new Error("Access to sun.misc.Unsafe is forbidden by the runtime.", e);
        } catch (Throwable t) {
            throw new Error(
                    "Unclassified error while trying to access the sun.misc.Unsafe handle.", t);
        }
    }

    private static long getClassFieldOffset(
            @SuppressWarnings("SameParameterValue") Class<?> cl, String fieldName) {
        try {
            return UNSAFE.objectFieldOffset(cl.getDeclaredField(fieldName));
        } catch (SecurityException e) {
            throw new Error(
                    getClassFieldOffsetErrorMessage(cl, fieldName)
                            + ", permission denied by security manager.",
                    e);
        } catch (NoSuchFieldException e) {
            throw new Error(getClassFieldOffsetErrorMessage(cl, fieldName), e);
        } catch (Throwable t) {
            throw new Error(
                    getClassFieldOffsetErrorMessage(cl, fieldName) + ", unclassified error", t);
        }
    }

    private static String getClassFieldOffsetErrorMessage(Class<?> cl, String fieldName) {
        return "Could not get field '"
                + fieldName
                + "' offset in class '"
                + cl
                + "' for unsafe operations";
    }

    private static Class<?> getClassByName(
            @SuppressWarnings("SameParameterValue") String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new Error("Could not find class '" + className + "' for unsafe operations.", e);
        }
    }

    /**
     * Allocates unsafe native memory.
     *
     * @param size size of the unsafe memory to allocate.
     * @return address of the allocated unsafe memory
     */
    static long allocateUnsafe(long size) {
        return UNSAFE.allocateMemory(Math.max(1L, size));
    }

    /**
     * Creates a cleaner to release the unsafe memory.
     *
     * @param address address of the unsafe memory to release
     * @param customCleanup A custom action to clean up GC
     * @return action to run to release the unsafe memory manually
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 创建一个清理器来释放不安全的内存。
     *
     * @param address 要释放的不安全内存的地址
     * @param customCleanup 自定义的GC清理动作
     * @return 一个Runnable对象，手动运行该对象将释放不安全内存并执行自定义清理动作
    */
    static Runnable createMemoryCleaner(long address, Runnable customCleanup) {
        // 返回一个lambda表达式定义的Runnable对象
        return () -> {
            // 调用releaseUnsafe方法来释放指定的不安全内存地址
            releaseUnsafe(address);
            // 执行传入的自定义清理动作
            customCleanup.run();
        };
    }

    private static void releaseUnsafe(long address) {
        UNSAFE.freeMemory(address);
    }

    /**
     * Wraps the unsafe native memory with a {@link ByteBuffer}.
     *
     * @param address address of the unsafe memory to wrap
     * @param size size of the unsafe memory to wrap
     * @return a {@link ByteBuffer} which is a view of the given unsafe memory
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 使用ByteBuffer包装非安全的原生内存。
     *
     * @param address 要包装的非安全内存的地址
     * @param size 要包装的非安全内存的大小
     * @return 一个ByteBuffer，它是给定非安全内存的视图
    */
    static ByteBuffer wrapUnsafeMemoryWithByteBuffer(long address, int size) {
        //noinspection OverlyBroadCatchBlock
        try {
            /**
             * 调用UNSAFE类的allocateInstance方法来创建一个DIRECT_BYTE_BUFFER_CLASS的实例
             * DIRECT_BYTE_BUFFER_CLASS =  java.nio.DirectByteBuffer
             */
            ByteBuffer buffer = (ByteBuffer) UNSAFE.allocateInstance(DIRECT_BYTE_BUFFER_CLASS);
            // 使用UNSAFE类将给定的内存地址设置到ByteBuffer实例的address字段中
            // BUFFER_ADDRESS_FIELD_OFFSET是address字段在ByteBuffer实例中的偏移量
            UNSAFE.putLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET, address);
            // 同样，使用UNSAFE类将给定的内存大小设置到ByteBuffer实例的capacity字段中
            // BUFFER_CAPACITY_FIELD_OFFSET是capacity在ByteBuffer实例中的偏移量
            UNSAFE.putInt(buffer, BUFFER_CAPACITY_FIELD_OFFSET, size);
            // 调用ByteBuffer的clear方法来设置其position为0，limit为capacity
            // 这样ByteBuffer就准备好了，可以被用来访问之前包装的原生内存
            buffer.clear();
            // 返回包装好的ByteBuffer
            return buffer;
        } catch (Throwable t) {
            // 如果在包装过程中发生任何异常，则抛出一个Error，并带上原始异常作为原因
            throw new Error("Failed to wrap unsafe off-heap memory with ByteBuffer", t);
        }
    }

    /**
     * Get native memory address wrapped by the given {@link ByteBuffer}.
     *
     * @param buffer {@link ByteBuffer} which wraps the native memory address to get
     * @return native memory address wrapped by the given {@link ByteBuffer}
     */
    static long getByteBufferAddress(ByteBuffer buffer) {
        Preconditions.checkNotNull(buffer, "buffer is null");
        Preconditions.checkArgument(
                buffer.isDirect(), "Can't get address of a non-direct ByteBuffer.");

        long offHeapAddress;
        try {
            offHeapAddress = UNSAFE.getLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET);
        } catch (Throwable t) {
            throw new Error("Could not access direct byte buffer address field.", t);
        }

        Preconditions.checkState(offHeapAddress > 0, "negative pointer or size");
        Preconditions.checkState(
                offHeapAddress < Long.MAX_VALUE - Integer.MAX_VALUE,
                "Segment initialized with too large address: "
                        + offHeapAddress
                        + " ; Max allowed address is "
                        + (Long.MAX_VALUE - Integer.MAX_VALUE - 1));

        return offHeapAddress;
    }

    /** Should not be instantiated. */
    private MemoryUtils() {}
}
