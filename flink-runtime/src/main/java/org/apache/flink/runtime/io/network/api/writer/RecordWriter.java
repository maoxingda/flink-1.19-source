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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.XORShiftRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * An abstract record-oriented runtime result writer.
 *
 * <p>The RecordWriter wraps the runtime's {@link ResultPartitionWriter} and takes care of
 * subpartition selection and serializing records into bytes.
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 将Task运行的结果通过RecordWriter写出到网络栈
*/
public abstract class RecordWriter<T extends IOReadableWritable> implements AvailabilityProvider {

    /** Default name for the output flush thread, if no name with a task reference is given. */
    @VisibleForTesting
    public static final String DEFAULT_OUTPUT_FLUSH_THREAD_NAME = "OutputFlusher";

    private static final Logger LOG = LoggerFactory.getLogger(RecordWriter.class);
    /** Task运行时用来存储结果的接口 */
    protected final ResultPartitionWriter targetPartition;
    /** 子分区个数*/
    protected final int numberOfSubpartitions;
    /** 一个简单且高效的序列化器，用于 DataOutput 接口 */
    protected final DataOutputSerializer serializer;

    protected final Random rng = new XORShiftRandom();

    protected final boolean flushAlways;

    /** The thread that periodically flushes the output, to give an upper latency bound. */
   /** 定期刷新输出的线程，以提供一个最大的延迟上限。 */
    @Nullable private final OutputFlusher outputFlusher;

    /**
     * To avoid synchronization overhead on the critical path, best-effort error tracking is enough
     * here.
     */
    private Throwable flusherException;

    private volatile Throwable volatileFlusherException;
    private int volatileFlusherExceptionCheckSkipCount;
    private static final int VOLATILE_FLUSHER_EXCEPTION_MAX_CHECK_SKIP_COUNT = 100;

    RecordWriter(ResultPartitionWriter writer, long timeout, String taskName) {
        this.targetPartition = writer;
        this.numberOfSubpartitions = writer.getNumberOfSubpartitions();

        this.serializer = new DataOutputSerializer(128);

        checkArgument(timeout >= ExecutionOptions.DISABLED_NETWORK_BUFFER_TIMEOUT);
        this.flushAlways = (timeout == ExecutionOptions.FLUSH_AFTER_EVERY_RECORD);
        if (timeout == ExecutionOptions.DISABLED_NETWORK_BUFFER_TIMEOUT
                || timeout == ExecutionOptions.FLUSH_AFTER_EVERY_RECORD) {
            outputFlusher = null;
        } else {
            String threadName =
                    taskName == null
                            ? DEFAULT_OUTPUT_FLUSH_THREAD_NAME
                            : DEFAULT_OUTPUT_FLUSH_THREAD_NAME + " for " + taskName;

            outputFlusher = new OutputFlusher(threadName, timeout);
            outputFlusher.start();
        }
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 发送记录到指定的子分区
     *
     * @param record 要发送的记录
     * @param targetSubpartition 目标子分区的索引
     * @throws IOException 如果在发送记录或刷新分区时发生I/O错误
    */
    public void emit(T record, int targetSubpartition) throws IOException {
        // 检查是否有错误发生
        checkErroneous();
        // 序列化记录，并将序列化后的记录和目标子分区索引发送给目标分区
        //流转给ResultPartitionWriter写出
        targetPartition.emitRecord(serializeRecord(serializer, record), targetSubpartition);
        // 如果总是需要刷新（flush），则刷新目标分区的指定子分区
        if (flushAlways) {
            targetPartition.flush(targetSubpartition);
        }
    }

    protected void emit(ByteBuffer record, int targetSubpartition) throws IOException {
        checkErroneous();

        targetPartition.emitRecord(record, targetSubpartition);

        if (flushAlways) {
            targetPartition.flush(targetSubpartition);
        }
    }

    public void broadcastEvent(AbstractEvent event) throws IOException {
        broadcastEvent(event, false);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将给定的事件广播到目标分区，并在需要时刷新所有输出。
     *
     * @param event 要广播的事件对象，必须是AbstractEvent或其子类的实例
     * @param isPriorityEvent 指示该事件是否为优先事件的布尔值
     * @throws IOException 如果在广播事件或刷新输出时发生I/O异常，则抛出此异常
    */
    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        // 将事件广播到目标分区
        targetPartition.broadcastEvent(event, isPriorityEvent);
        // 如果设置了flushAlways为true，则刷新所有输出
        if (flushAlways) {
            flushAll();
        }
    }

    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        targetPartition.alignedBarrierTimeout(checkpointId);
    }

    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        targetPartition.abortCheckpoint(checkpointId, cause);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将给定的 IOReadableWritable 类型的 record 序列化为一个 ByteBuffer
     *
     * @param serializer 用于序列化的 DataOutputSerializer 对象
     * @param record   需要被序列化的 IOReadableWritable 类型的对象
     * @return         序列化后的 ByteBuffer
     * @throws IOException 如果在序列化过程中发生 I/O 错误
    */
    @VisibleForTesting
    public static ByteBuffer serializeRecord(
            DataOutputSerializer serializer, IOReadableWritable record) throws IOException {
        // the initial capacity should be no less than 4 bytes
        //todo
        // 初始的容量应该不小于4个字节，因为我们要先预留4个字节的位置来写入序列化后的数据长度
        // 设置 serializer 的位置为 4，表示从 ByteBuffer 的第 5 个字节开始写入数据（索引从 0 开始）
        serializer.setPositionUnsafe(4);

        // write data
        // 调用 record 对象的 write 方法，将数据写入到 serializer 中
        record.write(serializer);

        // write length
        // 写入序列化后的数据长度（从第5个字节开始到当前位置的长度），到 ByteBuffer 的前4个字节
        // 注意：这里使用了 serializer.length() - 4 来获取实际的数据长度（因为前面预留了4个字节）
        // 第二个参数 0 表示要写入的位置是 ByteBuffer 的起始位置
        serializer.writeIntUnsafe(serializer.length() - 4, 0);
        // 将 serializer 封装为一个 ByteBuffer 并返回
        return serializer.wrapAsByteBuffer();
    }

    public void flushAll() {
        targetPartition.flushAll();
    }

    /** Sets the metric group for this RecordWriter. */
    public void setMetricGroup(TaskIOMetricGroup metrics) {
        targetPartition.setMetricGroup(metrics);
    }

    public int getNumberOfSubpartitions() {
        return numberOfSubpartitions;
    }

    /**
     * Whether the subpartition where an element comes from can be derived from the existing
     * information. If false, the caller of this writer should attach the subpartition information
     * onto an element before writing it to a subpartition, if the element needs this information
     * afterward.
     */
    public boolean isSubpartitionDerivable() {
        return !(targetPartition instanceof ResultPartition
                && ((ResultPartition) targetPartition).isNumberOfPartitionConsumerUndefined());
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return targetPartition.getAvailableFuture();
    }

    /** This is used to send regular records. */
    public abstract void emit(T record) throws IOException;

    /** This is used to send LatencyMarks to a random target subpartition. */
    public void randomEmit(T record) throws IOException {
        checkErroneous();

        int targetSubpartition = rng.nextInt(numberOfSubpartitions);
        emit(record, targetSubpartition);
    }

    /** This is used to broadcast streaming Watermarks in-band with records. */
    public abstract void broadcastEmit(T record) throws IOException;

    /** Closes the writer. This stops the flushing thread (if there is one). */
    public void close() {
        // make sure we terminate the thread in any case
        if (outputFlusher != null) {
            outputFlusher.terminate();
            try {
                outputFlusher.join();
            } catch (InterruptedException e) {
                // ignore on close
                // restore interrupt flag to fast exit further blocking calls
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Notifies the writer that the output flusher thread encountered an exception.
     *
     * @param t The exception to report.
     */
    private void notifyFlusherException(Throwable t) {
        if (flusherException == null) {
            LOG.error("An exception happened while flushing the outputs", t);
            flusherException = t;
            volatileFlusherException = t;
        }
    }

    protected void checkErroneous() throws IOException {
        // For performance reasons, we are not checking volatile field every single time.
        if (flusherException != null
                || (volatileFlusherExceptionCheckSkipCount
                                >= VOLATILE_FLUSHER_EXCEPTION_MAX_CHECK_SKIP_COUNT
                        && volatileFlusherException != null)) {
            throw new IOException(
                    "An exception happened while flushing the outputs", volatileFlusherException);
        }
        if (++volatileFlusherExceptionCheckSkipCount
                >= VOLATILE_FLUSHER_EXCEPTION_MAX_CHECK_SKIP_COUNT) {
            volatileFlusherExceptionCheckSkipCount = 0;
        }
    }

    /** Sets the max overdraft buffer size of per gate. */
    public void setMaxOverdraftBuffersPerGate(int maxOverdraftBuffersPerGate) {
        targetPartition.setMaxOverdraftBuffersPerGate(maxOverdraftBuffersPerGate);
    }

    // ------------------------------------------------------------------------

    /**
     * A dedicated thread that periodically flushes the output buffers, to set upper latency bounds.
     *
     * <p>The thread is daemonic, because it is only a utility thread.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 一个专用线程，用于定期刷新输出缓冲区，以设置最大的延迟上限。
    */
    private class OutputFlusher extends Thread {

        private final long timeout;

        private volatile boolean running = true;

        OutputFlusher(String name, long timeout) {
            super(name);
            setDaemon(true);
            this.timeout = timeout;
        }

        public void terminate() {
            running = false;
            interrupt();
        }

        @Override
        public void run() {
            try {
                while (running) {
                    try {
                        Thread.sleep(timeout);
                    } catch (InterruptedException e) {
                        // propagate this if we are still running, because it should not happen
                        // in that case
                        if (running) {
                            throw new Exception(e);
                        }
                    }

                    // any errors here should let the thread come to a halt and be
                    // recognized by the writer
                    flushAll();
                }
            } catch (Throwable t) {
                notifyFlusherException(t);
            }
        }
    }

    @VisibleForTesting
    ResultPartitionWriter getTargetPartition() {
        return targetPartition;
    }
}
