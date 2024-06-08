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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.PartitionRequestListener;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Simple wrapper for the subpartition view used in the new network credit-based mode.
 *
 * <p>It also keeps track of available buffers and notifies the outbound handler about
 * non-emptiness, similar to the {@link LocalInputChannel}.
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 在新的基于网络信用的模式中使用的子分区视图的简单包装器。
*/
class CreditBasedSequenceNumberingViewReader
        implements BufferAvailabilityListener, NetworkSequenceViewReader {

    private final Object requestLock = new Object();

    private final InputChannelID receiverId;

    private final PartitionRequestQueue requestQueue;

    private final int initialCredit;

    /**
     * Cache of the index of the only subpartition if the underlining {@link ResultSubpartitionView}
     * only consumes one subpartition, or -1 otherwise.
     */
    private int subpartitionId;

    private volatile ResultSubpartitionView subpartitionView;

    private volatile PartitionRequestListener partitionRequestListener;

    /**
     * The status indicating whether this reader is already enqueued in the pipeline for
     * transferring data or not.
     *
     * <p>It is mainly used to avoid repeated registrations but should be accessed by a single
     * thread only since there is no synchronisation.
     */
    private boolean isRegisteredAsAvailable = false;

    /** The number of available buffers for holding data on the consumer side. */
    private int numCreditsAvailable;

    CreditBasedSequenceNumberingViewReader(
            InputChannelID receiverId, int initialCredit, PartitionRequestQueue requestQueue) {
        checkArgument(initialCredit >= 0, "Must be non-negative.");

        this.receiverId = receiverId;
        this.initialCredit = initialCredit;
        this.numCreditsAvailable = initialCredit;
        this.requestQueue = requestQueue;
        this.subpartitionId = -1;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * @param partitionProvider 提供ResultPartition的提供者
     * @param resultPartitionId 要请求的ResultPartition的唯一标识符
     * @param subpartitionIndexSet 要请求的ResultSubpartition的索引集合
    */
    @Override
    public void requestSubpartitionViewOrRegisterListener(
            ResultPartitionProvider partitionProvider,
            ResultPartitionID resultPartitionId,
            ResultSubpartitionIndexSet subpartitionIndexSet)
            throws IOException {
        // 使用同步锁来确保同时只有一个线程可以执行此操作
        synchronized (requestLock) {
            // 检查是否已经请求了SubpartitionView，如果已经请求过则抛出异常
            checkState(subpartitionView == null, "Subpartitions already requested");
            // 检查是否已经创建了PartitionRequestListener，如果已经创建过则抛出异常
            checkState(
                    partitionRequestListener == null, "Partition request listener already created");
            // 创建一个新的NettyPartitionRequestListener对象，它将作为Partition的请求监听器
            // 并将接收到的Partition请求转发给ResultPartitionManager
            partitionRequestListener =
                    new NettyPartitionRequestListener(
                            partitionProvider, this, subpartitionIndexSet, resultPartitionId);
            // The partition provider will create subpartitionView if resultPartition is
            // registered, otherwise it will register a listener of partition request to the result
            // partition manager.
            // 调用partitionProvider的createSubpartitionViewOrRegisterListener方法
            // 如果ResultPartition已注册，则它将创建并返回ResultSubpartitionView；
            // 否则，它将向ResultPartitionManager注册partitionRequestListener以监听Partition请求
            Optional<ResultSubpartitionView> subpartitionViewOptional =
                    partitionProvider.createSubpartitionViewOrRegisterListener(
                            resultPartitionId,
                            subpartitionIndexSet,
                            this,
                            partitionRequestListener);
            // 如果返回了ResultSubpartitionView，则设置到当前实例的subpartitionView字段
            if (subpartitionViewOptional.isPresent()) {
                this.subpartitionView = subpartitionViewOptional.get();
                // 如果只需要一个子分区，则获取该子分区的索引并设置到subpartitionId字段
                if (subpartitionIndexSet.size() == 1) {
                    subpartitionId = subpartitionIndexSet.values().iterator().next();
                }
            } else {
                // If the subpartitionView is not exist, it means that the requested partition is
                // not registered.
                // 如果没有返回ResultSubpartitionView，则表示请求的分区尚未注册
                // 此时不需要进行任何操作，直接返回即可
                return;
            }
        }
        // 通知其他可能等待此SubpartitionView的组件，数据已经可用
        notifyDataAvailable(subpartitionView);
        // 通知请求队列，一个新的读取器（可能是本实例）已经被创建
        // 这可以允许队列进行一些优化，比如停止不必要的等待或其他操作  
        requestQueue.notifyReaderCreated(this);
    }
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 当子分区被创建时，通知当前实例相关的子分区视图已经准备就绪。
     *
     * @param partition 相关的结果分区
     * @param subpartitionIndexSet 子分区索引集合
     * @throws IOException 如果在创建子分区视图或通知过程中发生I/O错误
     * @throws IllegalStateException 如果子分区视图已经存在
    */
    @Override
    public void notifySubpartitionsCreated(
            ResultPartition partition, ResultSubpartitionIndexSet subpartitionIndexSet)
            throws IOException {
        // 使用requestLock作为锁，确保线程安全
        synchronized (requestLock) {
            // 检查是否已经存在子分区视图，如果存在则抛出异常
            checkState(subpartitionView == null, "Subpartitions already requested");
            // 调用结果分区的createSubpartitionView方法来创建子分区视图
            subpartitionView = partition.createSubpartitionView(subpartitionIndexSet, this);
            // 如果子分区索引集合只包含一个元素，则获取该子分区的ID
            if (subpartitionIndexSet.size() == 1) {
                subpartitionId = subpartitionIndexSet.values().iterator().next();
            }
        }
        // 通知数据已经可用
        notifyDataAvailable(subpartitionView);
        // 通知请求队列当前读取器已经创建，以便进行后续的请求处理
        requestQueue.notifyReaderCreated(this);
    }

    @Override
    public void addCredit(int creditDeltas) {
        numCreditsAvailable += creditDeltas;
    }

    @Override
    public void notifyRequiredSegmentId(int subpartitionId, int segmentId) {
        subpartitionView.notifyRequiredSegmentId(subpartitionId, segmentId);
    }

    @Override
    public void resumeConsumption() {
        if (initialCredit == 0) {
            // reset available credit if no exclusive buffer is available at the
            // consumer side for all floating buffers must have been released
            numCreditsAvailable = 0;
        }
        subpartitionView.resumeConsumption();
    }

    @Override
    public void acknowledgeAllRecordsProcessed() {
        subpartitionView.acknowledgeAllDataProcessed();
    }

    @Override
    public void setRegisteredAsAvailable(boolean isRegisteredAvailable) {
        this.isRegisteredAsAvailable = isRegisteredAvailable;
    }

    @Override
    public boolean isRegisteredAsAvailable() {
        return isRegisteredAsAvailable;
    }

    /**
     * Returns true only if the next buffer is an event or the reader has both available credits and
     * buffers.
     *
     * @implSpec BEWARE: this must be in sync with {@link #getNextDataType(BufferAndBacklog)}, such
     *     that {@code getNextDataType(bufferAndBacklog) != NONE <=>
     *     AvailabilityWithBacklog#isAvailable()}!
     */
    @Override
    public ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog() {
        return subpartitionView.getAvailabilityAndBacklog(numCreditsAvailable > 0);
    }

    /**
     * Returns the {@link org.apache.flink.runtime.io.network.buffer.Buffer.DataType} of the next
     * buffer in line.
     *
     * <p>Returns the next data type only if the next buffer is an event or the reader has both
     * available credits and buffers.
     *
     * @implSpec BEWARE: this must be in sync with {@link #getAvailabilityAndBacklog()}, such that
     *     {@code getNextDataType(bufferAndBacklog) != NONE <=>
     *     AvailabilityWithBacklog#isAvailable()}!
     * @param bufferAndBacklog current buffer and backlog including information about the next
     *     buffer
     * @return the next data type if the next buffer can be pulled immediately or {@link
     *     Buffer.DataType#NONE}
     */
    private Buffer.DataType getNextDataType(BufferAndBacklog bufferAndBacklog) {
        final Buffer.DataType nextDataType = bufferAndBacklog.getNextDataType();
        if (numCreditsAvailable > 0 || nextDataType.isEvent()) {
            return nextDataType;
        }
        return Buffer.DataType.NONE;
    }

    @Override
    public InputChannelID getReceiverId() {
        return receiverId;
    }

    @Override
    public void notifyNewBufferSize(int newBufferSize) {
        subpartitionView.notifyNewBufferSize(newBufferSize);
    }

    @Override
    public void notifyPartitionRequestTimeout(PartitionRequestListener partitionRequestListener) {
        requestQueue.notifyPartitionRequestTimeout(partitionRequestListener);
        this.partitionRequestListener = null;
    }

    @VisibleForTesting
    int getNumCreditsAvailable() {
        return numCreditsAvailable;
    }

    @VisibleForTesting
    ResultSubpartitionView.AvailabilityWithBacklog hasBuffersAvailable() {
        return subpartitionView.getAvailabilityAndBacklog(true);
    }

    @Override
    public int peekNextBufferSubpartitionId() throws IOException {
        if (subpartitionId >= 0) {
            return subpartitionId;
        }
        return subpartitionView.peekNextBufferSubpartitionId();
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 从子分区视图中获取下一个可用的缓冲区及其相关信息。
     *
     * @return 包含缓冲区、数据类型、积压缓冲区数量和序列号的BufferAndAvailability对象，
     *         如果没有可用的缓冲区则返回null。
     * @throws IOException 如果在获取缓冲区过程中发生I/O错误
     * @throws IllegalStateException 如果没有足够的信用额度（credits）来接收缓冲区
    */
    @Nullable
    @Override
    public BufferAndAvailability getNextBuffer() throws IOException {
        // 从子分区视图中获取下一个缓冲区及其积压缓冲区数量
        BufferAndBacklog next = subpartitionView.getNextBuffer();
        if (next != null) {
            // 如果下一个缓冲区是一个真正的缓冲区（而不是元数据或其他），并且当前可用的信用额度减一后小于0
            if (next.buffer().isBuffer() && --numCreditsAvailable < 0) {
                // 抛出异常，因为没有足够的信用额度来接收更多的缓冲区
                throw new IllegalStateException("no credit available");
            }
            // 获取下一个缓冲区的数据类型
            final Buffer.DataType nextDataType = getNextDataType(next);
            // 创建一个新的BufferAndAvailability对象，包含缓冲区、数据类型、积压缓冲区数量和序列号
            // 然后返回这个对象
            return new BufferAndAvailability(
                    next.buffer(), nextDataType, next.buffersInBacklog(), next.getSequenceNumber());
        } else {
            // 如果没有可用的缓冲区，则返回null
            return null;
        }
    }

    @Override
    public boolean needAnnounceBacklog() {
        return initialCredit == 0 && numCreditsAvailable == 0;
    }

    @Override
    public boolean isReleased() {
        return subpartitionView.isReleased();
    }

    @Override
    public Throwable getFailureCause() {
        return subpartitionView.getFailureCause();
    }

    @Override
    public void releaseAllResources() throws IOException {
        if (partitionRequestListener != null) {
            partitionRequestListener.releaseListener();
        }
        subpartitionView.releaseAllResources();
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 通知特定视图数据可用。
     *
     * 当有数据可以被特定视图 {@link ResultSubpartitionView} 读取时，调用此方法以通知其数据已经可用。
     * 通知是通过 {@link RequestQueue} 进行的，告诉请求队列该视图的数据已经准备就绪。
     *
     * @param view 通知数据可用的特定视图
    */
    @Override
    public void notifyDataAvailable(ResultSubpartitionView view) {
        requestQueue.notifyReaderNonEmpty(this);
    }

    @Override
    public void notifyPriorityEvent(int prioritySequenceNumber) {
        notifyDataAvailable(this.subpartitionView);
    }

    @Override
    public String toString() {
        return "CreditBasedSequenceNumberingViewReader{"
                + "requestLock="
                + requestLock
                + ", receiverId="
                + receiverId
                + ", numCreditsAvailable="
                + numCreditsAvailable
                + ", isRegisteredAsAvailable="
                + isRegisteredAsAvailable
                + '}';
    }
}
