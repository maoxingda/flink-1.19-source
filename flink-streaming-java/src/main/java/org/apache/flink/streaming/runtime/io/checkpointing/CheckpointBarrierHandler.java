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

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointableTask;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.concurrent.FutureUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@link CheckpointBarrierHandler} reacts to checkpoint barrier arriving from the input
 * channels. Different implementations may either simply track barriers, or block certain inputs on
 * barriers.
 */
public abstract class CheckpointBarrierHandler implements Closeable {
    private static final long OUTSIDE_OF_ALIGNMENT = Long.MIN_VALUE;

    /** The listener to be notified on complete checkpoints. */
    private final CheckpointableTask toNotifyOnCheckpoint;

    private final Clock clock;

    /** The time (in nanoseconds) that the latest alignment took. */
    private CompletableFuture<Long> latestAlignmentDurationNanos = new CompletableFuture<>();

    /**
     * The time (in nanoseconds) between creation of the checkpoint's first checkpoint barrier and
     * receiving it by this task.
     */
    private long latestCheckpointStartDelayNanos;

    /** The timestamp as in {@link System#nanoTime()} at which the last alignment started. */
    private long startOfAlignmentTimestamp = OUTSIDE_OF_ALIGNMENT;

    /** ID of checkpoint for which alignment was started last. */
    private long startAlignmentCheckpointId = -1;

    /**
     * Cumulative counter of bytes processed during alignment. Once we complete alignment, we will
     * put this value into the {@link #latestBytesProcessedDuringAlignment}.
     */
    private long bytesProcessedDuringAlignment;

    private CompletableFuture<Long> latestBytesProcessedDuringAlignment = new CompletableFuture<>();

    private final boolean enableCheckpointAfterTasksFinished;

    public CheckpointBarrierHandler(
            CheckpointableTask toNotifyOnCheckpoint,
            Clock clock,
            boolean enableCheckpointAfterTasksFinished) {
        this.toNotifyOnCheckpoint = checkNotNull(toNotifyOnCheckpoint);
        this.clock = checkNotNull(clock);
        this.enableCheckpointAfterTasksFinished = enableCheckpointAfterTasksFinished;
    }

    boolean isCheckpointAfterTasksFinishedEnabled() {
        return enableCheckpointAfterTasksFinished;
    }

    @Override
    public void close() throws IOException {}

    public abstract void processBarrier(
            CheckpointBarrier receivedBarrier, InputChannelInfo channelInfo, boolean isRpcTriggered)
            throws IOException;

    public abstract void processBarrierAnnouncement(
            CheckpointBarrier announcedBarrier, int sequenceNumber, InputChannelInfo channelInfo)
            throws IOException;

    public abstract void processCancellationBarrier(
            CancelCheckpointMarker cancelBarrier, InputChannelInfo channelInfo) throws IOException;

    public abstract void processEndOfPartition(InputChannelInfo channelInfo) throws IOException;

    public abstract long getLatestCheckpointId();

    public long getAlignmentDurationNanos() {
        if (isDuringAlignment()) {
            return clock.relativeTimeNanos() - startOfAlignmentTimestamp;
        } else {
            return FutureUtils.getOrDefault(latestAlignmentDurationNanos, 0L);
        }
    }

    public long getCheckpointStartDelayNanos() {
        return latestCheckpointStartDelayNanos;
    }

    public CompletableFuture<Void> getAllBarriersReceivedFuture(long checkpointId) {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *
     *
    */
    protected void notifyCheckpoint(CheckpointBarrier checkpointBarrier) throws IOException {
        // 创建一个CheckpointMetaData对象，用于存储检查点的元数据
        // 元数据包括检查点ID、检查点时间戳以及当前系统时间（毫秒）
        CheckpointMetaData checkpointMetaData =
                new CheckpointMetaData(
                        checkpointBarrier.getId(),
                        checkpointBarrier.getTimestamp(),
                        System.currentTimeMillis());
        // 创建一个CheckpointMetricsBuilder对象，用于构建检查点的度量指标
        CheckpointMetricsBuilder checkpointMetrics;
        // 判断当前检查点ID是否与开始对齐的检查点ID相同
        if (checkpointBarrier.getId() == startAlignmentCheckpointId) {
            // 如果是，则设置对齐相关的度量指标
            // 包括对齐持续时间、对齐期间处理的字节数以及检查点开始延迟
            checkpointMetrics =
                    new CheckpointMetricsBuilder()
                            .setAlignmentDurationNanos(latestAlignmentDurationNanos)
                            .setBytesProcessedDuringAlignment(latestBytesProcessedDuringAlignment)
                            .setCheckpointStartDelayNanos(latestCheckpointStartDelayNanos);
        } else {
            // 如果不是，则设置默认的对齐度量指标（通常为0）
            // 因为这些指标与当前检查点无关
            checkpointMetrics =
                    new CheckpointMetricsBuilder()
                            .setAlignmentDurationNanos(0L)
                            .setBytesProcessedDuringAlignment(0L)
                            .setCheckpointStartDelayNanos(0);
        }
        // 触发检查点通知，将检查点元数据、检查点选项以及检查点度量指标传递给toNotifyOnCheckpoint对象
        // toNotifyOnCheckpoint可能是一个用于处理检查点事件的监听器或回调接口
        toNotifyOnCheckpoint.triggerCheckpointOnBarrier(
                checkpointMetaData, checkpointBarrier.getCheckpointOptions(), checkpointMetrics);
    }

    protected void notifyAbortOnCancellationBarrier(long checkpointId) throws IOException {
        notifyAbort(
                checkpointId,
                new CheckpointException(
                        CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER));
    }

    protected void notifyAbort(long checkpointId, CheckpointException cause) throws IOException {
        toNotifyOnCheckpoint.abortCheckpointOnBarrier(checkpointId, cause);
    }


    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *
     *
    */
    protected void markAlignmentStartAndEnd(long checkpointId, long checkpointCreationTimestamp) {
        // 标记检查点对齐的开始，使用给定的检查点ID和创建时间戳
        // 通常在所有需要的通道都接收到检查点屏障时调用
        markAlignmentStart(checkpointId, checkpointCreationTimestamp);
        // 标记检查点对齐的结束
        // 这里传递了0作为参数，但在实际应用中可能需要一个具体的值来标识对齐结束的状态或时间戳
        // 此处可能是一个简化的示例或占位符，具体实现可能需要根据实际情况来完善
        markAlignmentEnd(0);
    }

    protected void markAlignmentStart(long checkpointId, long checkpointCreationTimestamp) {
        // 计算最新的检查点开始延迟（以纳秒为单位）
        // 延迟是当前绝对时间（毫秒）减去检查点创建时间戳（毫秒），然后乘以1,000,000转换为纳秒
        // 如果结果为负数，则取0（表示没有延迟）
        latestCheckpointStartDelayNanos =
                1_000_000 * Math.max(0, clock.absoluteTimeMillis() - checkpointCreationTimestamp);
        // 重置对齐状态，可能是清除之前的对齐相关信息或准备开始新的对齐过程
        resetAlignment();
        // 记录对齐开始的相对时间戳（纳秒）
        // 这里的clock.relativeTimeNanos()可能返回从某个固定时间点（如系统启动）开始到现在的经过时间
        startOfAlignmentTimestamp = clock.relativeTimeNanos();
        startAlignmentCheckpointId = checkpointId;
    }

    protected void markAlignmentEnd() {
        markAlignmentEnd(clock.relativeTimeNanos() - startOfAlignmentTimestamp);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     *
     *
    */
    protected void markAlignmentEnd(long alignmentDuration) {
        // 检查对齐持续时间是否大于等于0
        // 如果小于0，则抛出异常，提示对齐时间不应为负值，并询问时间是否单调递增
        checkState(
                alignmentDuration >= 0,
                "Alignment time is less than zero({}). Is the time monotonic?",
                alignmentDuration);
        // 完成对齐持续时间的记录，可能是将结果设置到某个完成状态的变量中
        // 这里的 alignmentDuration 是对齐的总时间（以纳秒为单位）
        latestAlignmentDurationNanos.complete(alignmentDuration);
        // 完成对齐期间处理的字节数的记录
        // bytesProcessedDuringAlignment 是在对齐过程中处理的字节数
        latestBytesProcessedDuringAlignment.complete(bytesProcessedDuringAlignment);

        // 将对齐开始的时间戳重置为 OUTSIDE_OF_ALIGNMENT，表示当前不在对齐状态
        // OUTSIDE_OF_ALIGNMENT 可能是一个常量，表示不在对齐状态的时间戳标识
        startOfAlignmentTimestamp = OUTSIDE_OF_ALIGNMENT;
        bytesProcessedDuringAlignment = 0;
    }

    protected void resetAlignment() {
        markAlignmentEnd(0);
        latestAlignmentDurationNanos = new CompletableFuture<>();
        latestBytesProcessedDuringAlignment = new CompletableFuture<>();
    }

    protected abstract boolean isCheckpointPending();

    public void addProcessedBytes(int bytes) {
        if (isDuringAlignment()) {
            bytesProcessedDuringAlignment += bytes;
        }
    }

    @VisibleForTesting
    boolean isDuringAlignment() {
        return startOfAlignmentTimestamp > OUTSIDE_OF_ALIGNMENT;
    }

    protected final Clock getClock() {
        return clock;
    }
}
