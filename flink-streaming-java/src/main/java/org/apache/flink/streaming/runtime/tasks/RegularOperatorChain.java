/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterDelegate;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;

/** A regular non finished on restore {@link OperatorChain}. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 *  OperatorChain 包含在一个单独的  StreamTask 中作为链执行的所有 Operator。
 */
@Internal
public class RegularOperatorChain<OUT, OP extends StreamOperator<OUT>>
        extends OperatorChain<OUT, OP> {

    private static final Logger LOG = LoggerFactory.getLogger(RegularOperatorChain.class);

    public RegularOperatorChain(
            StreamTask<OUT, OP> containingTask,
            RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> recordWriterDelegate) {
        super(containingTask, recordWriterDelegate);
    }

    @VisibleForTesting
    RegularOperatorChain(
            List<StreamOperatorWrapper<?, ?>> allOperatorWrappers,
            RecordWriterOutput<?>[] streamOutputs,
            WatermarkGaugeExposingOutput<StreamRecord<OUT>> mainOperatorOutput,
            StreamOperatorWrapper<OUT, OP> mainOperatorWrapper) {
        super(allOperatorWrappers, streamOutputs, mainOperatorOutput, mainOperatorWrapper);
    }

    @Override
    public boolean isTaskDeployedAsFinished() {
        return false;
    }

    @Override
    public void dispatchOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> event)
            throws FlinkException {
        operatorEventDispatcher.dispatchEventToHandlers(operator, event);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        // go forward through the operator chain and tell each operator
        // to prepare the checkpoint
        for (StreamOperatorWrapper<?, ?> operatorWrapper : getAllOperators()) {
            if (!operatorWrapper.isClosed()) {
                operatorWrapper.getStreamOperator().prepareSnapshotPreBarrier(checkpointId);
            }
        }
    }

    @Override
    public void endInput(int inputId) throws Exception {
        if (mainOperatorWrapper != null) {
            mainOperatorWrapper.endOperatorInput(inputId);
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 恢复StreamOperator状态
    */
    @Override
    public void initializeStateAndOpenOperators(
            StreamTaskStateInitializer streamTaskStateInitializer) throws Exception {
        // 遍历所有StreamOperator
        for (StreamOperatorWrapper<?, ?> operatorWrapper : getAllOperators(true)) {
            //获取StreamOperator
            StreamOperator<?> operator = operatorWrapper.getStreamOperator();
            //初始化StreamOperator所有状态
            operator.initializeState(streamTaskStateInitializer);
            //在处理任何元素之前立即调用，通常做一些初始化操作。
            operator.open();
        }
    }

    @Override
    public void finishOperators(StreamTaskActionExecutor actionExecutor, StopMode stopMode)
            throws Exception {
        if (firstOperatorWrapper != null) {
            firstOperatorWrapper.finish(actionExecutor, stopMode);
        }
    }

    @Override
    public void closeAllOperators() throws Exception {
        super.closeAllOperators();
        Exception closingException = null;
        for (StreamOperatorWrapper<?, ?> operatorWrapper : getAllOperators(true)) {
            try {
                operatorWrapper.close();
            } catch (Exception e) {
                closingException = firstOrSuppressed(e, closingException);
            }
        }
        if (closingException != null) {
            throw closingException;
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        Exception previousException = null;
        for (StreamOperatorWrapper<?, ?> operatorWrapper : getAllOperators(true)) {
            try {
                operatorWrapper.notifyCheckpointComplete(checkpointId);
            } catch (Exception e) {
                previousException = ExceptionUtils.firstOrSuppressed(e, previousException);
            }
        }
        ExceptionUtils.tryRethrowException(previousException);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        Exception previousException = null;
        for (StreamOperatorWrapper<?, ?> operatorWrapper : getAllOperators(true)) {
            try {
                operatorWrapper.getStreamOperator().notifyCheckpointAborted(checkpointId);
            } catch (Exception e) {
                previousException = ExceptionUtils.firstOrSuppressed(e, previousException);
            }
        }
        ExceptionUtils.tryRethrowException(previousException);
    }

    @Override
    public void notifyCheckpointSubsumed(long checkpointId) throws Exception {
        Exception previousException = null;
        for (StreamOperatorWrapper<?, ?> operatorWrapper : getAllOperators(true)) {
            try {
                operatorWrapper.notifyCheckpointSubsumed(checkpointId);
            } catch (Exception e) {
                previousException = ExceptionUtils.firstOrSuppressed(e, previousException);
            }
        }
        ExceptionUtils.tryRethrowException(previousException);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 捕获并存储流处理中所有未关闭的算子的状态快照。
     *
     * @param operatorSnapshotsInProgress 存储算子快照进度的映射
     * @param checkpointMetaData          检查点元数据
     * @param checkpointOptions           检查点选项
     * @param isRunning                   用于检查任务是否仍在运行的提供者
     * @param channelStateWriteResult     通道状态写入结果
     * @param storage                     用于存储检查点数据的工厂
     * @throws Exception 抛出异常（如果发生任何错误）
    */
    @Override
    public void snapshotState(
            Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            Supplier<Boolean> isRunning,
            ChannelStateWriter.ChannelStateWriteResult channelStateWriteResult,
            CheckpointStreamFactory storage)
            throws Exception {
        // 遍历所有算子（包括子任务）
        for (StreamOperatorWrapper<?, ?> operatorWrapper : getAllOperators(true)) {
            // 如果算子未关闭
            if (!operatorWrapper.isClosed()) {
                // 将算子的ID和对应的快照未来对象添加到映射中
                // 快照未来对象是通过调用 buildOperatorSnapshotFutures 方法创建的
                operatorSnapshotsInProgress.put(
                        operatorWrapper.getStreamOperator().getOperatorID(),
                        buildOperatorSnapshotFutures(
                                checkpointMetaData,// 检查点元数据
                                checkpointOptions,// 检查点选项
                                operatorWrapper.getStreamOperator(),// 当前的算子对象
                                isRunning,// 检查任务是否仍在运行的提供者
                                channelStateWriteResult,// 通道状态写入结果
                                storage));// 用于存储检查点数据的工厂
            }
        }
        // 发送确认检查点事件，告知系统已经完成了检查点的捕获
        /**
         * 像JobMaster发送AcknowledgeCheckpointEvent事件消息
         */
        sendAcknowledgeCheckpointEvent(checkpointMetaData.getCheckpointId());
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 为给定的 StreamOperator 构建并返回 OperatorSnapshotFutures 对象，该对象表示算子的快照进度。
     *
     * @param checkpointMetaData   检查点元数据，包含了关于检查点的信息。
     * @param checkpointOptions    检查点选项，用于配置检查点行为。
     * @param op                   要进行快照的 StreamOperator 对象。
     * @param isRunning            一个 Supplier，用于检查任务是否仍在运行。
     * @param channelStateWriteResult 通道状态写入结果，包含了通道状态写入的相关信息。
     * @param storage              用于存储检查点数据的 CheckpointStreamFactory。
     * @return OperatorSnapshotFutures 表示算子快照进度的对象。
     * @throws Exception 如果在构建快照过程中发生任何异常。
    */
    private OperatorSnapshotFutures buildOperatorSnapshotFutures(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            StreamOperator<?> op,
            Supplier<Boolean> isRunning,
            ChannelStateWriter.ChannelStateWriteResult channelStateWriteResult,
            CheckpointStreamFactory storage)
            throws Exception {
        // 调用 checkpointStreamOperator 方法为 StreamOperator 创建快照，并返回 OperatorSnapshotFutures 对象
        // 该对象表示算子的快照进度
        OperatorSnapshotFutures snapshotInProgress =
                checkpointStreamOperator(
                        op, checkpointMetaData, checkpointOptions, storage, isRunning);
        // 调用 snapshotChannelStates 方法将 StreamOperator 的通道状态写入快照
        snapshotChannelStates(op, channelStateWriteResult, snapshotInProgress);
        // 返回表示算子快照进度的 OperatorSnapshotFutures 对象
        return snapshotInProgress;
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 对给定的 StreamOperator 进行快照操作，并返回 OperatorSnapshotFutures 对象以跟踪快照进度。
     *
     * @param op                   要进行快照操作的 StreamOperator 对象。
     * @param checkpointMetaData   检查点元数据，包含了关于检查点的信息，如检查点ID和时间戳。
     * @param checkpointOptions    检查点选项，用于配置检查点行为。
     * @param storageLocation      CheckpointStreamFactory，用于指定检查点数据的存储位置。
     * @param isRunning            一个 Supplier，用于检查任务是否仍在运行。
     * @return OperatorSnapshotFutures 表示 StreamOperator 快照进度的对象。
     * @throws Exception 如果在快照过程中发生任何异常。
    */
    private static OperatorSnapshotFutures checkpointStreamOperator(
            StreamOperator<?> op,
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory storageLocation,
            Supplier<Boolean> isRunning)
            throws Exception {
        try {
            // 调用 StreamOperator 的 snapshotState 方法进行快照操作
            // 并传入检查点ID、时间戳、检查点选项和存储位置
            // 返回 OperatorSnapshotFutures 对象以跟踪快照进度
            return op.snapshotState(
                    checkpointMetaData.getCheckpointId(),
                    checkpointMetaData.getTimestamp(),
                    checkpointOptions,
                    storageLocation);
        } catch (Exception ex) {
            // 如果在快照过程中捕获到异常，并且任务仍在运行
            // 则记录日志信息，包括异常的消息和堆栈跟踪
            if (isRunning.get()) {
                LOG.info(ex.getMessage(), ex);
            }
            throw ex;//抛出异常
        }
    }
}
