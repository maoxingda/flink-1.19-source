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
 * WITHOUStreamRecord<?>WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.watermark.InternalWatermark;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * Serializer for {@link StreamRecord}, {@link Watermark}, {@link LatencyMarker}, and {@link
 * WatermarkStatus}.
 *
 * <p>This does not behave like a normal {@link TypeSerializer}, instead, this is only used at the
 * stream task/operator level for transmitting StreamRecords and Watermarks.
 *
 * @param <T> The type of value in the StreamRecord
 */
@Internal
public final class StreamElementSerializer<T> extends TypeSerializer<StreamElement> {

    private static final long serialVersionUID = 1L;

    private static final int TAG_REC_WITH_TIMESTAMP = 0;
    private static final int TAG_REC_WITHOUT_TIMESTAMP = 1;
    private static final int TAG_WATERMARK = 2;
    private static final int TAG_LATENCY_MARKER = 3;
    private static final int TAG_STREAM_STATUS = 4;
    private static final int TAG_RECORD_ATTRIBUTES = 5;
    private static final int TAG_INTERNAL_WATERMARK = 6;

    private final TypeSerializer<T> typeSerializer;

    public StreamElementSerializer(TypeSerializer<T> serializer) {
        if (serializer instanceof StreamElementSerializer) {
            throw new RuntimeException(
                    "StreamRecordSerializer given to StreamRecordSerializer as value TypeSerializer: "
                            + serializer);
        }
        this.typeSerializer = requireNonNull(serializer);
    }

    public TypeSerializer<T> getContainedTypeSerializer() {
        return this.typeSerializer;
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public StreamElementSerializer<T> duplicate() {
        TypeSerializer<T> copy = typeSerializer.duplicate();
        return (copy == typeSerializer) ? this : new StreamElementSerializer<T>(copy);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public StreamRecord<T> createInstance() {
        return new StreamRecord<T>(typeSerializer.createInstance());
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public StreamElement copy(StreamElement from) {
        // we can reuse the timestamp since Instant is immutable
        if (from.isRecord()) {
            StreamRecord<T> fromRecord = from.asRecord();
            return fromRecord.copy(typeSerializer.copy(fromRecord.getValue()));
        } else if (from.isWatermark() || from.isWatermarkStatus() || from.isLatencyMarker()) {
            // is immutable
            return from;
        } else {
            throw new RuntimeException();
        }
    }

    @Override
    public StreamElement copy(StreamElement from, StreamElement reuse) {
        if (from.isRecord() && reuse.isRecord()) {
            StreamRecord<T> fromRecord = from.asRecord();
            StreamRecord<T> reuseRecord = reuse.asRecord();

            T valueCopy = typeSerializer.copy(fromRecord.getValue(), reuseRecord.getValue());
            fromRecord.copyTo(valueCopy, reuseRecord);
            return reuse;
        } else if (from.isWatermark() || from.isWatermarkStatus() || from.isLatencyMarker()) {
            // is immutable
            return from;
        } else {
            throw new RuntimeException("Cannot copy " + from + " -> " + reuse);
        }
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int tag = source.readByte();
        target.write(tag);

        if (tag == TAG_REC_WITH_TIMESTAMP) {
            // move timestamp
            target.writeLong(source.readLong());
            typeSerializer.copy(source, target);
        } else if (tag == TAG_REC_WITHOUT_TIMESTAMP) {
            typeSerializer.copy(source, target);
        } else if (tag == TAG_WATERMARK) {
            target.writeLong(source.readLong());
        } else if (tag == TAG_INTERNAL_WATERMARK) {
            target.writeInt(source.readInt());
            target.writeLong(source.readLong());
        } else if (tag == TAG_STREAM_STATUS) {
            target.writeInt(source.readInt());
        } else if (tag == TAG_LATENCY_MARKER) {
            target.writeLong(source.readLong());
            target.writeLong(source.readLong());
            target.writeLong(source.readLong());
            target.writeInt(source.readInt());
        } else if (tag == TAG_RECORD_ATTRIBUTES) {
            target.writeBoolean(source.readBoolean());
        } else {
            throw new IOException("Corrupt stream, found tag: " + tag);
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将StreamElement类型的值序列化为DataOutputView目标流。
     *
     * @param value 要序列化的StreamElement值
     * @param target 序列化目标，即DataOutputView对象
     * @throws IOException 如果在序列化过程中发生I/O错误
    */
    @Override
    public void serialize(StreamElement value, DataOutputView target) throws IOException {
        // 判断value是否为Record
        if (value.isRecord()) {
            // 转换为StreamRecord<T>类型
            StreamRecord<T> record = value.asRecord();
            // 判断Record是否有时间戳
            if (record.hasTimestamp()) {
                // 写入带有时间戳的Record的标记
                target.write(TAG_REC_WITH_TIMESTAMP);
                // 写入Record的时间戳
                target.writeLong(record.getTimestamp());
            } else {
                // 写入没有时间戳的Record的标记
                target.write(TAG_REC_WITHOUT_TIMESTAMP);
            }
            // 使用类型序列化器序列化Record的值
            typeSerializer.serialize(record.getValue(), target);
            // 判断value是否为Watermark
        } else if (value.isWatermark()) {
            // 判断Watermark是否为InternalWatermark
            if (value instanceof InternalWatermark) {
                // 写入InternalWatermark的标记
                target.write(TAG_INTERNAL_WATERMARK);
                // 写入InternalWatermark的子分区索引
                target.writeInt(((InternalWatermark) value).getSubpartitionIndex());
            } else {
                // 写入Watermark的标记
                target.write(TAG_WATERMARK);
            }
            // 写入Watermark的时间戳
            target.writeLong(value.asWatermark().getTimestamp());
            // 判断value是否为WatermarkStatus
        } else if (value.isWatermarkStatus()) {
            // 写入WatermarkStatus的标记
            target.write(TAG_STREAM_STATUS);
            // 写入WatermarkStatus的状态
            target.writeInt(value.asWatermarkStatus().getStatus());
            // 判断value是否为LatencyMarker
        } else if (value.isLatencyMarker()) {
            // 写入LatencyMarker的标记
            target.write(TAG_LATENCY_MARKER);
            // 写入LatencyMarker的标记时间
            target.writeLong(value.asLatencyMarker().getMarkedTime());
            // 写入LatencyMarker的OperatorId的低32位
            target.writeLong(value.asLatencyMarker().getOperatorId().getLowerPart());
            // 写入LatencyMarker的OperatorId的高32位
            target.writeLong(value.asLatencyMarker().getOperatorId().getUpperPart());
            // 写入LatencyMarker的子任务索引
            target.writeInt(value.asLatencyMarker().getSubtaskIndex());
            // 判断value是否为RecordAttributes
        } else if (value.isRecordAttributes()) {
            // 写入RecordAttributes的标记
            target.write(TAG_RECORD_ATTRIBUTES);
            // 写入RecordAttributes的backlog状态
            target.writeBoolean(value.asRecordAttributes().isBacklog());
        } else {
            // 抛出运行时异常
            throw new RuntimeException();
        }
    }

    @Override
    public StreamElement deserialize(DataInputView source) throws IOException {
        int tag = source.readByte();
        if (tag == TAG_REC_WITH_TIMESTAMP) {
            long timestamp = source.readLong();
            return new StreamRecord<T>(typeSerializer.deserialize(source), timestamp);
        } else if (tag == TAG_REC_WITHOUT_TIMESTAMP) {
            return new StreamRecord<T>(typeSerializer.deserialize(source));
        } else if (tag == TAG_WATERMARK) {
            return new Watermark(source.readLong());
        } else if (tag == TAG_INTERNAL_WATERMARK) {
            int subpartitionIndex = source.readInt();
            return new InternalWatermark(source.readLong(), subpartitionIndex);
        } else if (tag == TAG_STREAM_STATUS) {
            return new WatermarkStatus(source.readInt());
        } else if (tag == TAG_LATENCY_MARKER) {
            return new LatencyMarker(
                    source.readLong(),
                    new OperatorID(source.readLong(), source.readLong()),
                    source.readInt());
        } else if (tag == TAG_RECORD_ATTRIBUTES) {
            return new RecordAttributes(source.readBoolean());
        } else {
            throw new IOException("Corrupt stream, found tag: " + tag);
        }
    }

    @Override
    public StreamElement deserialize(StreamElement reuse, DataInputView source) throws IOException {
        int tag = source.readByte();
        if (tag == TAG_REC_WITH_TIMESTAMP) {
            long timestamp = source.readLong();
            T value = typeSerializer.deserialize(source);
            StreamRecord<T> reuseRecord = reuse.asRecord();
            reuseRecord.replace(value, timestamp);
            return reuseRecord;
        } else if (tag == TAG_REC_WITHOUT_TIMESTAMP) {
            T value = typeSerializer.deserialize(source);
            StreamRecord<T> reuseRecord = reuse.asRecord();
            reuseRecord.replace(value);
            return reuseRecord;
        } else if (tag == TAG_WATERMARK) {
            return new Watermark(source.readLong());
        } else if (tag == TAG_INTERNAL_WATERMARK) {
            int subpartitionIndex = source.readInt();
            return new InternalWatermark(source.readLong(), subpartitionIndex);
        } else if (tag == TAG_LATENCY_MARKER) {
            return new LatencyMarker(
                    source.readLong(),
                    new OperatorID(source.readLong(), source.readLong()),
                    source.readInt());
        } else if (tag == TAG_RECORD_ATTRIBUTES) {
            return new RecordAttributes(source.readBoolean());
        } else {
            throw new IOException("Corrupt stream, found tag: " + tag);
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof StreamElementSerializer) {
            StreamElementSerializer<?> other = (StreamElementSerializer<?>) obj;

            return typeSerializer.equals(other.typeSerializer);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return typeSerializer.hashCode();
    }

    // --------------------------------------------------------------------------------------------
    // Serializer configuration snapshotting & compatibility
    //
    // This serializer may be used by Flink internal operators that need to checkpoint
    // buffered records. Therefore, it may be part of managed state and need to implement
    // the configuration snapshot and compatibility methods.
    // --------------------------------------------------------------------------------------------

    @Override
    public StreamElementSerializerSnapshot<T> snapshotConfiguration() {
        return new StreamElementSerializerSnapshot<>(this);
    }

    /** Configuration snapshot specific to the {@link StreamElementSerializer}. */
    public static final class StreamElementSerializerSnapshot<T>
            extends CompositeTypeSerializerSnapshot<StreamElement, StreamElementSerializer<T>> {

        private static final int VERSION = 2;

        @SuppressWarnings("WeakerAccess")
        public StreamElementSerializerSnapshot() {}

        StreamElementSerializerSnapshot(StreamElementSerializer<T> serializerInstance) {
            super(serializerInstance);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return VERSION;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(
                StreamElementSerializer<T> outerSerializer) {
            return new TypeSerializer[] {outerSerializer.getContainedTypeSerializer()};
        }

        @Override
        protected StreamElementSerializer<T> createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            @SuppressWarnings("unchecked")
            TypeSerializer<T> casted = (TypeSerializer<T>) nestedSerializers[0];

            return new StreamElementSerializer<>(casted);
        }
    }
}
