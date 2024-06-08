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
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FileRegionBuffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufOutputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOutboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOutboundInvoker;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ProtocolException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A simple and generic interface to serialize messages to Netty's buffer space.
 *
 * <p>This class must be public as long as we are using a Netty version prior to 4.0.45. Please
 * check FLINK-7845 for more information.
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 通用的接口，用于将消息序列化到Netty的缓冲区空间。
*/
public abstract class NettyMessage {

    // ------------------------------------------------------------------------
    // Note: Every NettyMessage subtype needs to have a public 0-argument
    // constructor in order to work with the generic deserializer.
    // ------------------------------------------------------------------------

    static final int FRAME_HEADER_LENGTH =
            4 + 4 + 1; // frame length (4), magic number (4), msg ID (1)

    static final int MAGIC_NUMBER = 0xBADC0FFE;

    abstract void write(
            ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
            throws IOException;

    // ------------------------------------------------------------------------

    /**
     * Allocates a new (header and contents) buffer and adds some header information for the frame
     * decoder.
     *
     * <p>Before sending the buffer, you must write the actual length after adding the contents as
     * an integer to position <tt>0</tt>!
     *
     * @param allocator byte buffer allocator to use
     * @param id {@link NettyMessage} subclass ID
     * @return a newly allocated direct buffer with header data written for {@link
     *     NettyMessageEncoder}
     */
    private static ByteBuf allocateBuffer(ByteBufAllocator allocator, byte id) {
        return allocateBuffer(allocator, id, -1);
    }

    /**
     * Allocates a new (header and contents) buffer and adds some header information for the frame
     * decoder.
     *
     * <p>If the <tt>contentLength</tt> is unknown, you must write the actual length after adding
     * the contents as an integer to position <tt>0</tt>!
     *
     * @param allocator byte buffer allocator to use
     * @param id {@link NettyMessage} subclass ID
     * @param contentLength content length (or <tt>-1</tt> if unknown)
     * @return a newly allocated direct buffer with header data written for {@link
     *     NettyMessageEncoder}
     */
    private static ByteBuf allocateBuffer(ByteBufAllocator allocator, byte id, int contentLength) {
        return allocateBuffer(allocator, id, 0, contentLength, true);
    }

    /**
     * Allocates a new buffer and adds some header information for the frame decoder.
     *
     * <p>If the <tt>contentLength</tt> is unknown, you must write the actual length after adding
     * the contents as an integer to position <tt>0</tt>!
     *
     * @param allocator byte buffer allocator to use
     * @param id {@link NettyMessage} subclass ID
     * @param messageHeaderLength additional header length that should be part of the allocated
     *     buffer and is written outside of this method
     * @param contentLength content length (or <tt>-1</tt> if unknown)
     * @param allocateForContent whether to make room for the actual content in the buffer
     *     (<tt>true</tt>) or whether to only return a buffer with the header information
     *     (<tt>false</tt>)
     * @return a newly allocated direct buffer with header data written for {@link
     *     NettyMessageEncoder}
     */

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 分配ByteBuf对象，用于消息封装
     *
     * @param allocator          ByteBufAllocator对象，用于分配ByteBuf
     * @param id                 消息的ID
     * @param messageHeaderLength 消息头长度
     * @param contentLength      消息内容长度，如果未知则为-1
     * @param allocateForContent 是否为消息内容分配内存
     * @return 分配好的ByteBuf对象
     * @throws IllegalArgumentException 如果contentLength大于允许的最大值
    */
    private static ByteBuf allocateBuffer(
            ByteBufAllocator allocator,
            byte id,
            int messageHeaderLength,
            int contentLength,
            boolean allocateForContent) {
        // 检查contentLength是否超出允许的最大值
        checkArgument(contentLength <= Integer.MAX_VALUE - FRAME_HEADER_LENGTH);

        final ByteBuf buffer;
        if (!allocateForContent) {
            // 如果不需要为消息内容分配内存，则只分配消息头所需的内存
            buffer = allocator.directBuffer(FRAME_HEADER_LENGTH + messageHeaderLength);
        } else if (contentLength != -1) {
            // 如果需要为消息内容分配内存且contentLength已知，则分配消息头和消息内容所需的内存
            buffer =
                    allocator.directBuffer(
                            FRAME_HEADER_LENGTH + messageHeaderLength + contentLength);
        } else {
            // content length unknown -> start with the default initial size (rather than
            // FRAME_HEADER_LENGTH only):
            // 如果需要为消息内容分配内存但contentLength未知，则先分配默认大小的内存
            // 而不是仅分配FRAME_HEADER_LENGTH
            buffer = allocator.directBuffer();
        }
        // 写入消息的总长度（可能稍后会更新，例如当contentLength为-1时）
        buffer.writeInt(
                FRAME_HEADER_LENGTH
                        + messageHeaderLength
                        + contentLength); // may be updated later, e.g. if contentLength == -1
        // 写入魔法数字（标识消息的开始）
        buffer.writeInt(MAGIC_NUMBER);
      ///写入id
        buffer.writeByte(id);

        // 返回分配好的ByteBuf对象
        return buffer;
    }

    // ------------------------------------------------------------------------
    // Generic NettyMessage encoder and decoder
    // ------------------------------------------------------------------------

    @ChannelHandler.Sharable
    static class NettyMessageEncoder extends ChannelOutboundHandlerAdapter {

        /**
         * @授课老师(微信): yi_locus
         * email: 156184212@qq.com
         * 定义一个公共的write方法，它接收三个参数：
         * 1. ChannelHandlerContext：这是Netty中表示Channel处理器上下文的对象，它提供了与Channel、Pipeline和EventExecutor交互的接口。
         * 2. Object msg：要写入通道的消息对象。这里使用Object作为泛型类型，表示它可以接受任何类型的消息。
         * 3. ChannelPromise：Netty中的Promise，用于异步操作完成后通知监听者。
         * 方法声明可能会抛出IOException，表示在写入过程中可能会发生I/O异常
        */
        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
                throws IOException {
            // 使用instanceof关键字检查传入的消息对象是否是NettyMessage类型的实例。
            // NettyMessage可能是自定义的消息类型，用于封装Netty中的消息数据。
            if (msg instanceof NettyMessage) {
                // 如果消息是NettyMessage类型的实例，则调用它的write方法，并将ChannelHandlerContext、ChannelPromise和ByteBufAllocator（通过ctx.alloc()获取）作为参数传入。
                ((NettyMessage) msg).write(ctx, promise, ctx.alloc());
            } else {
                // 如果消息不是NettyMessage类型的实例，则直接调用ChannelHandlerContext的write方法，将原始消息和ChannelPromise作为参数传入。
                ctx.write(msg, promise);
            }
        }
    }

    /**
     * Message decoder based on netty's {@link LengthFieldBasedFrameDecoder} but avoiding the
     * additional memory copy inside {@link #extractFrame(ChannelHandlerContext, ByteBuf, int, int)}
     * since we completely decode the {@link ByteBuf} inside {@link #decode(ChannelHandlerContext,
     * ByteBuf)} and will not re-use it afterwards.
     *
     * <p>The frame-length encoder will be based on this transmission scheme created by {@link
     * NettyMessage#allocateBuffer(ByteBufAllocator, byte, int)}:
     *
     * <pre>
     * +------------------+------------------+--------++----------------+
     * | FRAME LENGTH (4) | MAGIC NUMBER (4) | ID (1) || CUSTOM MESSAGE |
     * +------------------+------------------+--------++----------------+
     * </pre>
     */
    static class NettyMessageDecoder extends LengthFieldBasedFrameDecoder {
        /** Creates a new message decoded with the required frame properties. */
        NettyMessageDecoder() {
            super(Integer.MAX_VALUE, 0, 4, -4, 4);
        }

        @Override
        protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
            ByteBuf msg = (ByteBuf) super.decode(ctx, in);
            if (msg == null) {
                return null;
            }

            try {
                int magicNumber = msg.readInt();

                if (magicNumber != MAGIC_NUMBER) {
                    throw new IllegalStateException(
                            "Network stream corrupted: received incorrect magic number.");
                }

                byte msgId = msg.readByte();

                final NettyMessage decodedMsg;
                switch (msgId) {
                    case PartitionRequest.ID:
                        decodedMsg = PartitionRequest.readFrom(msg);
                        break;
                    case TaskEventRequest.ID:
                        decodedMsg = TaskEventRequest.readFrom(msg, getClass().getClassLoader());
                        break;
                    case CancelPartitionRequest.ID:
                        decodedMsg = CancelPartitionRequest.readFrom(msg);
                        break;
                    case CloseRequest.ID:
                        decodedMsg = CloseRequest.readFrom(msg);
                        break;
                    case AddCredit.ID:
                        decodedMsg = AddCredit.readFrom(msg);
                        break;
                    case ResumeConsumption.ID:
                        decodedMsg = ResumeConsumption.readFrom(msg);
                        break;
                    case AckAllUserRecordsProcessed.ID:
                        decodedMsg = AckAllUserRecordsProcessed.readFrom(msg);
                        break;
                    case NewBufferSize.ID:
                        decodedMsg = NewBufferSize.readFrom(msg);
                        break;
                    case SegmentId.ID:
                        decodedMsg = SegmentId.readFrom(msg);
                        break;
                    default:
                        throw new ProtocolException(
                                "Received unknown message from producer: " + msg);
                }

                return decodedMsg;
            } finally {
                // ByteToMessageDecoder cleanup (only the BufferResponse holds on to the decoded
                // msg but already retain()s the buffer once)
                msg.release();
            }
        }
    }

    // ------------------------------------------------------------------------
    // Server responses
    // ------------------------------------------------------------------------

    static class BufferResponse extends NettyMessage {

        static final byte ID = 0;

        // receiver ID (16), sequence number (4), backlog (4), dataType (1), isCompressed (1),
        // buffer size (4)
        static final int MESSAGE_HEADER_LENGTH =
                InputChannelID.getByteBufLength()
                        + Integer.BYTES
                        + Integer.BYTES
                        + Integer.BYTES
                        + Byte.BYTES
                        + Byte.BYTES
                        + Integer.BYTES;

        final Buffer buffer;

        final InputChannelID receiverId;

        final int subpartitionId;

        final int sequenceNumber;

        final int backlog;

        final Buffer.DataType dataType;

        final boolean isCompressed;

        final int bufferSize;

        private BufferResponse(
                @Nullable Buffer buffer,
                Buffer.DataType dataType,
                boolean isCompressed,
                int sequenceNumber,
                InputChannelID receiverId,
                int subpartitionId,
                int backlog,
                int bufferSize) {
            this.buffer = buffer;
            this.dataType = dataType;
            this.isCompressed = isCompressed;
            this.sequenceNumber = sequenceNumber;
            this.receiverId = checkNotNull(receiverId);
            this.subpartitionId = subpartitionId;
            this.backlog = backlog;
            this.bufferSize = bufferSize;
        }

        BufferResponse(
                Buffer buffer,
                int sequenceNumber,
                InputChannelID receiverId,
                int subpartitionId,
                int backlog) {
            this.buffer = checkNotNull(buffer);
            checkArgument(
                    buffer.getDataType().ordinal() <= Byte.MAX_VALUE,
                    "Too many data types defined!");
            checkArgument(backlog >= 0, "Must be non-negative.");
            this.dataType = buffer.getDataType();
            this.isCompressed = buffer.isCompressed();
            this.sequenceNumber = sequenceNumber;
            this.receiverId = checkNotNull(receiverId);
            this.subpartitionId = subpartitionId;
            this.backlog = backlog;
            this.bufferSize = buffer.getSize();
        }

        boolean isBuffer() {
            return dataType.isBuffer();
        }

        @Nullable
        public Buffer getBuffer() {
            return buffer;
        }

        void releaseBuffer() {
            if (buffer != null) {
                buffer.recycleBuffer();
            }
        }

        // --------------------------------------------------------------------
        // Serialization
        // --------------------------------------------------------------------

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            ByteBuf headerBuf = null;
            try {
                // in order to forward the buffer to netty, it needs an allocator set
                buffer.setAllocator(allocator);

                headerBuf = fillHeader(allocator);
                out.write(headerBuf);
                if (buffer instanceof FileRegionBuffer) {
                    out.write(buffer, promise);
                } else {
                    out.write(buffer.asByteBuf(), promise);
                }
            } catch (Throwable t) {
                handleException(headerBuf, buffer, t);
            }
        }

        @VisibleForTesting
        ByteBuf write(ByteBufAllocator allocator) throws IOException {
            ByteBuf headerBuf = null;
            try {
                // in order to forward the buffer to netty, it needs an allocator set
                buffer.setAllocator(allocator);

                headerBuf = fillHeader(allocator);

                CompositeByteBuf composityBuf = allocator.compositeDirectBuffer();
                composityBuf.addComponent(headerBuf);
                composityBuf.addComponent(buffer.asByteBuf());
                // update writer index since we have data written to the components:
                composityBuf.writerIndex(
                        headerBuf.writerIndex() + buffer.asByteBuf().writerIndex());
                return composityBuf;
            } catch (Throwable t) {
                handleException(headerBuf, buffer, t);
                return null; // silence the compiler
            }
        }

        private ByteBuf fillHeader(ByteBufAllocator allocator) {
            // only allocate header buffer - we will combine it with the data buffer below
            ByteBuf headerBuf =
                    allocateBuffer(allocator, ID, MESSAGE_HEADER_LENGTH, bufferSize, false);

            receiverId.writeTo(headerBuf);
            headerBuf.writeInt(subpartitionId);
            headerBuf.writeInt(sequenceNumber);
            headerBuf.writeInt(backlog);
            headerBuf.writeByte(dataType.ordinal());
            headerBuf.writeBoolean(isCompressed);
            headerBuf.writeInt(buffer.readableBytes());
            return headerBuf;
        }

        /**
         * Parses the message header part and composes a new BufferResponse with an empty data
         * buffer. The data buffer will be filled in later.
         *
         * @param messageHeader the serialized message header.
         * @param bufferAllocator the allocator for network buffer.
         * @return a BufferResponse object with the header parsed and the data buffer to fill in
         *     later. The data buffer will be null if the target channel has been released or the
         *     buffer size is 0.
         */
        /**
         * @授课老师(微信): yi_locus
         * email: 156184212@qq.com
         * 读取来自网络的数据并返回一个BufferResponse对象
        */
        static BufferResponse readFrom(
                ByteBuf messageHeader, NetworkBufferAllocator bufferAllocator) {
            // 从消息头中解析出接收者的ID
            InputChannelID receiverId = InputChannelID.fromByteBuf(messageHeader);
            // 从消息头中读取子分区ID
            int subpartitionId = messageHeader.readInt();
            // 从消息头中读取序列号
            int sequenceNumber = messageHeader.readInt();
            // 从消息头中读取积压量（可能是等待处理的消息数量）
            int backlog = messageHeader.readInt();
            // 从消息头中读取数据类型
            Buffer.DataType dataType = Buffer.DataType.values()[messageHeader.readByte()];
            // 从消息头中读取是否压缩的标志
            boolean isCompressed = messageHeader.readBoolean();
            // 从消息头中读取数据的大小
            int size = messageHeader.readInt();
            // 声明一个用于存储数据的Buffer对象
            Buffer dataBuffer;
            // 如果数据类型是Buffer类型，则分配一个池化的网络缓冲区
            if (dataType.isBuffer()) {
                dataBuffer = bufferAllocator.allocatePooledNetworkBuffer(receiverId);
                if (dataBuffer != null) {
                    dataBuffer.setDataType(dataType);
                }
            } else {
                // 如果数据类型不是Buffer类型，则分配一个非池化的网络缓冲区，并指定大小和数据类型
                dataBuffer = bufferAllocator.allocateUnPooledNetworkBuffer(size, dataType);
            }
            // 如果数据大小为0且成功分配了缓冲区，则直接回收空缓冲区，因为我们必须为空数据分配一个缓冲区以释放已经分配的信用
            if (size == 0 && dataBuffer != null) {
                // recycle the empty buffer directly, we must allocate a buffer for
                // the empty data to release the credit already allocated for it
                dataBuffer.recycleBuffer();
                dataBuffer = null;
            }
            // 如果dataBuffer不为空，则设置其压缩状态
            if (dataBuffer != null) {
                dataBuffer.setCompressed(isCompressed);
            }
            // 返回一个包含所有必要信息的BufferResponse对象
            return new BufferResponse(
                    dataBuffer,
                    dataType,
                    isCompressed,
                    sequenceNumber,
                    receiverId,
                    subpartitionId,
                    backlog,
                    size);
        }
    }

    static class ErrorResponse extends NettyMessage {

        static final byte ID = 1;

        final Throwable cause;

        @Nullable final InputChannelID receiverId;

        ErrorResponse(Throwable cause) {
            this.cause = checkNotNull(cause);
            this.receiverId = null;
        }

        ErrorResponse(Throwable cause, InputChannelID receiverId) {
            this.cause = checkNotNull(cause);
            this.receiverId = receiverId;
        }

        boolean isFatalError() {
            return receiverId == null;
        }

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            final ByteBuf result = allocateBuffer(allocator, ID);

            try (ObjectOutputStream oos = new ObjectOutputStream(new ByteBufOutputStream(result))) {
                oos.writeObject(cause);

                if (receiverId != null) {
                    result.writeBoolean(true);
                    receiverId.writeTo(result);
                } else {
                    result.writeBoolean(false);
                }

                // Update frame length...
                result.setInt(0, result.readableBytes());
                out.write(result, promise);
            } catch (Throwable t) {
                handleException(result, null, t);
            }
        }

        static ErrorResponse readFrom(ByteBuf buffer) throws Exception {
            try (ObjectInputStream ois = new ObjectInputStream(new ByteBufInputStream(buffer))) {
                Object obj = ois.readObject();

                if (!(obj instanceof Throwable)) {
                    throw new ClassCastException(
                            "Read object expected to be of type Throwable, "
                                    + "actual type is "
                                    + obj.getClass()
                                    + ".");
                } else {
                    if (buffer.readBoolean()) {
                        InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);
                        return new ErrorResponse((Throwable) obj, receiverId);
                    } else {
                        return new ErrorResponse((Throwable) obj);
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------------
    // Client requests
    // ------------------------------------------------------------------------

    static class PartitionRequest extends NettyMessage {

        private static final byte ID = 2;

        final ResultPartitionID partitionId;

        final ResultSubpartitionIndexSet queueIndexSet;

        final InputChannelID receiverId;

        final int credit;

        PartitionRequest(
                ResultPartitionID partitionId,
                ResultSubpartitionIndexSet queueIndexSet,
                InputChannelID receiverId,
                int credit) {
            this.partitionId = checkNotNull(partitionId);
            this.queueIndexSet = queueIndexSet;
            this.receiverId = checkNotNull(receiverId);
            this.credit = credit;
        }
        /**
         * @授课老师(微信): yi_locus
         * email: 156184212@qq.com
         * 这是一个被重写的 write 方法，用于将数据写入到指定的输出通道
        */
        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            // 创建一个 Consumer 对象 consumer，用于处理 ByteBuf 的写入操作
            Consumer<ByteBuf> consumer =
                    (bb) -> {
                        partitionId.getPartitionId().writeTo(bb);  // 将 partitionId 的 partitionId 写入 ByteBuf
                        partitionId.getProducerId().writeTo(bb);         // 将 partitionId 的 producerId 写入 ByteBuf
                        queueIndexSet.writeTo(bb); // 将 queueIndexSet 写入 ByteBuf
                        receiverId.writeTo(bb);        // 将 receiverId 写入 ByteBuf
                        bb.writeInt(credit);        // 将 credit 写入 ByteBuf，这里假设 credit 是一个 int 类型的值
                    };
            // 调用 writeToChannel 方法，将数据写入到指定的输出通道
            writeToChannel(
                    out,
                    promise,
                    allocator,
                    consumer,
                    ID,
                    IntermediateResultPartitionID.getByteBufLength() // 中间结果分区 ID 的字节长度
                            + ExecutionAttemptID.getByteBufLength() // 执行尝试 ID 的字节长度
                            + ResultSubpartitionIndexSet.getByteBufLength(queueIndexSet)  // 结果子分区索引集的字节长度（基于 queueIndexSet）
                            + InputChannelID.getByteBufLength()   // 输入通道 ID 的字节长度
                            + Integer.BYTES);  // credit 的字节长度（一个 int 类型的值）
        }

        static PartitionRequest readFrom(ByteBuf buffer) {
            ResultPartitionID partitionId =
                    new ResultPartitionID(
                            IntermediateResultPartitionID.fromByteBuf(buffer),
                            ExecutionAttemptID.fromByteBuf(buffer));
            ResultSubpartitionIndexSet queueIndexSet =
                    ResultSubpartitionIndexSet.fromByteBuf(buffer);
            InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);
            int credit = buffer.readInt();

            return new PartitionRequest(partitionId, queueIndexSet, receiverId, credit);
        }

        @Override
        public String toString() {
            return String.format("PartitionRequest(%s:%s:%d)", partitionId, queueIndexSet, credit);
        }
    }

    static class TaskEventRequest extends NettyMessage {

        private static final byte ID = 3;

        final TaskEvent event;

        final InputChannelID receiverId;

        final ResultPartitionID partitionId;

        TaskEventRequest(
                TaskEvent event, ResultPartitionID partitionId, InputChannelID receiverId) {
            this.event = checkNotNull(event);
            this.receiverId = checkNotNull(receiverId);
            this.partitionId = checkNotNull(partitionId);
        }

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            // TODO Directly serialize to Netty's buffer
            ByteBuffer serializedEvent = EventSerializer.toSerializedEvent(event);

            Consumer<ByteBuf> consumer =
                    (bb) -> {
                        bb.writeInt(serializedEvent.remaining());
                        bb.writeBytes(serializedEvent);

                        partitionId.getPartitionId().writeTo(bb);
                        partitionId.getProducerId().writeTo(bb);
                        receiverId.writeTo(bb);
                    };

            writeToChannel(
                    out,
                    promise,
                    allocator,
                    consumer,
                    ID,
                    Integer.BYTES
                            + serializedEvent.remaining()
                            + IntermediateResultPartitionID.getByteBufLength()
                            + ExecutionAttemptID.getByteBufLength()
                            + InputChannelID.getByteBufLength());
        }

        static TaskEventRequest readFrom(ByteBuf buffer, ClassLoader classLoader)
                throws IOException {
            // directly deserialize fromNetty's buffer
            int length = buffer.readInt();
            ByteBuffer serializedEvent = buffer.nioBuffer(buffer.readerIndex(), length);
            // assume this event's content is read from the ByteBuf (positions are not shared!)
            buffer.readerIndex(buffer.readerIndex() + length);

            TaskEvent event =
                    (TaskEvent) EventSerializer.fromSerializedEvent(serializedEvent, classLoader);

            ResultPartitionID partitionId =
                    new ResultPartitionID(
                            IntermediateResultPartitionID.fromByteBuf(buffer),
                            ExecutionAttemptID.fromByteBuf(buffer));

            InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);

            return new TaskEventRequest(event, partitionId, receiverId);
        }
    }

    /**
     * Cancels the partition request of the {@link InputChannel} identified by {@link
     * InputChannelID}.
     *
     * <p>There is a 1:1 mapping between the input channel and partition per physical channel.
     * Therefore, the {@link InputChannelID} instance is enough to identify which request to cancel.
     */
    static class CancelPartitionRequest extends NettyMessage {

        private static final byte ID = 4;

        final InputChannelID receiverId;

        CancelPartitionRequest(InputChannelID receiverId) {
            this.receiverId = checkNotNull(receiverId);
        }

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            writeToChannel(
                    out,
                    promise,
                    allocator,
                    receiverId::writeTo,
                    ID,
                    InputChannelID.getByteBufLength());
        }

        static CancelPartitionRequest readFrom(ByteBuf buffer) throws Exception {
            return new CancelPartitionRequest(InputChannelID.fromByteBuf(buffer));
        }
    }

    static class CloseRequest extends NettyMessage {

        private static final byte ID = 5;

        CloseRequest() {}

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            writeToChannel(out, promise, allocator, ignored -> {}, ID, 0);
        }

        static CloseRequest readFrom(@SuppressWarnings("unused") ByteBuf buffer) throws Exception {
            return new CloseRequest();
        }
    }

    /** Incremental credit announcement from the client to the server. */
    static class AddCredit extends NettyMessage {

        private static final byte ID = 6;

        final int credit;

        final InputChannelID receiverId;

        AddCredit(int credit, InputChannelID receiverId) {
            checkArgument(credit > 0, "The announced credit should be greater than 0");
            this.credit = credit;
            this.receiverId = receiverId;
        }

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            ByteBuf result = null;

            try {
                result =
                        allocateBuffer(
                                allocator, ID, Integer.BYTES + InputChannelID.getByteBufLength());
                result.writeInt(credit);
                receiverId.writeTo(result);

                out.write(result, promise);
            } catch (Throwable t) {
                handleException(result, null, t);
            }
        }

        static AddCredit readFrom(ByteBuf buffer) {
            int credit = buffer.readInt();
            InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);

            return new AddCredit(credit, receiverId);
        }

        @Override
        public String toString() {
            return String.format("AddCredit(%s : %d)", receiverId, credit);
        }
    }

    /** Message to notify the producer to unblock from checkpoint. */
    static class ResumeConsumption extends NettyMessage {

        private static final byte ID = 7;

        final InputChannelID receiverId;

        ResumeConsumption(InputChannelID receiverId) {
            this.receiverId = receiverId;
        }

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            writeToChannel(
                    out,
                    promise,
                    allocator,
                    receiverId::writeTo,
                    ID,
                    InputChannelID.getByteBufLength());
        }

        static ResumeConsumption readFrom(ByteBuf buffer) {
            return new ResumeConsumption(InputChannelID.fromByteBuf(buffer));
        }

        @Override
        public String toString() {
            return String.format("ResumeConsumption(%s)", receiverId);
        }
    }

    static class AckAllUserRecordsProcessed extends NettyMessage {

        private static final byte ID = 8;

        final InputChannelID receiverId;

        AckAllUserRecordsProcessed(InputChannelID receiverId) {
            this.receiverId = receiverId;
        }

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            writeToChannel(
                    out,
                    promise,
                    allocator,
                    receiverId::writeTo,
                    ID,
                    InputChannelID.getByteBufLength());
        }

        static AckAllUserRecordsProcessed readFrom(ByteBuf buffer) {
            return new AckAllUserRecordsProcessed(InputChannelID.fromByteBuf(buffer));
        }

        @Override
        public String toString() {
            return String.format("AckAllUserRecordsProcessed(%s)", receiverId);
        }
    }

    /** Backlog announcement from the producer to the consumer for credit allocation. */
    static class BacklogAnnouncement extends NettyMessage {

        static final byte ID = 9;

        final int backlog;

        final InputChannelID receiverId;

        BacklogAnnouncement(int backlog, InputChannelID receiverId) {
            checkArgument(backlog > 0, "Must be positive.");
            checkArgument(receiverId != null, "Must be not null.");

            this.backlog = backlog;
            this.receiverId = receiverId;
        }

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            ByteBuf result = null;

            try {
                result =
                        allocateBuffer(
                                allocator, ID, Integer.BYTES + InputChannelID.getByteBufLength());
                result.writeInt(backlog);
                receiverId.writeTo(result);

                out.write(result, promise);
            } catch (Throwable t) {
                handleException(result, null, t);
            }
        }

        static BacklogAnnouncement readFrom(ByteBuf buffer) {
            int backlog = buffer.readInt();
            InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);

            return new BacklogAnnouncement(backlog, receiverId);
        }

        @Override
        public String toString() {
            return String.format("BacklogAnnouncement(%d : %s)", backlog, receiverId);
        }
    }

    /** Message to notify producer about new buffer size. */
    static class NewBufferSize extends NettyMessage {

        private static final byte ID = 10;

        final int bufferSize;

        final InputChannelID receiverId;

        NewBufferSize(int bufferSize, InputChannelID receiverId) {
            checkArgument(bufferSize > 0, "The new buffer size should be greater than 0");
            this.bufferSize = bufferSize;
            this.receiverId = receiverId;
        }

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            ByteBuf result = null;

            try {
                result =
                        allocateBuffer(
                                allocator, ID, Integer.BYTES + InputChannelID.getByteBufLength());
                result.writeInt(bufferSize);
                receiverId.writeTo(result);

                out.write(result, promise);
            } catch (Throwable t) {
                handleException(result, null, t);
            }
        }

        static NewBufferSize readFrom(ByteBuf buffer) {
            int bufferSize = buffer.readInt();
            InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);

            return new NewBufferSize(bufferSize, receiverId);
        }

        @Override
        public String toString() {
            return String.format("NewBufferSize(%s : %d)", receiverId, bufferSize);
        }
    }

    /** Message to notify producer about the id of required segment. */
    static class SegmentId extends NettyMessage {

        private static final byte ID = 11;

        final int subpartitionId;

        final int segmentId;

        final InputChannelID receiverId;

        SegmentId(int subpartitionId, int segmentId, InputChannelID receiverId) {
            this.subpartitionId = subpartitionId;
            checkArgument(segmentId > 0L, "The segmentId should be greater than 0");
            this.segmentId = segmentId;
            this.receiverId = receiverId;
        }

        @Override
        void write(ChannelOutboundInvoker out, ChannelPromise promise, ByteBufAllocator allocator)
                throws IOException {
            ByteBuf result = null;

            try {
                result =
                        allocateBuffer(
                                allocator,
                                ID,
                                Integer.BYTES + Integer.BYTES + InputChannelID.getByteBufLength());
                result.writeInt(subpartitionId);
                result.writeInt(segmentId);
                receiverId.writeTo(result);

                out.write(result, promise);
            } catch (Throwable t) {
                handleException(result, null, t);
            }
        }

        static SegmentId readFrom(ByteBuf buffer) {
            int subpartitionId = buffer.readInt();
            int segmentId = buffer.readInt();
            InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);

            return new SegmentId(subpartitionId, segmentId, receiverId);
        }

        @Override
        public String toString() {
            return String.format("SegmentId(%s : %d)", receiverId, segmentId);
        }
    }

    // ------------------------------------------------------------------------
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将数据写入通道的方法
     *
     * @param out           通道输出调用者，用于将数据写入通道
     * @param promise       通道承诺，通常用于异步操作中的结果处理
     * @param allocator     ByteBuf分配器，用于分配ByteBuf实例
     * @param consumer      消费者，用于处理并填充ByteBuf实例
     * @param id            字节标识符，可能用于ByteBuf的特定用途或识别
     * @param length        要写入的数据长度
     * @throws IOException  如果写入过程中发生I/O错误，则抛出此异常
    */
    void writeToChannel(
            ChannelOutboundInvoker out,
            ChannelPromise promise,
            ByteBufAllocator allocator,
            Consumer<ByteBuf> consumer,
            byte id,
            int length)
            throws IOException {

        ByteBuf byteBuf = null;
        try {
            // 使用指定的allocator、id和length来分配ByteBuf
            byteBuf = allocateBuffer(allocator, id, length);
            // 调用consumer的accept方法，将byteBuf作为参数传递，用于填充ByteBuf实例
            consumer.accept(byteBuf);
            // 将ByteBuf写入通道，并关联promise以处理异步结果
            out.write(byteBuf, promise);
        } catch (Throwable t) {
            //处理异常
            handleException(byteBuf, null, t);
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 异常处理函数，用于在发生异常时释放和回收相关资源
     *
     * @param byteBuf  需要释放的ByteBuf对象，可以为null
     * @param buffer   需要回收的Buffer对象，可以为null
     * @param t        发生的异常
     * @throws IOException 如果异常是IOException类型或其子类，则重新抛出
    */
    void handleException(@Nullable ByteBuf byteBuf, @Nullable Buffer buffer, Throwable t)
            throws IOException {
        if (byteBuf != null) {
            // 如果ByteBuf不为null，则释放ByteBuf占用的资源
            byteBuf.release();
        }
        if (buffer != null) {
            // 如果Buffer不为null，则回收Buffer占用的资源
            buffer.recycleBuffer();
        }
        ExceptionUtils.rethrowIOException(t);
    }
}
