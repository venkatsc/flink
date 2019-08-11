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

package org.apache.flink.runtime.io.network.rdma;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import javax.annotation.Nullable;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufOutputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.CompositeByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.util.ExceptionUtils;

/**
 * A simple and generic interface to serialize messages to Netty's buffer space.
 *
 * <p>This class must be public as long as we are using a Netty version prior to 4.0.45. Please check FLINK-7845 for
 * more information.
 */
public abstract class RdmaMessage {

	// ------------------------------------------------------------------------
	// Note: Every NettyMessage subtype needs to have a public 0-argument
	// constructor in order to work with the generic deserializer.
	// ------------------------------------------------------------------------

	static final int FRAME_HEADER_LENGTH = 4 + 4 + 1; // frame length (4), magic number (4), msg ID (1)

	// TODO (venkat): do we need it?
	static final int MAGIC_NUMBER = 0xBADC0FFE;

	abstract void writeTo(ByteBuffer buffer) throws Exception;

	abstract int getMessageLength();

	abstract byte getID();

	// ------------------------------------------------------------------------
//	// TODO (venkat): do we need it?
//	private static ByteBuf allocateBuffer(ByteBufAllocator allocator, byte id, int contentLength) {
//		return allocateBuffer(allocator, id, 0, contentLength, true);
//	}


	private static ByteBuffer writeHeaderInfo(
		ByteBuffer buffer, RdmaMessage message) {
//		checkArgument(contentLength <= Integer.MAX_VALUE - FRAME_HEADER_LENGTH);

//		buffer.putInt(FRAME_HEADER_LENGTH + message.getMessageLength());
//		buffer.putInt(MAGIC_NUMBER);
		buffer.put(message.getID());

		return buffer;
	}

	private static byte boolToByte(boolean bool) {
		return bool ? (byte) 1 : (byte) 0;
	}

	private static boolean byteToBool(byte val) {
		return (int) val != 0;
	}
	// ------------------------------------------------------------------------
	// Server responses
	// ------------------------------------------------------------------------

	static class BufferResponse extends RdmaMessage {

		public static final byte ID = 0;
		// receiver receiverId (16), sequence number (4), backlog (4), isBuffer (1), buffer size (4)
		private final static int MESSAGE_LENGTH = /*ID*/ 1 + 16 + 4 + 4 + 1 + 4; // without content length

		final ByteBuf buffer;

		final InputChannelID receiverId;

		final int sequenceNumber;

		final int backlog;

		final boolean isBuffer;

		private BufferResponse(
			ByteBuf buffer,
			boolean isBuffer,
			int sequenceNumber,
			InputChannelID receiverId,
			int backlog) {
			this.buffer = checkNotNull(buffer);
			this.isBuffer = isBuffer;
			this.sequenceNumber = sequenceNumber;
			this.receiverId = checkNotNull(receiverId);
			this.backlog = backlog;
		}

		BufferResponse(
			Buffer buffer,
			int sequenceNumber,
			InputChannelID receiverId,
			int backlog) {
			this.buffer = checkNotNull(buffer).asByteBuf();
			this.isBuffer = buffer.isBuffer();
			this.sequenceNumber = sequenceNumber;
			this.receiverId = checkNotNull(receiverId);
			this.backlog = backlog;
		}

		boolean isBuffer() {
			return isBuffer;
		}

		ByteBuf getBuffer() {
			return buffer;
		}

		void releaseBuffer() {
			buffer.release();
		}

		// --------------------------------------------------------------------
		// Serialization
		// --------------------------------------------------------------------
		@Override
		void writeTo(ByteBuffer buffer) throws Exception {
			RdmaMessage.writeHeaderInfo(buffer, this);
			receiverId.writeTo(buffer);
			buffer.putInt(sequenceNumber);
			buffer.putInt(backlog);
			buffer.put(boolToByte(isBuffer));
			buffer.putInt(this.buffer.readableBytes());
			buffer.put(this.buffer.nioBuffer());
		}

		@Override
		int getMessageLength() {
			return MESSAGE_LENGTH + this.buffer.readableBytes();
		}

		@Override
		byte getID() {
			return ID;
		}

		static BufferResponse readFrom(ByteBuffer buffer) {
			InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);
			int sequenceNumber = buffer.getInt();
			int backlog = buffer.getInt();
			boolean isBuffer = byteToBool(buffer.get());
			int size = buffer.getInt();

			byte[] buffer1 = new byte[size];
			buffer.get(buffer1, 31, size + 31);
			// we already read 1 byte ID + 16 InputChannelID + 4 sequence number+ 4 backlog + 1 isBuffer+ 4 size . i
			// .e 30
			ByteBuf retainedSlice = Unpooled.copiedBuffer(buffer1);
			return new BufferResponse(retainedSlice, isBuffer, sequenceNumber, receiverId, backlog);
		}


	}

	static class ErrorResponse extends RdmaMessage {

		public static final byte ID = 1;
		private final static int MESSAGE_LENGTH = /*ID*/ 1 + 16 + 4 + 4 + 1 + 4; // without content length

		final Throwable cause;

		@Nullable
		final InputChannelID receiverId;

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
		void writeTo(ByteBuffer buffer) throws Exception {
			try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream oos = new
				ObjectOutputStream(baos)) {
				oos.writeObject(cause);
				buffer.put(baos.toByteArray());
				if (receiverId != null) {
					buffer.put(RdmaMessage.boolToByte(true)); // bollean true
					receiverId.writeTo(buffer);
				} else {
					buffer.put(RdmaMessage.boolToByte(false)); //boolean false
				}
				// TODO(venkat): what we need to do here?
				// Update frame length...
//				buffer.putInt(0, result.readableBytes());
//				return result;
			} catch (Throwable t) {
				buffer.clear();
				if (t instanceof IOException) {
					throw (IOException) t;
				} else {
					throw new IOException(t);
				}
			}
		}

		static ErrorResponse readFrom(ByteBuffer buffer) throws Exception {
			try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(buffer.array()))) {
				Object obj = ois.readObject();

				if (!(obj instanceof Throwable)) {
					throw new ClassCastException("Read object expected to be of type Throwable, " +
						"actual type is " + obj.getClass() + ".");
				} else {
					if (RdmaMessage.byteToBool(buffer.get())) {
						InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);
						return new ErrorResponse((Throwable) obj, receiverId);
					} else {
						return new ErrorResponse((Throwable) obj);
					}
				}
			}
		}

		@Override
		int getMessageLength() {
			return 0;
		}

		@Override
		byte getID() {
			return ID;
		}
	}

	// ------------------------------------------------------------------------
	// Client requests
	// ------------------------------------------------------------------------

	static class PartitionRequest extends RdmaMessage {

		public static final byte ID = 2;

		private final static int MESSAGE_LENGTH = /*ID*/ 1 + /*partitionId.getPartitionId()*/ 16 + /*partitionId
		.getProducerId()*/16 + /*queueIndex*/ 4 + /*receiverId*/ 16 + /*credit*/4;

		final ResultPartitionID partitionId;

		final int queueIndex;

		final InputChannelID receiverId;

		final int credit;

		PartitionRequest(ResultPartitionID partitionId, int queueIndex, InputChannelID receiverId, int credit) {
			this.partitionId = checkNotNull(partitionId);
			this.queueIndex = queueIndex;
			this.receiverId = checkNotNull(receiverId);
			this.credit = credit;
		}

		static PartitionRequest readFrom(ByteBuffer buffer) {
			ResultPartitionID partitionId =
				new ResultPartitionID(
					IntermediateResultPartitionID.fromByteBuf(buffer),
					ExecutionAttemptID.fromByteBuf(buffer));
			int queueIndex = buffer.getInt();
			InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);
			int credit = buffer.getInt();
			return new PartitionRequest(partitionId, queueIndex, receiverId, credit);
		}

		@Override
		public String toString() {
			return String.format("PartitionRequest(%s:%d:%d)", partitionId, queueIndex, credit);
		}

		@Override
		void writeTo(ByteBuffer buffer) throws Exception {
			RdmaMessage.writeHeaderInfo(buffer, this);
			partitionId.getPartitionId().writeTo(buffer);
			partitionId.getProducerId().writeTo(buffer);
			buffer.putInt(queueIndex);
			receiverId.writeTo(buffer);
			buffer.putInt(credit);
		}

		@Override
		int getMessageLength() {
			return MESSAGE_LENGTH;
		}

		@Override
		byte getID() {
			return ID;
		}
	}

	static class TaskEventRequest extends RdmaMessage {

		public static final byte ID = 3;

		private static final int MESSAGE_LENGTH = 1 + 4 + 16 + 16 + 16;

		final ByteBuffer serializedEvent;
		final TaskEvent event;

		final InputChannelID receiverId;

		final ResultPartitionID partitionId;

		TaskEventRequest(TaskEvent event, ResultPartitionID partitionId, InputChannelID receiverId) throws
			IOException {
			this.serializedEvent = EventSerializer.toSerializedEvent(checkNotNull(event));
			this.event = event;
			this.receiverId = checkNotNull(receiverId);
			this.partitionId = checkNotNull(partitionId);
		}

		@Override
		void writeTo(ByteBuffer buffer) throws Exception {
			RdmaMessage.writeHeaderInfo(buffer, this);

//			result = allocateBuffer(allocator, ID, 4 + serializedEvent.remaining() + 16 + 16 + 16);

			buffer.putInt(serializedEvent.remaining());
			buffer.put(serializedEvent);

			partitionId.getPartitionId().writeTo(buffer);
			partitionId.getProducerId().writeTo(buffer);

			receiverId.writeTo(buffer);
		}

		@Override
		int getMessageLength() {
			return MESSAGE_LENGTH + serializedEvent.remaining();
		}

		@Override
		byte getID() {
			return ID;
		}

		static TaskEventRequest readFrom(ByteBuffer buffer, ClassLoader classLoader) throws IOException {
			// directly deserialize fromNetty's buffer
			int length = buffer.getInt();
			byte[] buffer1 = new byte[length];
			buffer.get(buffer1, buffer.position(), length);
			ByteBuffer serializedEvent = ByteBuffer.wrap(buffer1);
			// assume this event's content is read from the ByteBuf (positions are not shared!)
//			buffer.readerIndex(buffer.readerIndex() + length);

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

	static class CancelPartitionRequest extends RdmaMessage {

		public static final byte ID = 4;

		final InputChannelID receiverId;

		CancelPartitionRequest(InputChannelID receiverId) {
			this.receiverId = checkNotNull(receiverId);
		}

		static CancelPartitionRequest readFrom(ByteBuf buffer) throws Exception {
			return new CancelPartitionRequest(InputChannelID.fromByteBuf(buffer));
		}

		@Override
		void writeTo(ByteBuffer buffer) throws Exception {
			RdmaMessage.writeHeaderInfo(buffer, this);
			receiverId.writeTo(buffer);
		}

		@Override
		int getMessageLength() {
			return 0;
		}

		@Override
		byte getID() {
			return 0;
		}
	}

	static class CloseRequest extends RdmaMessage {

		public static final byte ID = 5;

		private static final int MESSAGE_LENGTH = 1;

		CloseRequest() {
		}

		static CloseRequest readFrom(@SuppressWarnings("unused") ByteBuffer buffer) throws Exception {
			return new CloseRequest();
		}

		@Override
		void writeTo(ByteBuffer buffer) throws Exception {
			RdmaMessage.writeHeaderInfo(buffer, this);
		}

		@Override
		int getMessageLength() {
			return MESSAGE_LENGTH;
		}

		@Override
		byte getID() {
			return ID;
		}
	}

	//
//	/**
//	 * Incremental credit announcement from the client to the server.
//	 */
	static class AddCredit extends RdmaMessage {

		public static final byte ID = 6;

		final ResultPartitionID partitionId;

		final int credit;

		final InputChannelID receiverId;

		AddCredit(ResultPartitionID partitionId, int credit, InputChannelID receiverId) {
			checkArgument(credit > 0, "The announced credit should be greater than 0");
			this.partitionId = partitionId;
			this.credit = credit;
			this.receiverId = receiverId;
		}

		static AddCredit readFrom(ByteBuf buffer) {
			ResultPartitionID partitionId =
				new ResultPartitionID(
					IntermediateResultPartitionID.fromByteBuf(buffer),
					ExecutionAttemptID.fromByteBuf(buffer));
			int credit = buffer.readInt();
			InputChannelID receiverId = InputChannelID.fromByteBuf(buffer);

			return new AddCredit(partitionId, credit, receiverId);
		}

		@Override
		public String toString() {
			return String.format("AddCredit(%s : %d)", receiverId, credit);
		}

		@Override
		void writeTo(ByteBuffer buffer) throws Exception {
			RdmaMessage.writeHeaderInfo(buffer, this);
			partitionId.getPartitionId().writeTo(buffer);
			partitionId.getProducerId().writeTo(buffer);
			buffer.putInt(credit);
			receiverId.writeTo(buffer);
		}

		@Override
		int getMessageLength() {
			return 0;
		}

		@Override
		byte getID() {
			return ID;
		}
	}
}
