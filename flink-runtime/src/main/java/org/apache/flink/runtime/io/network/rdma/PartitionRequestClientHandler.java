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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.ReadOnlySlicedNetworkBuffer;
import org.apache.flink.runtime.io.network.rdma.exception.RemoteTransportException;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

/**
 * Channel handler to read the messages of buffer response or error response from the
 * producer.
 *
 * <p>It is used in the old network mode.
 */

class PartitionRequestClientHandler {

	private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestClientHandler.class);

	private final ConcurrentMap<InputChannelID, RemoteInputChannel> inputChannels = new
		ConcurrentHashMap<InputChannelID, RemoteInputChannel>();

	private final AtomicReference<Throwable> channelError = new AtomicReference<Throwable>();

//	private final BufferListenerTask bufferListener = new BufferListenerTask();

	private final Queue<Object> stagedMessages = new ArrayDeque<Object>();

//	private final StagedMessagesHandlerTask stagedMessagesHandler = new StagedMessagesHandlerTask();

	/**
	 * Set of cancelled partition requests. A request is cancelled iff an input channel is cleared
	 * while data is still coming in for this channel.
	 */
	private final ConcurrentMap<InputChannelID, InputChannelID> cancelled = Maps.newConcurrentMap();
//	private volatile ChannelHandlerContext ctx;

	// ------------------------------------------------------------------------
	// Input channel/receiver registration
	// ------------------------------------------------------------------------

	public void cancelRequestFor(InputChannelID inputChannelId, RdmaShuffleClientEndpoint clientEndpoint) {
		if (inputChannelId == null) {
			return;
		}
//TODO (venkat): imp: handle cancellation
		if (cancelled.putIfAbsent(inputChannelId, inputChannelId) == null) {
//			clientEndpoint.write(new CancelPartitionRequest(inputChannelId));
		}
	}

	//	@Override
//	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//		try {
//			// TODO (venkat): imp: what are staged messages?
//			if (!bufferListener.hasStagedBufferOrEvent() && stagedMessages.isEmpty()) {
//				decodeMsg(msg, false);
//			}
//			else {
//				stagedMessages.add(msg);
//			}
//		}
//		catch (Throwable t) {
//			notifyAllChannelsOfErrorAndClose(t);
//		}
//	}
//
	public void addInputChannel(RemoteInputChannel listener) throws IOException {
		inputChannels.putIfAbsent(listener.getInputChannelId(), listener);
	}
	private void notifyAllChannelsOfErrorAndClose(Throwable cause, RdmaShuffleClientEndpoint clientEndpoint) {
		System.out.println("Error ---->");
		System.out.println(cause.getMessage());
		try {
			clientEndpoint.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	// ------------------------------------------------------------------------
	public void decodeMsg(Object msg, boolean isStagedBuffer, RdmaShuffleClientEndpoint clientEndpoint,
						  RemoteInputChannel inputChannel, Map<InputChannelID, RemoteInputChannel> inputChannels) throws
		Throwable {
		final Class<?> msgClazz = msg.getClass();

		// ---- Buffer --------------------------------------------------------
		if (msgClazz == NettyMessage.BufferResponse.class) {
			NettyMessage.BufferResponse bufferOrEvent = (NettyMessage.BufferResponse) msg;

//			RemoteInputChannel inputChannel = inputChannels.get(bufferOrEvent.receiverId);
			if (inputChannel == null) {
				bufferOrEvent.releaseBuffer();

				cancelRequestFor(bufferOrEvent.receiverId, clientEndpoint);

			}

			decodeBufferOrEvent(inputChannel, bufferOrEvent, isStagedBuffer, clientEndpoint,inputChannels);
		}
		// ---- Error ---------------------------------------------------------
		else if (msgClazz == NettyMessage.ErrorResponse.class) {
			NettyMessage.ErrorResponse error = (NettyMessage.ErrorResponse) msg;

//			SocketAddress remoteAddr = ctx.channel().remoteAddress();

			if (error.isFatalError()) {
				notifyAllChannelsOfErrorAndClose(new RemoteTransportException(
					"Fatal error at remote task manager '" + clientEndpoint.getSrcAddr() + "'.",
					clientEndpoint.getDstAddr(), error.cause), clientEndpoint);
			} else {
//				RemoteInputChannel inputChannel = inputChannels.get(error.receiverId);

				if (inputChannel != null) {
					if (error.cause.getClass() == PartitionNotFoundException.class) {
						inputChannel.onFailedPartitionRequest();
					} else {
						notifyAllChannelsOfErrorAndClose(new RemoteTransportException(
							"Error at remote task manager '" + clientEndpoint.getSrcAddr() + "'.",
							clientEndpoint.getDstAddr(), error.cause), clientEndpoint);
					}
				}
			}
		} else {
			throw new IllegalStateException("Received unknown message from producer: " + msg.getClass());
		}

//		return true;
	}

	private void decodeBufferOrEvent(RemoteInputChannel inputChannel, NettyMessage.BufferResponse bufferOrEvent,
									 boolean isStagedBuffer, RdmaShuffleClientEndpoint clientEndpoint, Map
										 <InputChannelID, RemoteInputChannel> inputChannels) throws
		Throwable {
//		boolean releaseNettyBuffer = true;

		try {
//			ByteBuf nettyBuffer = bufferOrEvent.getNettyBuffer();
			final int receivedSize = bufferOrEvent.getSizeOfRetainedSlice();
			if (bufferOrEvent.isBuffer()) {
				// ---- Buffer ------------------------------------------------
				// Early return for empty buffers. Otherwise Netty's readBytes() throws an
				// IndexOutOfBoundsException.
				if (bufferOrEvent.getSizeOfRetainedSlice() == 0) {
					inputChannel.onEmptyBuffer(bufferOrEvent.sequenceNumber, -1);
				}else {
//					LOG.info("onBuffer " + bufferOrEvent.sequenceNumber + " receiver id " + bufferOrEvent.receiverId+ " backlog: "+bufferOrEvent.backlog);
					inputChannel.onBuffer(bufferOrEvent.getResponseReadonlySlice(), bufferOrEvent.sequenceNumber, bufferOrEvent.backlog);
				}
				//boolean readFinished= false;
//				do {
//					Buffer buffer = bufferProvider.requestBuffer();
//					if (buffer != null) {
//						nettyBuffer.readBytes(buffer.asByteBuf(), receivedSize)
//						readFinished = true;
//					}
////					else if (bufferListener.waitForBuffer(bufferProvider, bufferOrEvent)) {
////						releaseNettyBuffer = false;
////
////						return false;
////					}
//					else if (bufferProvider.isDestroyed()) {
//						LOG.info("buffer provider is destroyed");
//					}
//				} while (!readFinished);
			} else {
//				LOG.info("in event");


				// ---- Event -------------------------------------------------
				// TODO We can just keep the serialized data in the Netty buffer and release it later at the reader
				// SingleInputGate.java getNextBufferOrEvent() has a weired way of checking buffer or event
				// if it is not wrapped memorySegment, then it is considered as buffer. WTF, spent hours debugging it.
				// see how ReadOnlySlicedNetworkBuffer isBuffer implemented
				//	@Override
				//	public boolean isBuffer() {
				//		return getBuffer().isBuffer();
				//	}
				ReadOnlySlicedNetworkBuffer buffer=(ReadOnlySlicedNetworkBuffer)bufferOrEvent.getResponseReadonlySlice();
				byte[] byteArray = new byte[receivedSize];
				buffer.getBytes(buffer.getReaderIndex(),byteArray);
//				nettyBuffer.readBytes(byteArray);
				MemorySegment memSeg = MemorySegmentFactory.wrap(byteArray);
				Buffer networkBuffer = new NetworkBuffer(memSeg, FreeingBufferRecycler.INSTANCE, false, receivedSize);
				inputChannel.onBuffer(networkBuffer, bufferOrEvent.sequenceNumber, -1);
				buffer.release();
				final AbstractEvent event = EventSerializer.fromBuffer(networkBuffer, getClass().getClassLoader());
				if (event.getClass()==EndOfPartitionEvent.class){
					RemoteInputChannel remoteInputChannel = inputChannels.remove(inputChannel.getInputChannelId());
					LOG.info("Received EndOfPartitionEvent {}",remoteInputChannel);
				}else{
					LOG.info("Received event {}",event.getClass().getSimpleName());
				}
//				bufferOrEvent.releaseBuffer();
			}
		} finally {
//			if (releaseNettyBuffer) { RDMA code does not have a copy, so this buffer should be release after processing
			// by the serializer.
//				bufferOrEvent.releaseBuffer();
//			}
		}
	}
}
