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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.rdma.NettyMessage.BufferResponse;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.rdma.NettyMessage.ErrorResponse;
import org.apache.flink.runtime.io.network.partition.ProducerFailedException;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;
//import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
//import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
//import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
//import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
//import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;


/**
 * A nonEmptyReader of partition queues, which listens for channel writability changed
 * events before writing and flushing {@link Buffer} instances.
 */
class PartitionRequestQueue {

	private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestQueue.class);

	/** The readers which are already enqueued available for transferring data. */
	private final ConcurrentLinkedDeque<NetworkSequenceViewReader> availableReaders = new ConcurrentLinkedDeque<>();

	/** All the readers created for the consumers' partition requests. */
	private final ConcurrentMap<InputChannelID, NetworkSequenceViewReader> allReaders = new ConcurrentHashMap<>();

	private final Set<InputChannelID> released = Sets.newHashSet();

	private boolean fatalError;

//	private ChannelHandlerContext ctx;
//
//	@Override
//	public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
//		if (this.ctx == null) {
//			this.ctx = ctx;
//		}
//
//		super.channelRegistered(ctx);
//	}

	void notifyReaderNonEmpty(final NetworkSequenceViewReader reader) {
		enqueueAvailableReader(reader);
	}

	/**
	 * Try to enqueue the reader once receiving credit notification from the consumer or receiving
	 * non-empty reader notification from the producer.
	 *
	 * <p>NOTE: Only one thread would trigger the actual enqueue after checking the reader's
	 * availability, so there is no race condition here.
	 */
	private synchronized void enqueueAvailableReader(final NetworkSequenceViewReader reader) {
		if (reader.isRegisteredAsAvailable() || !reader.isAvailable()) {
			return;
		}
		registerAvailableReader(reader);
	}

	/**
	 * Accesses internal state to verify reader registration in the unit tests.
	 *
	 * <p><strong>Do not use anywhere else!</strong>
	 *
	 * @return readers which are enqueued available for transferring data
	 */
	@VisibleForTesting
	ConcurrentLinkedDeque<NetworkSequenceViewReader> getAvailableReaders() {
		return availableReaders;
	}

	public void notifyReaderCreated(final NetworkSequenceViewReader reader) {
		allReaders.put(reader.getReceiverId(), reader);
	}

	public void cancel(InputChannelID receiverId) {
//		ctx.pipeline().fireUserEventTriggered(receiverId);
	}

	public void close() throws IOException {
//		if (ctx != null) {
//			ctx.channel().close();
//		}

		for (NetworkSequenceViewReader reader : allReaders.values()) {
			reader.notifySubpartitionConsumed();
			reader.releaseAllResources();
			markAsReleased(reader.getReceiverId());
		}
		allReaders.clear();
	}

	/**
	 * Adds unannounced credits from the consumer and enqueues the corresponding reader for this
	 * consumer (if not enqueued yet).
	 *
	 * @param receiverId The input channel id to identify the consumer.
	 * @param credit The unannounced credits of the consumer.
	 */
	void addCredit(InputChannelID receiverId, int credit) throws Exception {
		if (fatalError) {
			return;
		}

		NetworkSequenceViewReader reader = allReaders.get(receiverId);
		if (reader != null) {
				reader.addCredit(credit);
				enqueueAvailableReader(reader);
		} else {
			throw new IllegalStateException("No reader for receiverId = " + receiverId + " exists.");
		}
	}

	public void tryEnqueueReader(){
		for (NetworkSequenceViewReader reader : allReaders.values()) {
			enqueueAvailableReader(reader);
		}
	}

//	@Override
//	public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
//		// The user event triggered event loop callback is used for thread-safe
//		// hand over of reader queues and cancelled producers.
//
//		if (msg instanceof NetworkSequenceViewReader) {
//			enqueueAvailableReader((NetworkSequenceViewReader) msg);
//		} else if (msg.getClass() == InputChannelID.class) {
//			// Release partition view that get a cancel request.
//			InputChannelID toCancel = (InputChannelID) msg;
//			if (released.contains(toCancel)) {
//				return;
//			}
//
//			// Cancel the request for the input channel
//			int size = availableReaders.size();
//			for (int i = 0; i < size; i++) {
//				NetworkSequenceViewReader reader = pollAvailableReader();
//				if (reader.getReceiverId().equals(toCancel)) {
//					reader.releaseAllResources();
//					markAsReleased(reader.getReceiverId());
//				} else {
//					registerAvailableReader(reader);
//				}
//			}
//
//			allReaders.remove(toCancel);
//		} else {
//			ctx.fireUserEventTriggered(msg);
//		}
//	}

//	@Override
//	public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
//		writeAndFlushNextMessageIfPossible(ctx.channel());
//	}
//
	public NettyMessage getResponseMessage() throws IOException {
//		if (fatalError || !channel.isWritable()) {
//			return;
//		}

		// The logic here is very similar to the combined input gate and local
		// input channel logic. You can think of this class acting as the input
		// gate and the consumed views as the local input channels.

		BufferAndAvailability next = null;
		try {
			while (true) {

					NetworkSequenceViewReader reader = pollAvailableReader();

					// No queue with available data. We allow this here, because
					// of the write callbacks that are executed after each write.
					if (reader == null) {
						return null;
					}
				synchronized (reader) {
					// Reader poll resets the status of registered without decrementing the credit.
					// So, adding and enqueuing should be done as atomic. So that, there will not be
					// duplicate enqueue for same credit.
					reader.setRegisteredAsAvailable(false);
					next = reader.getNextBuffer();
				}
				if (next == null) {
					if (!reader.isReleased()) {
						continue;
					}
					markAsReleased(reader.getReceiverId());

					Throwable cause = reader.getFailureCause();
					if (cause != null) {
						ErrorResponse msg = new ErrorResponse(
							new ProducerFailedException(cause),
							reader.getReceiverId());

						return msg;
					}
				} else {
					// This channel was now removed from the available reader queue.
					// We re-add it into the queue if it is still available
					if (next.moreAvailable()) {
						// make sure we enqueue only if credit available
						enqueueAvailableReader(reader);
//						reader.setRegisteredAsAvailable(true);
					}

					BufferResponse msg = new BufferResponse(
						next.buffer(),
						reader.getSequenceNumber(),
						reader.getReceiverId(),
						next.buffersInBacklog(),reader.getAvailableCredit());

					// Write and flush and wait until this is done before
					// trying to continue with the next buffer.
					return msg;

				}
			}
		} catch (Throwable t) {
			if (next != null) {
				next.buffer().recycleBuffer();
			}

			throw new IOException(t.getMessage(), t);
		}
	}

	private void registerAvailableReader(NetworkSequenceViewReader reader) {
		// Reader should be set availble first.
		// otherwie, writer may write out data on add(reader) and reader as unavailable.
		// Later, credit thread will set this reader as availble.
		// Leaving it forever in available state, but the available readers will be empty.
		reader.setRegisteredAsAvailable(true);
		availableReaders.add(reader);
	}

	@Nullable
	private NetworkSequenceViewReader pollAvailableReader() {
		NetworkSequenceViewReader reader = availableReaders.poll();
		return reader;
	}

//	@Override
//	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
//		releaseAllResources();
//
//		ctx.fireChannelInactive();
//	}
//
//	@Override
//	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
//		handleException(ctx.channel(), cause);
//	}
//
//	private void handleException(Channel channel, Throwable cause) throws IOException {
//		LOG.error("Encountered error while consuming partitions", cause);
//
//		fatalError = true;
//		releaseAllResources();
//
//		if (channel.isActive()) {
//			channel.writeAndFlush(new ErrorResponse(cause)).addListener(ChannelFutureListener.CLOSE);
//		}
//	}

	public void releaseAllResources() throws IOException {
		// note: this is only ever executed by one thread: the Netty IO thread!
		for (NetworkSequenceViewReader reader : allReaders.values()) {
			reader.releaseAllResources();
			markAsReleased(reader.getReceiverId());
		}

		availableReaders.clear();
		allReaders.clear();
	}

	/**
	 * Marks a receiver as released.
	 */
	private void markAsReleased(InputChannelID receiverId) {
		released.add(receiverId);
	}

	// This listener is called after an element of the current nonEmptyReader has been
	// flushed. If successful, the listener triggers further processing of the
	// queues.
//	private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {
//
//		@Override
//		public void operationComplete(ChannelFuture future) throws Exception {
//			try {
//				if (future.isSuccess()) {
//					writeAndFlushNextMessageIfPossible(future.channel());
//				} else if (future.cause() != null) {
//					handleException(future.channel(), future.cause());
//				} else {
//					handleException(future.channel(), new IllegalStateException("Sending cancelled by user."));
//				}
//			} catch (Throwable t) {
//				handleException(future.channel(), t);
//			}
//		}
//	}
}

