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

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.PartitionRequestClientIf;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.util.AtomicDisposableReferenceCounter;

import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.StatefulVerbCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkNotNull;

//import scala.collection.mutable.HashMap;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;

/**
 * Partition request client for remote partition requests.
 *
 * <p>This client is shared by all remote input channels, which request a partition
 * from the same {@link ConnectionID}.
 */

public class PartitionRequestClient implements PartitionRequestClientIf {

	private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestClient.class);

	private final RdmaShuffleClientEndpoint clientEndpoint;

	private final PartitionRequestClientHandler clientHandler;

	private final ConnectionID connectionId;

	private final PartitionRequestClientFactory clientFactory;

	/**
	 * If zero, the underlying TCP channel can be safely closed.
	 */

	private final AtomicDisposableReferenceCounter closeReferenceCounter = new AtomicDisposableReferenceCounter();

	PartitionRequestClient(
		RdmaShuffleClientEndpoint clientEndpoint,
		PartitionRequestClientHandler clientHandler,
		ConnectionID connectionId,
		PartitionRequestClientFactory clientFactory) {

		this.clientEndpoint = checkNotNull(clientEndpoint);
		this.clientHandler = checkNotNull(clientHandler);
		this.connectionId = checkNotNull(connectionId);
		this.clientFactory = checkNotNull(clientFactory);
	}

	boolean disposeIfNotUsed() {
		return closeReferenceCounter.disposeIfNotUsed();
	}

	/**
	 * Increments the reference counter.
	 *
	 * <p>Note: the reference counter has to be incremented before returning the
	 * instance of this client to ensure correct closing logic.
	 */
	public boolean incrementReferenceCounter() {
		return closeReferenceCounter.increment();
	}

	/**
	 * Requests a remote intermediate result partition queue.
	 *
	 * <p>The request goes to the remote producer, for which this partition
	 * request client instance has been created.
	 */
	public ChannelFuture requestSubpartition(
		final ResultPartitionID partitionId,
		final int subpartitionIndex,
		final RemoteInputChannel inputChannel,
		int delayMs) throws IOException {

		checkNotClosed();
		PartitionReaderClient readerClient = new PartitionReaderClient(partitionId, subpartitionIndex, inputChannel,
			delayMs, clientEndpoint, clientHandler);
		LOG.info(readerClient.toString());
		Thread clientReaderThread = new Thread(readerClient,"partition-client");
		clientReaderThread.start();
		// TODO (venkat): this should be done in seperate thread (see SingleInputGate.java:494)
		// input channels are iterated over, i.e; future operator has to wait for one by one completion

		return null;
	}

	/**
	 * Sends a task event backwards to an intermediate result partition producer.
	 * <p>
	 * Backwards task events flow between readers and writers and therefore
	 * will only work when both are running at the same time, which is only
	 * guaranteed to be the case when both the respective producer and
	 * consumer task run pipelined.
	 */
	public void sendTaskEvent(ResultPartitionID partitionId, TaskEvent event, final RemoteInputChannel inputChannel)
		throws IOException {
		checkNotClosed();
		LOG.info("Sending task events");
		boolean[] finished = new boolean[1];
//		NettyMessage bufferResponseorEvent = clientEndpoint.writeAndRead(new NettyMessage.TaskEventRequest(event,
//			partitionId, inputChannel.getInputChannelId()));
//
//		try {
//			clientHandler.decodeMsg(bufferResponseorEvent, false, clientEndpoint, inputChannel, finished);
//		} catch (Throwable t) {
//			LOG.error("decode failure ", t);
//		}
	}

	public void notifyCreditAvailable(RemoteInputChannel inputChannel) {
		synchronized (inputChannel){
			inputChannel.notifyAll();
		}
//		LOG.info("Credit available notification received on channel {}",inputChannel);
//		clientHandler.notifyCreditAvailable(inputChannel);
	}

	public void close(RemoteInputChannel inputChannel) throws IOException {
		if (closeReferenceCounter.decrement()) {
			// Close the TCP connection. Send a close request msg to ensure
			// that outstanding backwards task events are not discarded.
			//			tcpChannel.writeAndFlush(new NettyMessage.CloseRequest());
			try {
				LOG.info("sending client close request");
//				clientEndpoint.writeAndRead(new NettyMessage.CloseRequest()); // TODO(venkat): need clean way to write
				// close request
			} catch (Exception e) {
				LOG.error("Failed sending close request ", e);
			}
			// Make sure to remove the client from the factory
			clientFactory.destroyPartitionRequestClient(connectionId, this);
		} else {
			clientHandler.cancelRequestFor(inputChannel.getInputChannelId(), clientEndpoint);
		}
	}

	private void checkNotClosed() throws IOException {
		if (closeReferenceCounter.isDisposed()) {
			final SocketAddress localAddr = clientEndpoint.getSrcAddr();
			final SocketAddress remoteAddr = clientEndpoint.getDstAddr();
			throw new LocalTransportException(String.format("Channel to '%s' closed.", remoteAddr), localAddr);
		}
	}

	private String getEndpointStr(RdmaShuffleClientEndpoint clientEndpoint) throws Exception {
		return "src: " + clientEndpoint.getSrcAddr() + " dst: " +
			clientEndpoint.getDstAddr();
	}
}

class PartitionReaderClient implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(PartitionReaderClient.class);
	private ResultPartitionID partitionId;
	private int subpartitionIndex;
	private RemoteInputChannel inputChannel;
	private int delayMs;
	private final RdmaShuffleClientEndpoint clientEndpoint;
	private final PartitionRequestClientHandler clientHandler;
	private final Map<Long,ByteBuf> receivedBuffers = new HashMap<>();
//	ArrayDeque<ByteBuf> receivedBuffers = new ArrayDeque<>();
	Map<Long,ByteBuf> inFlight = new HashMap<Long,ByteBuf>();
//	private long workRequestId;

	public PartitionReaderClient(final ResultPartitionID partitionId,
								 final int subpartitionIndex,
								 final RemoteInputChannel inputChannel,
								 int delayMs, RdmaShuffleClientEndpoint clientEndpoint, final
								 PartitionRequestClientHandler clientHandler) {
		this.partitionId = partitionId;
		this.subpartitionIndex = subpartitionIndex;
		this.inputChannel = inputChannel;
		this.clientHandler = clientHandler;
		this.delayMs = delayMs;
		this.clientEndpoint = clientEndpoint;
	}

	private int postBuffers(int credit) {
		int failed = 0;
		for (int c = 0; c < credit; c++) {
			ByteBuf receiveBuffer = null;
			try {
//			ByteBuf receiveBuffer = (NetworkBuffer)inputChannel.getBufferProvider().requestBuffer();
				// Should be taken from buffer queue for the crediting to work, otherwise buffers may not be added to
				// the credit if there is no backlog
				receiveBuffer = (NetworkBuffer) inputChannel.requestBuffer();
				if (receiveBuffer != null) {
					long id = clientEndpoint.workRequestId.incrementAndGet();
					RdmaSendReceiveUtil.postReceiveReqWithChannelBuf(clientEndpoint,id, receiveBuffer);
					receivedBuffers.put(id,receiveBuffer);
				} else {
					LOG.error("Buffer from the channel is null");
				}
			} catch (IOException e) {
				LOG.error("failed posting buffer. current credit {} due to {}", c, e);
				failed++;
				receiveBuffer.release();
			}
		}
		return failed;
	}

	@Override
	public void run() {
		int i = 0;
		boolean[] finished = new boolean[1];
		// given the number of buffers configured for network low, it is set to 10 but should be configurable.
		int availableCredit = inputChannel.getInitialCredit();
		int failed = postBuffers(availableCredit);
//		boolean canSendCredit= false; // server will send the flag with message
		NettyMessage msg = new NettyMessage.PartitionRequest(
			partitionId, subpartitionIndex, inputChannel.getInputChannelId(), inputChannel.getInitialCredit() -
			failed);
		ByteBuf buf;
		try {
			buf = msg.write(clientEndpoint.getNettyBufferpool());
			clientEndpoint.getSendBuffer().put(buf.nioBuffer());
			RdmaSendReceiveUtil.postSendReq(clientEndpoint, clientEndpoint.workRequestId.incrementAndGet());
		} catch (Exception ioe) {
			LOG.error("Failed to serialize partition request {}",ioe);
			return;
		}
		do {
			try {
				// TODO: send credit if credit is reached zero
//			for (int i=0;i<takeEventsCount;i++) {
//				long startTime = System.nanoTime();
				if (availableCredit<=0) {
					if (inputChannel.getUnannouncedCredit() > 0) {
						int unannouncedCredit = inputChannel.getAndResetUnannouncedCredit();
//						LOG.info("Adding credit: {} on channel {}", unannouncedCredit, inputChannel);
						failed = postBuffers(unannouncedCredit);
						msg = new NettyMessage.AddCredit(
							inputChannel.getPartitionId(),
							unannouncedCredit - failed,
							inputChannel.getInputChannelId());
						availableCredit += unannouncedCredit;
						LOG.info("Announced available credit: {} on {}",availableCredit,clientEndpoint.getEndpointStr());
						ByteBuf message = msg.write(clientEndpoint.getNettyBufferpool());
						// TODO: lurcking bug, if credit posted before sending out the previous credit, we might hold
						clientEndpoint.getSendBuffer().put(message.nioBuffer());
						long workID = clientEndpoint.workRequestId.incrementAndGet();
						inFlight.put(workID,message);
						RdmaSendReceiveUtil.postSendReq(clientEndpoint, workID);
					} else {
//						LOG.info("No credit available on channel {}",availableCredit,inputChannel);
							// wait for the credit to be available, otherwise connection stucks in blocking
							synchronized (inputChannel) {
								inputChannel.wait();
							}
							continue;
					}
				}

				IbvWC wc = clientEndpoint.getWcEvents().take();
//				inFlightVerbs.remove(wc.getWr_id()).free();
//				LOG.info("took client event with wr_id {} on endpoint {}", wc.getWr_id(), clientEndpoint.getEndpointStr());
				if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_RECV) {
					if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
						LOG.error("Receive posting failed. reposting new receive request");
					} else {
						// InfiniBand completes requests in FIFO, so we should have first buffer filled with the data
						ByteBuf receiveBuffer = receivedBuffers.get(wc.getWr_id());
//						availableCredit--;
						receiveBuffer.readerIndex();
						int segmentSize = ((NetworkBuffer) receiveBuffer).getMemorySegment().size();
						receiveBuffer.writerIndex(segmentSize);
						receiveBuffer.readerIndex();
//						  discard frame length
						int magic = receiveBuffer.readInt();
						if (magic != NettyMessage.MAGIC_NUMBER) {
							LOG.error("Magic number mistmatch expected: {} got: {} on receiver {}", NettyMessage
								.MAGIC_NUMBER, magic, inputChannel.getInputChannelId());
							receiveBuffer.release();
						} else {
							byte ID = receiveBuffer.readByte();
							switch (ID) {
								case NettyMessage.BufferResponse.ID:
									NettyMessage.BufferResponse bufferOrEvent = NettyMessage.BufferResponse.readFrom(receiveBuffer);
									availableCredit = bufferOrEvent.availableCredit;
//									canSendCredit = bufferOrEvent.canRecvCredit;
//									LOG.info("Receive complete: " + wc.getWr_id() + "buff address: "+ receiveBuffer.memoryAddress() + " seq:" + bufferOrEvent
//										.sequenceNumber + " receiver id " + bufferOrEvent.receiverId + " backlog: " +
//										bufferOrEvent.backlog);
									clientHandler.decodeMsg(bufferOrEvent,
										false, clientEndpoint, inputChannel, finished);
									break;
								case NettyMessage.CloseRequest.ID:
									LOG.info("closing on client side upon close request. Something might have gone " +
										"wrong on server (reader released etc)");

									clientHandler.decodeMsg(EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE),
										false, clientEndpoint, inputChannel, finished);
								default:
									LOG.error(" Un-identified response type " + ID);
							}
						}
					}
				} else if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_SEND) {
					if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
						LOG.error("Client: Send failed. reposting new send request request " + clientEndpoint
							.getEndpointStr());
					}
					ByteBuf sendBufNetty =inFlight.get(wc.getWr_id());
					if (sendBufNetty!=null){
						sendBufNetty.release();
					}
					clientEndpoint.getSendBuffer().clear();
				} else {
					LOG.error("failed to match any condition " + wc.getOpcode());
				}
//			}
			} catch (Throwable e) {
				try {
					LOG.error("failed client read " + clientEndpoint.getEndpointStr(), e);
				} catch (Exception e1) {
					LOG.error("failed get endpoint", e);
				}
			}
		}
		while (!inputChannel.isReleased() && !finished[0]); // TODO(venkat): we should close the connection on reaching
		// EndOfPartitionEvent
		// waiting would make the partitionRequest being posted after EndOfPartitionEvent. This would hang server
		// thread
		// waiting for more data.
	}

	@Override
	public String toString() {
		return "Reading subpartition " + subpartitionIndex + " partition Index " + partitionId + " at endpoint " +
			clientEndpoint.getEndpointStr() + " remote channel " + inputChannel;
	}
}
