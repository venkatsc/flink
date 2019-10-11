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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.concurrent.ArrayBlockingQueue;

import static org.apache.flink.util.Preconditions.checkNotNull;

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
//		try {
//			LOG.info("posting partition request against "+getEndpointStr(clientEndpoint));
//			final NettyMessage.PartitionRequest request = new NettyMessage.PartitionRequest(
//				partitionId, subpartitionIndex, inputChannel.getInputChannelId(), inputChannel.getInitialCredit());
//			NettyMessage bufferResponseorEvent = clientEndpoint.writeAndRead(request);
//			LOG.info("partition request completed on "+ getEndpointStr(clientEndpoint));
//			if (bufferResponseorEvent != null) {
//				Class<?> msgClazz = bufferResponseorEvent.getClass();
//				if (msgClazz == NettyMessage.BufferResponse.class) {
//					LOG.info("got partition response from endpoint: " + getEndpointStr(clientEndpoint));
//					NettyMessage.BufferResponse bufferOrEvent = (NettyMessage.BufferResponse) bufferResponseorEvent;
//					try {
//						// TODO (venkat): decode the message
//						clientHandler.decodeMsg(bufferOrEvent, false, clientEndpoint, inputChannel);
//					} catch (Throwable t) {
//						LOG.error("decode failure ", t);
//					}
//				} else {
//					LOG.info("received message type is not handled " + msgClazz.toString());
//				}
//			} else {
//				LOG.error("received partition response is null and it should never be the case");
//			}
//		}catch (Exception e){
//			LOG.error("failed client ",e);
//			throw new IOException(e);
//		}

		PartitionReaderClient readerClient = new PartitionReaderClient(partitionId, subpartitionIndex, inputChannel,
			delayMs, clientEndpoint, clientHandler);
		Thread clientReaderThread = new Thread(readerClient);
		clientReaderThread.start();

		// TODO (venkat): this should be done in seperate thread (see SingleInputGate.java:494)
		// input channels are iterated over, i.e; future operator has to wait for one by one completion
		LOG.info("Requesting subpartition {} of partition {} with {} ms delay using reader client {}.",
			subpartitionIndex, partitionId, delayMs,readerClient.toString());

//		clientHandler.addInputChannel(inputChannel);


//		LOG.info("returned from partition request");
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
        boolean[] finished= new boolean[1];
		NettyMessage bufferResponseorEvent = clientEndpoint.writeAndRead(new NettyMessage.TaskEventRequest(event,
			partitionId, inputChannel.getInputChannelId()));

		try {
			clientHandler.decodeMsg(bufferResponseorEvent, false, clientEndpoint, inputChannel,finished);
		} catch (Throwable t) {
			LOG.error("decode failure ", t);
		}
	}

	public void notifyCreditAvailable(RemoteInputChannel inputChannel) {
		LOG.info("Credit available notification received on channel {}",inputChannel);
//		clientHandler.notifyCreditAvailable(inputChannel);
	}

	public void close(RemoteInputChannel inputChannel) throws IOException {
		if (closeReferenceCounter.decrement()) {
			// Close the TCP connection. Send a close request msg to ensure
			// that outstanding backwards task events are not discarded.
			//			tcpChannel.writeAndFlush(new NettyMessage.CloseRequest());
			try {
				LOG.info("sending client close request");
				clientEndpoint.writeAndRead(new NettyMessage.CloseRequest()); // TODO(venkat): need clean way to write
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
	ArrayDeque<ByteBuf> receivedBuffers = new ArrayDeque<>();
	private long workRequestId;

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

	private void postBuffers(int credit) throws IOException {
		for (int c=0;c<credit;c++){
//			ByteBuf receiveBuffer = (NetworkBuffer)inputChannel.getBufferProvider().requestBuffer();
			ByteBuf receiveBuffer = (NetworkBuffer)inputChannel.requestBuffer();
			if (receiveBuffer!=null){
				receivedBuffers.addLast(receiveBuffer);
				RdmaSendReceiveUtil.postReceiveReqWithChannelBuf(clientEndpoint, ++workRequestId,receiveBuffer);
			}else {
				LOG.error("Buffer from the channel is null");
			}
		}
	}

	@Override
	public void run() {
		int i = 0;
		boolean[] finished = new boolean[1];
		NettyMessage msg = new NettyMessage.PartitionRequest(
			partitionId, subpartitionIndex, inputChannel.getInputChannelId(), inputChannel.getInitialCredit());
		ByteBuf buf;
		// given the number of buffers configured for network low, it is set to 10 but should be configurable.
		int availableCredit=inputChannel.getInitialCredit();
		try {
			postBuffers(availableCredit);
			buf = msg.write(clientEndpoint.getNettyBufferpool());
			clientEndpoint.getSendBuffer().put(buf.nioBuffer());
			RdmaSendReceiveUtil.postSendReq(clientEndpoint, ++workRequestId);
		} catch (Exception ioe) {
			LOG.error("Failed to serialize partition request");
			return;
		}
		do {
			try {
				// TODO: send credit if credit is reached zero
//			for (int i=0;i<takeEventsCount;i++) {
				if (availableCredit==0){
					if (inputChannel.getUnannouncedCredit()>0) {
						availableCredit = inputChannel.getAndResetUnannouncedCredit();
						LOG.info("Adding credit: {} on channel {}",availableCredit,inputChannel);
						postBuffers(availableCredit);
						msg = new NettyMessage.AddCredit(
							inputChannel.getPartitionId(),
							availableCredit,
							inputChannel.getInputChannelId());
						clientEndpoint.getSendBuffer().put(msg.write(clientEndpoint.getNettyBufferpool()).nioBuffer());
						RdmaSendReceiveUtil.postSendReq(clientEndpoint, ++workRequestId);
					}else{
//						LOG.info("No credit available on channel {}",availableCredit,inputChannel);
						continue;
					}
				}
				IbvWC wc = clientEndpoint.getWcEvents().take();
//				LOG.info("Took completion event with work id {} ", wc.getWr_id());
				if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_RECV) {
					if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
						LOG.error("Receive posting failed. reposting new receive request");
//						RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId);
					} else {
						// InfiniBand completes requests in FIFO, so we should have first buffer filled with the data
						ByteBuf receiveBuffer = receivedBuffers.pollFirst();
//						if (receiveBuffer.refCnt()==2) {
//							receiveBuffer.release();
//						}else if (receiveBuffer.refCnt()>2){
//							LOG.info("Receive buffer ref count {}",receiveBuffer.refCnt());
//						}
						availableCredit--;
						// first receive succeeded. Read the data and repost the next message
						// since RDMA writes to the direct memory, receiver buffer indexes starts at 0
						// resulting in IndexOutOfBoundsException. To make it work, we need to set index to
						// max segment size
						receiveBuffer.writerIndex(((NetworkBuffer) receiveBuffer).getMemorySegment().size());
						receiveBuffer.readInt(); // discard frame length
						int magic = receiveBuffer.readInt();
						if (magic!= NettyMessage.MAGIC_NUMBER){
							LOG.error("Magic number mistmatch expected: {} got: {} on receiver {}",NettyMessage.MAGIC_NUMBER,magic,inputChannel.getInputChannelId());
							// discard magic number
						}
						byte ID = receiveBuffer.readByte();
						switch (ID) {
							case NettyMessage.BufferResponse.ID:
								clientHandler.decodeMsg(NettyMessage.BufferResponse.readFrom(receiveBuffer), false, clientEndpoint, inputChannel,finished);
//								int refCount= receiveBuffer.refCnt();
								break;
							case NettyMessage.CloseRequest.ID:
								LOG.info("closing on client side upon close request. Something might have gone wrong on server (reader released etc)");
								clientHandler.decodeMsg(EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE),false, clientEndpoint, inputChannel,finished);
							default:
								LOG.error(" Un-identified response type " + ID);
						}
//						receiveBuffer = (ByteBuf)inputChannel.getBufferProvider().requestBuffer();
//						clientEndpoint.getReceiveBuffer().clear();
//						RdmaSendReceiveUtil.postReceiveReqWithChannelBuf(clientEndpoint, ++workRequestId,receiveBuffer);
						//Post next request
//						clientEndpoint.getSendBuffer().clear();

						// TODO: send credit
//						clientEndpoint.getSendBuffer().put(buf.nioBuffer());
//						RdmaSendReceiveUtil.postSendReq(clientEndpoint, ++workRequestId);
					}
				} else if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_SEND) {
					if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
						LOG.error("Client: Send failed. reposting new send request request "+clientEndpoint.getEndpointStr());
//						RdmaSendReceiveUtil.postSendReq(clientEndpoint, ++workRequestId);
					}
					clientEndpoint.getSendBuffer().clear();
//					if (buf.refCnt()>1) {
//						buf.release();
//					}
					// Send succeed does not require any action
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
//			throw new IOException(e);
			}
//			NettyMessage bufferResponseorEvent = clientEndpoint.writeAndRead(request);
//			LOG.info("partition request completed ",inputChannel);
//			if (bufferResponseorEvent != null) {
//				Class<?> msgClazz = bufferResponseorEvent.getClass();
//				if (msgClazz == NettyMessage.BufferResponse.class) {
//					LOG.info("got partition response {}", inputChannel);
//					NettyMessage.BufferResponse bufferOrEvent = (NettyMessage.BufferResponse) bufferResponseorEvent;
//					try {
//						// TODO (venkat): decode the message
//						clientHandler.decodeMsg(bufferOrEvent, false, clientEndpoint, inputChannel);
//					} catch (Throwable t) {
//						LOG.error("decode failure ", t);
//					}
//				} else {
//					LOG.info("received message type is not handled " + msgClazz.toString());
//				}
//			} else {
//				LOG.error("received partition response is null and it should never be the case");
//			}
		}
		while (!inputChannel.isReleased()&& !finished[0]); // TODO(venkat): we should close the connection on reaching
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
