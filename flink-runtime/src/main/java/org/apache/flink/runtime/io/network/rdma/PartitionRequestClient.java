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
import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.util.AtomicDisposableReferenceCounter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;

import static org.apache.flink.util.Preconditions.checkNotNull;

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
		try {
			LOG.info("posting partition request against "+getEndpointStr(clientEndpoint));
			final NettyMessage.PartitionRequest request = new NettyMessage.PartitionRequest(
				partitionId, subpartitionIndex, inputChannel.getInputChannelId(), inputChannel.getInitialCredit());
			NettyMessage bufferResponseorEvent = clientEndpoint.writeAndRead(request);
			LOG.info("partition request completed on "+ getEndpointStr(clientEndpoint));
			if (bufferResponseorEvent != null) {
				Class<?> msgClazz = bufferResponseorEvent.getClass();
				if (msgClazz == NettyMessage.BufferResponse.class) {
					LOG.info("got partition response from endpoint: " + getEndpointStr(clientEndpoint));
					NettyMessage.BufferResponse bufferOrEvent = (NettyMessage.BufferResponse) bufferResponseorEvent;
					try {
						// TODO (venkat): decode the message
						clientHandler.decodeMsg(bufferOrEvent, false, clientEndpoint, inputChannel);
					} catch (Throwable t) {
						LOG.error("decode failure ", t);
					}
				} else {
					LOG.info("received message type is not handled " + msgClazz.toString());
				}
			} else {
				LOG.error("received partition response is null and it should never be the case");
			}
		}catch (Exception e){
			LOG.error("failed client ",e);
		}

//		PartitionReaderClient readerClient = new PartitionReaderClient(partitionId, subpartitionIndex, inputChannel,delayMs,clientEndpoint,clientHandler );
//		Thread clientReaderThread = new Thread(readerClient);
//		clientReaderThread.start();

		// TODO (venkat): this should be done in seperate thread (see SingleInputGate.java:494)
		// input channels are iterated over, i.e; future operator has to wait for one by one completion
		LOG.debug("Requesting subpartition {} of partition {} with {} ms delay.",
			subpartitionIndex, partitionId, delayMs);

//		clientHandler.addInputChannel(inputChannel);


		LOG.info("returned from partition request");
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

		NettyMessage bufferResponseorEvent = clientEndpoint.writeAndRead(new NettyMessage.TaskEventRequest(event,
			partitionId, inputChannel.getInputChannelId()));

		try {
			clientHandler.decodeMsg(bufferResponseorEvent, false, clientEndpoint, inputChannel);
		} catch (Throwable t) {
			LOG.error("decode failure ", t);
		}
	}

	public void notifyCreditAvailable(RemoteInputChannel inputChannel) {
//		clientHandler.notifyCreditAvailable(inputChannel);
	}

	public void close(RemoteInputChannel inputChannel) throws IOException {
		if (closeReferenceCounter.decrement()) {
			// Close the TCP connection. Send a close request msg to ensure
			// that outstanding backwards task events are not discarded.
			//			tcpChannel.writeAndFlush(new NettyMessage.CloseRequest());
			try{
			 clientEndpoint.writeAndRead(new NettyMessage.CloseRequest()); // TODO(venkat): need clean way to write close request
			}catch (Exception e){
				LOG.error("Failed sending close request ",e);
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

	private String getEndpointStr(RdmaShuffleClientEndpoint clientEndpoint) throws  Exception{
		return  "src: " + clientEndpoint.getSrcAddr() + " dst: " +
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

	@Override
	public void run() {
		boolean moreAvailable = false;
		do {
			LOG.info("sending partition completed. input channel is closed? {}",inputChannel.isReleased());
			final NettyMessage.PartitionRequest request = new NettyMessage.PartitionRequest(
				partitionId, subpartitionIndex, inputChannel.getInputChannelId(), inputChannel.getInitialCredit());
			NettyMessage bufferResponseorEvent = clientEndpoint.writeAndRead(request);
			LOG.info("partition request completed");
			if (bufferResponseorEvent != null) {
				Class<?> msgClazz = bufferResponseorEvent.getClass();
				if (msgClazz == NettyMessage.BufferResponse.class) {
					LOG.info("got partition response");
					NettyMessage.BufferResponse bufferOrEvent = (NettyMessage.BufferResponse) bufferResponseorEvent;
					moreAvailable = bufferOrEvent.moreAvailable;
					LOG.info("more available {}", moreAvailable);
					try {
						// TODO (venkat): decode the message
						clientHandler.decodeMsg(bufferOrEvent, false, clientEndpoint, inputChannel);
					} catch (Throwable t) {
						LOG.error("decode failure ", t);
					}
				} else {
					LOG.info("received message type is not handled " + msgClazz.toString());
					moreAvailable = false;
				}
			} else {
				LOG.error("received partition response is null and it should never be the case");
				moreAvailable = false;
			}
//			if (!moreAvailable){
//				LOG.info("Done with this client and sending close request");
//				this.close(inputChannel); // TODO (venkat): do not close it here, endpoint TaskEvents to send.
//				// see SingleInputGate.java:496
//			}
		} while (!inputChannel.isReleased());
	}
}

