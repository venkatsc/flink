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

		LOG.debug("Requesting subpartition {} of partition {} with {} ms delay.",
			subpartitionIndex, partitionId, delayMs);

//		clientHandler.addInputChannel(inputChannel);

		boolean partitionReadFinished = false;
		do {
			final NettyMessage.PartitionRequest request = new NettyMessage.PartitionRequest(
				partitionId, subpartitionIndex, inputChannel.getInputChannelId(), inputChannel.getInitialCredit());
			NettyMessage bufferResponseorEvent = clientEndpoint.writeAndRead(request);
			LOG.info("sending partition request");
			Class<?> msgClazz = bufferResponseorEvent.getClass();
			if (msgClazz == NettyMessage.BufferResponse.class) {
				NettyMessage.BufferResponse bufferOrEvent = (NettyMessage.BufferResponse) bufferResponseorEvent;
				partitionReadFinished = bufferOrEvent.moreAvailable;
			}
			try {
				clientHandler.decodeMsg(bufferResponseorEvent, false, clientEndpoint);
			} catch (Throwable t) {
				LOG.error("decode failure ", t);
			}
		}while (!partitionReadFinished);

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
			clientHandler.decodeMsg(bufferResponseorEvent, false, clientEndpoint);
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
			clientEndpoint.writeAndRead(new NettyMessage.CloseRequest());

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
}

