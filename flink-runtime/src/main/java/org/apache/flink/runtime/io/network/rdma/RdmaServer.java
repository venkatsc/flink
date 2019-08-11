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

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.RdmaServerEndpoint;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;
import org.apache.commons.cli.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;

import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.netty.NettyBufferPool;
import org.apache.flink.runtime.io.network.partition.ProducerFailedException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

public class RdmaServer implements RdmaEndpointFactory<RdmaShuffleServerEndpoint> {
	private static final Logger LOG = LoggerFactory.getLogger(RdmaServer.class);
	private RdmaActiveEndpointGroup<RdmaShuffleServerEndpoint> endpointGroup;
	private final RdmaConfig rdmaConfig;
	private int workRequestId = 1;
	private RdmaServerEndpoint<RdmaShuffleServerEndpoint> serverEndpoint;
	private InetSocketAddress address;
	private boolean stopped = false;

	public RdmaShuffleServerEndpoint getClientEndpoint() {
		return clientEndpoint;
	}

	private RdmaShuffleServerEndpoint clientEndpoint;
	private NettyBufferPool bufferPool;
	private NettyMessage.NettyMessageDecoder decoder = new NettyMessage.NettyMessageDecoder(false);
	private ResultPartitionProvider partitionProvider;
	private TaskEventDispatcher taskEventDispatcher;
	private Map<InputChannelID, NetworkSequenceViewReader> readers = new HashMap<>();

	/**
	 * Creates the Queue pair endpoint and waits for the incoming connections
	 *
	 * @param idPriv
	 * @param serverSide
	 * @return
	 * @throws IOException
	 */
	public RdmaShuffleServerEndpoint createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
		return new RdmaShuffleServerEndpoint(endpointGroup, idPriv, serverSide, 100);
	}

	public RdmaServer(RdmaConfig rdmaConfig, NettyBufferPool bufferPool) {
		this.rdmaConfig = rdmaConfig;
		this.bufferPool = bufferPool;
	}

	public void setPartitionProvider(ResultPartitionProvider partitionProvider) {
		this.partitionProvider = partitionProvider;
	}

	public void setTaskEventDispatcher(TaskEventDispatcher taskEventDispatcher) {
		this.taskEventDispatcher = taskEventDispatcher;
	}

	public void start() throws Exception {
		// create a EndpointGroup. The RdmaActiveEndpointGroup contains CQ processing and delivers CQ event to the
		// endpoint.dispatchCqEvent() method.
		endpointGroup = new RdmaActiveEndpointGroup<RdmaShuffleServerEndpoint>(1000, true, 128, 4, 128);
		endpointGroup.init(this);
		// create a server endpoint
		serverEndpoint = endpointGroup.createServerEndpoint();

		// we can call bind on a server endpoint, just like we do with sockets
		// InetAddress ipAddress = InetAddress.getByName(host);
		address = new InetSocketAddress(rdmaConfig.getServerAddress(), rdmaConfig.getServerPort());
		try {
			serverEndpoint.bind(address, 10);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("SimpleServer::servers bound to address " + address.toString());
		// we can accept client connections untill this server is stopped
		while (!stopped) {
			clientEndpoint = serverEndpoint.accept();
			boolean clientClose = false;
			while (!clientClose) {
				IbvWC wc = clientEndpoint.getWcEvents().take();
				if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_RECV) {
					if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
						System.out.println("Receive posting failed. reposting new receive request");
						RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId);
					} else { // first receive succeeded. Read the data and repost the next message
						NettyMessage clientRequest = decodeMessageFromBuffer(clientEndpoint.getReceiveBuffer());
						Class<?> msgClazz = clientRequest.getClass();
						if (msgClazz == NettyMessage.CloseRequest.class) {
							clientClose = true;
						} else if (msgClazz == NettyMessage.PartitionRequest.class) {
							// prepare response and post it
							NettyMessage.PartitionRequest partitionRequest = (NettyMessage.PartitionRequest)
								clientRequest;
							NettyMessage response = readPartition(partitionRequest);
							clientEndpoint.getSendBuffer().put(response.write(bufferPool).nioBuffer());
							clientEndpoint.getReceiveBuffer().clear();
							RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId); // post next receive
							RdmaSendReceiveUtil.postSendReq(clientEndpoint, ++workRequestId);
						} else if (msgClazz == RdmaMessage.TaskEventRequest.class) {
							NettyMessage.TaskEventRequest request = (NettyMessage.TaskEventRequest) clientRequest;
							LOG.error("Unhandled request type TaskEventRequest");
							RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId); // post next receive
							// TODO (venkat): Handle it
							if (!taskEventDispatcher.publish(request.partitionId, request.event)) {
//								respondWithError(ctx, new IllegalArgumentException("Task event receiver not found."), request.receiverId);
							}
						} else if (msgClazz == NettyMessage.CancelPartitionRequest.class) {
							NettyMessage.CancelPartitionRequest request = (NettyMessage.CancelPartitionRequest)
								clientRequest;
							LOG.error("Unhandled request type CancelPartitionRequest");
							RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId); // post next receive
							// TODO (venkat): Handle it
//							outboundQueue.cancel(request.receiverId);
						} else if (msgClazz == NettyMessage.AddCredit.class) {
							NettyMessage.AddCredit request = (NettyMessage.AddCredit) clientRequest;
							RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId); // post next receive
							// TODO (venkat): Handle it
							LOG.error("Unhandled request type AddCredit");
							// outboundQueue.addCredit(request.receiverId, request.credit);
						} else {
							RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId); // post next receive
							LOG.warn("Received unexpected client request: {}", clientRequest);
						}
					}
				} else if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_SEND) {
					if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
						System.out.println("Send failed. reposting new send request request");
						RdmaSendReceiveUtil.postSendReq(clientEndpoint, ++workRequestId);
					}
					// send completed, so clear the buffer
					clientEndpoint.getSendBuffer().clear();
					// Send succeed does not require any action
				} else {
					System.out.println("failed to match any condition " + wc.getOpcode());
				}
				// TODO: create requested handler
			}
			clientEndpoint.close();
		}
	}

	private NettyMessage decodeMessageFromBuffer(ByteBuffer clientReq) {
		NettyMessage clientMessage = null;
		try {
			clientMessage = (NettyMessage) decoder.decode(null, Unpooled.wrappedBuffer(clientReq));
		} catch (Exception e) {
			LOG.error("Error message: ", e);
		}
		return clientMessage;
	}

	private NettyMessage readPartition(NettyMessage.PartitionRequest request) throws IOException,
		InterruptedException {
		NetworkSequenceViewReader reader = readers.get(request.receiverId);
		if (reader == null) {
			reader = new SequenceNumberingViewReader(request.receiverId);
			reader.requestSubpartitionView(
				partitionProvider,
				request.partitionId,
				request.queueIndex);
			readers.put(request.receiverId, reader);
		}
		InputChannel.BufferAndAvailability next = null;
		while (true) {
			next = reader.getNextBuffer();
			if (next == null) {
				if (!reader.isReleased()) {
					continue;
				}
//				markAsReleased(reader.getReceiverId());

				Throwable cause = reader.getFailureCause();
				if (cause != null) {
					NettyMessage.ErrorResponse msg = new NettyMessage.ErrorResponse(
						new ProducerFailedException(cause),
						reader.getReceiverId());
					return msg;
				}
			} else {
				// This channel was now removed from the available reader queue.
				// We re-add it into the queue if it is still available
				if (next.moreAvailable()) {
					reader.setRegisteredAsAvailable(true);
				} else {
					reader.setRegisteredAsAvailable(false);
				}

				NettyMessage.BufferResponse msg = new NettyMessage.BufferResponse(
					next.buffer(),
					reader.getSequenceNumber(),
					reader.getReceiverId(),
					next.buffersInBacklog(), next.moreAvailable());

				// Write and flush and wait until this is done before
				// trying to continue with the next buffer.
//				channel.writeAndFlush(msg).addListener(writeListener);

				return msg;
			}
		}
	}

	public void stop() {
		stopped = true;
		try {
			serverEndpoint.close();
		} catch (Exception e) {
			LOG.error("Failed to stop server ", e);
		}
	}
}
