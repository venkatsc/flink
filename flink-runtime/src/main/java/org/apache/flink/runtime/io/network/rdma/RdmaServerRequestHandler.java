package org.apache.flink.runtime.io.network.rdma;

import com.esotericsoftware.minlog.Log;
import com.ibm.disni.RdmaServerEndpoint;
import com.ibm.disni.verbs.IbvWC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

public class RdmaServerRequestHandler implements Runnable {
	private boolean stopped = false;
	private RdmaServerEndpoint<RdmaShuffleServerEndpoint> serverEndpoint;
	private int workRequestId = 0;
	private static final Logger LOG = LoggerFactory.getLogger(RdmaServerRequestHandler.class);

	private final NettyBufferPool bufferPool;
	private NettyMessage.NettyMessageDecoder decoder = new NettyMessage.NettyMessageDecoder(false);
	private final ResultPartitionProvider partitionProvider;
	private final TaskEventDispatcher taskEventDispatcher;
	private final Map<InputChannelID, NetworkSequenceViewReader> readers = new HashMap<>();

	public RdmaServerRequestHandler(RdmaServerEndpoint<RdmaShuffleServerEndpoint> serverEndpoint,
									ResultPartitionProvider partitionProvider, TaskEventDispatcher
										taskEventDispatcher, NettyBufferPool bufferPool) {
		this.serverEndpoint = serverEndpoint;
		this.partitionProvider = partitionProvider;
		this.taskEventDispatcher = taskEventDispatcher;
		this.bufferPool = bufferPool;
	}

	@Override
	public void run() {
		while (!stopped) {
			try {
				RdmaShuffleServerEndpoint clientEndpoint = serverEndpoint.accept();
				RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId);
				// TODO (venkat): Handle accepted connection, not using thread pool as it is only proto-type with 4
				// servers
				// Should be changed to configurable ThreadPool to work with more nodes.
				HandleClientConnection connectionHandler = new HandleClientConnection(clientEndpoint);
				Thread t = new Thread(connectionHandler);
				t.start();
			} catch (Exception ie) {
				Log.error("Failed to handle request", ie);
			}
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

	private NettyMessage readPartition(NettyMessage.PartitionRequest request, NetworkSequenceViewReader reader) throws
		IOException,
		InterruptedException {
		InputChannel.BufferAndAvailability next = null;
		while (true) {
			next = reader.getNextBuffer();
			if (next == null) {
				if (!reader.isReleased()) {
					continue;
				}
				Throwable cause = reader.getFailureCause();
				LOG.info("Sending error message ",cause);
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
					LOG.info("server more available: true");
					reader.setRegisteredAsAvailable(true);
				} else {
					LOG.info("server more available: false");
					reader.setRegisteredAsAvailable(false);
				}
				LOG.info("Sending BufferResponse message");
				NettyMessage.BufferResponse msg = new NettyMessage.BufferResponse(
					next.buffer(),
					reader.getSequenceNumber(),
					reader.getReceiverId(),
					next.buffersInBacklog(), next.moreAvailable());
				return msg;
			}
		}
	}

	private class HandleClientConnection implements Runnable {
		RdmaShuffleServerEndpoint clientEndpoint;

		HandleClientConnection(RdmaShuffleServerEndpoint clientEndpoint) {
			this.clientEndpoint = clientEndpoint;
		}

		@Override
		public void run() {
			try {
				LOG.info("Server accepted connection src " + clientEndpoint.getSrcAddr() + " dst: " + clientEndpoint
					.getDstAddr());
				boolean clientClose = false;
				while (!clientClose) {
					IbvWC wc = null;
					wc = clientEndpoint.getWcEvents().take();
					if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_RECV) {
						if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
							LOG.error("Receive posting failed. reposting new receive request");
							RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId);
						} else { // first receive succeeded. Read the data and repost the next message
							LOG.info("Received message from "+getEndpointStr(clientEndpoint));
							NettyMessage clientRequest = decodeMessageFromBuffer(clientEndpoint.getReceiveBuffer());
							Class<?> msgClazz = clientRequest.getClass();
							if (msgClazz == NettyMessage.CloseRequest.class) {
								clientClose = true;

								LOG.info("closing the endpoint "+getEndpointStr(clientEndpoint));
//								for (InputChannelID channelID: readers.keySet()){
//									NetworkSequenceViewReader reader = readers.get(channelID);
//									reader.notifySubpartitionConsumed();
//									reader.releaseAllResources();
//								}
							} else if (msgClazz == NettyMessage.PartitionRequest.class) {
								// prepare response and post it
								NettyMessage.PartitionRequest partitionRequest = (NettyMessage.PartitionRequest)
									clientRequest;
								LOG.info("received partition request: " + partitionRequest.receiverId + "at endpoint: "+getEndpointStr(clientEndpoint));
								NetworkSequenceViewReader reader = readers.get(partitionRequest.receiverId);
								if (reader == null) {
									reader = new SequenceNumberingViewReader(partitionRequest.receiverId);
									reader.requestSubpartitionView(
										partitionProvider,
										partitionRequest.partitionId,
										partitionRequest.queueIndex);
									readers.put(partitionRequest.receiverId, reader);
								}
								// TODO(venkat): do something better here, we should not poll reader
								while (!reader.isRegisteredAsAvailable()) {
									LOG.info("waiting for partition"+ partitionRequest.partitionId +" at the endpoint for data availability: "+getEndpointStr(clientEndpoint) + "reader status:" + reader);
									synchronized (reader){
										reader.wait();
									}
								}

								NettyMessage response = readPartition(partitionRequest, reader);
							if (response instanceof NettyMessage.BufferResponse) {
								NettyMessage.BufferResponse tmpResp = (NettyMessage.BufferResponse)
									response;
								LOG.error(" Sending partition with seq. number: " + tmpResp.sequenceNumber + " receiver Id " + tmpResp.receiverId);

							}else{
								LOG.info("skip: sending error message");
							}
								clientEndpoint.getSendBuffer().put(response.write(bufferPool).nioBuffer());
								clientEndpoint.getReceiveBuffer().clear();
								RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId); // post next
								// receive
								RdmaSendReceiveUtil.postSendReq(clientEndpoint, ++workRequestId);
//								if (!reader.isRegisteredAsAvailable()) {
//									LOG.error("Notifying the sub-partition consume");
//									reader.notifySubpartitionConsumed();
//									reader.releaseAllResources();
//								} else {
//									LOG.error("Still not finished subpartition");
//								}
							} else if (msgClazz == RdmaMessage.TaskEventRequest.class) {
								NettyMessage.TaskEventRequest request = (NettyMessage.TaskEventRequest) clientRequest;
								LOG.error("Unhandled request type TaskEventRequest");
								RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId); // post next
								// receive
								// TODO (venkat): Handle it
								if (!taskEventDispatcher.publish(request.partitionId, request.event)) {
//								respondWithError(ctx, new IllegalArgumentException("Task event receiver not found."),
// request.receiverId);
								}
							} else if (msgClazz == NettyMessage.CancelPartitionRequest.class) {
								NettyMessage.CancelPartitionRequest request = (NettyMessage.CancelPartitionRequest)
									clientRequest;
								LOG.error("Unhandled request type CancelPartitionRequest");
								RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId); // post next
								// receive
								// TODO (venkat): Handle it
//							outboundQueue.cancel(request.receiverId);
							} else if (msgClazz == NettyMessage.AddCredit.class) {
								NettyMessage.AddCredit request = (NettyMessage.AddCredit) clientRequest;
								RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId); // post next
								// receive
								// TODO (venkat): Handle it
								LOG.error("Unhandled request type AddCredit");
								// outboundQueue.addCredit(request.receiverId, request.credit);
							} else {
								RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId); // post next
								// receive
								LOG.warn("Received unexpected client request: {}", clientRequest);
							}
						}
					} else if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_SEND) {
						if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
							LOG.error("Server:Send failed. reposting new send request request"+getEndpointStr(clientEndpoint));
//							RdmaSendReceiveUtil.postSendReq(clientEndpoint, ++workRequestId);
						}
						// send completed, so clear the buffer
						clientEndpoint.getSendBuffer().clear();
						// Send succeed does not require any action
					} else {
						System.out.println("failed to match any condition " + wc.getOpcode());
					}
					// TODO: create requested handler
				}
				LOG.info("Server for client endpoint closed. src: " + clientEndpoint.getSrcAddr() + " dst: " +
					clientEndpoint.getDstAddr());
				clientEndpoint.close();
			} catch (Exception e) {
				LOG.error("error handling client request ", e);
			}
		}
	}

	private String getEndpointStr(RdmaShuffleServerEndpoint clientEndpoint) throws  Exception{
		return  "src: " + clientEndpoint.getSrcAddr() + " dst: " +
		clientEndpoint.getDstAddr();
	}
	public void stop() {
		stopped = true;
	}
}
