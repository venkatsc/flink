package org.apache.flink.runtime.io.network.rdma;

import com.esotericsoftware.minlog.Log;
import com.ibm.disni.RdmaServerEndpoint;
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.StatefulVerbCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;

import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.netty.NettyBufferPool;
import org.apache.flink.runtime.io.network.partition.ProducerFailedException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;

public class RdmaServerRequestHandler implements Runnable {
	private boolean stopped = false;
	private RdmaServerEndpoint<RdmaShuffleServerEndpoint> serverEndpoint;
	private static final Logger LOG = LoggerFactory.getLogger(RdmaServerRequestHandler.class);

	private final NettyBufferPool bufferPool;
	private NettyMessage.NettyMessageDecoder decoder = new NettyMessage.NettyMessageDecoder(false);
	private final ResultPartitionProvider partitionProvider;
	private final TaskEventDispatcher taskEventDispatcher;
	private final Map<Long, IbvMr> registerdMRs;


	public RdmaServerRequestHandler(RdmaServerEndpoint<RdmaShuffleServerEndpoint> serverEndpoint,
									ResultPartitionProvider partitionProvider, TaskEventDispatcher
										taskEventDispatcher, NettyBufferPool bufferPool, Map<Long, IbvMr>
										registerdMRs) {
		this.serverEndpoint = serverEndpoint;
		this.partitionProvider = partitionProvider;
		this.taskEventDispatcher = taskEventDispatcher;
		this.bufferPool = bufferPool;
		this.registerdMRs = registerdMRs;
	}

	@Override
	public void run() {
		while (!stopped) {
			try {
				RdmaShuffleServerEndpoint clientEndpoint = serverEndpoint.accept();
				clientEndpoint.setRegisteredMRs(registerdMRs);
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

//	private NettyMessage readPartition(NetworkSequenceViewReader reader) throws
//		IOException,
//		InterruptedException {
//		InputChannel.BufferAndAvailability next = null;
//		next = reader.getNextBuffer();
//		if (next == null) {
//			Throwable cause = reader.getFailureCause();
//			if (cause != null) {
//				NettyMessage.ErrorResponse msg = new NettyMessage.ErrorResponse(
//					new ProducerFailedException(cause),
//					reader.getReceiverId());
//				return msg;
//			} else {
//				// False available is set
//				return null;
//			}
//		} else {
//			// This channel was now removed from the available reader queue.
//			// We re-add it into the queue if it is still available
//			if (next.moreAvailable()) {
//				reader.setRegisteredAsAvailable(true);
//			} else {
//				reader.setRegisteredAsAvailable(false);
//			}
//			NettyMessage.BufferResponse msg = new NettyMessage.BufferResponse(
//				next.buffer(),
//				reader.getSequenceNumber(),
//				reader.getReceiverId(),
//				next.buffersInBacklog());
//			return msg;
//		}
//	}

	private class HandleClientConnection implements Runnable {
		RdmaShuffleServerEndpoint clientEndpoint;
		Map<Long, StatefulVerbCall<? extends StatefulVerbCall<?>>> inFlightVerbs = new ConcurrentHashMap<>();
		private final PartitionRequestQueue requestQueueOnCurrentConnection;


		HandleClientConnection(RdmaShuffleServerEndpoint clientEndpoint) throws IOException {
			this.clientEndpoint = clientEndpoint;
			RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, 0, inFlightVerbs);
			this.requestQueueOnCurrentConnection = new PartitionRequestQueue();
		}

		private final ConcurrentHashMap<Long, NettyMessage.BufferResponse> inFlight = new ConcurrentHashMap<>();
		long lastEventID = -1;

		@Override
		public void run() {
			try {
//				LOG.info("Server accepted connection src " + clientEndpoint.getSrcAddr() + " dst: " + clientEndpoint
//					.getDstAddr());
				new Thread(new RDMAWriter(requestQueueOnCurrentConnection, clientEndpoint, inFlight,inFlightVerbs)).start();
				boolean clientClose = false;
				NetworkSequenceViewReader reader = null;
				while (!clientClose) {
					// we need to do writing in seperate thread, otherwise buffers may not be release on
					// completion events
					IbvWC wc = clientEndpoint.getWcEvents().take();
//					if (lastEventID==-1){
//						lastEventID = wc.getWr_id();
//					}else if (lastEventID+1 != wc.getWr_id()){
//						LOG.info("Server: Did not get expected wr_id {} on endpoint {}", wc.getWr_id(), clientEndpoint
// .getEndpointStr());
//					}
//					synchronized (inFlightVerbs){
//						inFlightVerbs.remove(wc.getWr_id()).free();
//					}
					if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_RECV) {
						if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
							LOG.error("Receive posting failed. reposting new receive request");
//							RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId);
						} else { // first receive succeeded. Read the data and repost the next message
//							LOG.info("Received message from "+getEndpointStr(clientEndpoint));
							NettyMessage clientRequest = decodeMessageFromBuffer(clientEndpoint.getReceiveBuffer());
							Class<?> msgClazz = clientRequest.getClass();
							if (msgClazz == NettyMessage.CloseRequest.class) {
								clientClose = true;
								LOG.info("closing the endpoint " + getEndpointStr(clientEndpoint));
							} else if (msgClazz == NettyMessage.PartitionRequest.class) {
								// prepare response and post it
								NettyMessage.PartitionRequest partitionRequest = (NettyMessage.PartitionRequest)
									clientRequest;
								reader = new CreditBasedSequenceNumberingViewReader(partitionRequest
									.receiverId, partitionRequest.credit,requestQueueOnCurrentConnection);
								LOG.info("received partition request: " + partitionRequest.receiverId + " with " +
									"initial credit: " + partitionRequest.credit + "at endpoint: " +
									getEndpointStr(clientEndpoint));
								reader.requestSubpartitionView(
									partitionProvider,
									partitionRequest.partitionId,
									partitionRequest.queueIndex);
								// we need to post receive for next message. for example credit
								// TODO: We should do executor service here
								requestQueueOnCurrentConnection.notifyReaderCreated(reader);
								// If there is data on the connection,
								requestQueueOnCurrentConnection.tryEnqueueReader();

								RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, clientEndpoint.workRequestId
									.incrementAndGet(), inFlightVerbs);
								// TODO(venkat): do something better here, we should not poll reader
							} else if (msgClazz == NettyMessage.TaskEventRequest.class) {
								NettyMessage.TaskEventRequest request = (NettyMessage.TaskEventRequest) clientRequest;
								LOG.error("Unhandled request type TaskEventRequest");
								RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, clientEndpoint.workRequestId
									.incrementAndGet(), inFlightVerbs); // post next
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
								RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, clientEndpoint.workRequestId
									.incrementAndGet(), inFlightVerbs); // post next
								// receive
								// TODO (venkat): Handle it
//							outboundQueue.cancel(request.receiverId);
							} else if (msgClazz == NettyMessage.AddCredit.class) {
								NettyMessage.AddCredit request = (NettyMessage.AddCredit) clientRequest;
//								NetworkSequenceViewReader reader = readers.get(request.receiverId);
								LOG.info("Add credit: credit {} on the {}",request.credit,clientEndpoint.getEndpointStr());
								requestQueueOnCurrentConnection.addCredit(request.receiverId,request.credit);
								RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, clientEndpoint.workRequestId
									.incrementAndGet(), inFlightVerbs); // post next
								// receive
								// TODO (venkat): Handle it
								// outboundQueue.addCredit(request.receiverId, request.credit);
							} else {
								RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, clientEndpoint.workRequestId
									.incrementAndGet(), inFlightVerbs); // post next
								// receive
								LOG.warn("Received unexpected client request: {}", clientRequest);
							}
						}
						clientEndpoint.getReceiveBuffer().clear();
					} else if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_SEND) {
						if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
							LOG.error("Server:Send failed. reposting new send request request" + getEndpointStr
								(clientEndpoint));
//							RdmaSendReceiveUtil.postSendReq(clientEndpoint, ++workRequestId);
						}
						NettyMessage.BufferResponse response;
						synchronized (inFlight) {
							long wc_id = wc.getWr_id();
//							LOG.info("send work request completed {}", wc_id);
							response = inFlight.remove(wc_id);
							if (response != null) {
//								LOG.info("releasing buffer on send completion for WR {} address {}", wc_id, response
//									.getBuffer().memoryAddress());
								response.releaseBuffer();
							}
						}

						// send completed, so clear the buffer
//						clientEndpoint.getSendBuffer().clear();
						// Send succeed does not require any action
					} else {
						LOG.error("failed to match any condition " + wc.getOpcode());
					}
					// TODO: create requested handler
				}
				LOG.info("Server for client endpoint closed. src: " + clientEndpoint.getSrcAddr() + " dst: " +
					clientEndpoint.getDstAddr());
				requestQueueOnCurrentConnection.releaseAllResources();
				clientEndpoint.close();
			} catch (Exception e) {
				LOG.error("error handling client request ", e);
			}
		}
	}

	private class RDMAWriter implements Runnable {
		private PartitionRequestQueue requestQueueOnCurrentConnection;

		private RdmaShuffleServerEndpoint clientEndpoint;
		private ConcurrentHashMap<Long, NettyMessage.BufferResponse> inFlight;
		private Map<Long, StatefulVerbCall<? extends StatefulVerbCall<?>>> inFlightVerbs;


		public RDMAWriter( PartitionRequestQueue requestQueueOnCurrentConnection, RdmaShuffleServerEndpoint clientEndpoint,
						  ConcurrentHashMap<Long, NettyMessage.BufferResponse> inFlight, Map<Long, StatefulVerbCall<?
			extends StatefulVerbCall<?>>> inFlightVerbs) {
			this.requestQueueOnCurrentConnection = requestQueueOnCurrentConnection;
			this.clientEndpoint = clientEndpoint;
			this.inFlight = inFlight;
			this.inFlightVerbs = inFlightVerbs;
		}

		@Override
		public void run() {
			while (!clientEndpoint.isClosed()) {
			try {
				if (!requestQueueOnCurrentConnection.getAvailableReaders().isEmpty()) {
						NettyMessage response = requestQueueOnCurrentConnection.getResponseMessage();

						if (response==null){
							continue;
						}

						if (response instanceof NettyMessage.BufferResponse) {
							NettyMessage.BufferResponse tmpResp = (NettyMessage.BufferResponse)
								response;
						} else {
							LOG.info("skip: sending error message/close request");
						}
//								clientEndpoint.getSendBuffer().put(response.write(bufferPool).nioBuffer());
//								clientEndpoint.getReceiveBuffer().clear();
//								RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId); // post next
						// receive

						// hold references of the response until the send completes
						if (response instanceof NettyMessage.BufferResponse) {
							response.write(bufferPool); // creates the header info
							synchronized (inFlight) {
								long workRequestId = clientEndpoint.workRequestId.incrementAndGet();
//								LOG.info("Add buffer to inFlight: wr {} memory address: {}", workRequestId, (
//									(NettyMessage.BufferResponse) response).getBuffer().memoryAddress());
								inFlight.put(workRequestId, (NettyMessage.BufferResponse) response);
								RdmaSendReceiveUtil.postSendReqForBufferResponse(clientEndpoint, workRequestId,
									(NettyMessage.BufferResponse) response,inFlightVerbs);
							}
						} else {
							clientEndpoint.getSendBuffer().put(response.write(bufferPool).nioBuffer());
							RdmaSendReceiveUtil.postSendReq(clientEndpoint, clientEndpoint.workRequestId
								.incrementAndGet(),inFlightVerbs);
						}
				}
				else{
					// place holder for debug
					int t=0;
					//requestQueueOnCurrentConnection.tryEnqueueReader();
				}
			} catch (Exception e) {
				LOG.error("failed to writing out the data (if no credit, then we still continue the loop)", e);
			}
			}
		}
	}

	private String getEndpointStr(RdmaShuffleServerEndpoint clientEndpoint) throws Exception {
		return "src: " + clientEndpoint.getSrcAddr() + " dst: " +
			clientEndpoint.getDstAddr();
	}

	public void stop() {
		stopped = true;
	}
}
