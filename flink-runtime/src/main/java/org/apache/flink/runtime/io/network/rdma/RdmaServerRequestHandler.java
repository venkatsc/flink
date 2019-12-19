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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;

import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.netty.NettyBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

public class RdmaServerRequestHandler implements Runnable {
	private volatile boolean stopped = false;
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
				LOG.info("Accepted connection {}", clientEndpoint.getEndpointStr());
				clientEndpoint.setRegisteredMRs(registerdMRs);
				// TODO (venkat): Handle accepted connection, not using thread pool as it is only proto-type with 4
				// servers
				// Should be changed to configurable ThreadPool to work with more nodes.
				PartitionRequestQueue queue = new PartitionRequestQueue();
				ConcurrentHashMap<Long, NettyMessage.BufferResponse> inFlightRequests = new ConcurrentHashMap<>();
				// event queue handler
				HandleClientConnection connectionHandler = new HandleClientConnection(clientEndpoint, queue,
					inFlightRequests);

				clientEndpoint.setConectionHandler(connectionHandler);

//				Thread wcEventLoop = new Thread(connectionHandler, "work-completion-loop");
//				wcEventLoop.start();
				// data writeout handler
				RDMAWriter writer = new RDMAWriter(queue, clientEndpoint, inFlightRequests);
				Thread writerThread = new Thread(writer, "rdma-writer");
				writerThread.start();
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

	public class HandleClientConnection  {
		RdmaShuffleServerEndpoint clientEndpoint;
		private final PartitionRequestQueue requestQueueOnCurrentConnection;

		private final ConcurrentHashMap<Long, NettyMessage.BufferResponse> inFlightRequests;

		HandleClientConnection(RdmaShuffleServerEndpoint clientEndpoint,
							   PartitionRequestQueue requestQueueOnCurrentConnection, ConcurrentHashMap<Long,
			NettyMessage.BufferResponse> inFlightRequests) throws IOException {
			this.clientEndpoint = clientEndpoint;
//			RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, 0);
			this.requestQueueOnCurrentConnection = requestQueueOnCurrentConnection;
			this.inFlightRequests = inFlightRequests;
//			this.executorService = executorService;
		}

		long lastEventID = -1;

		public void handleWC(IbvWC wc) {
			try {
				boolean clientClose = false;
				NetworkSequenceViewReader reader = null;
//				while (!clientClose) {
					// we need to do writing in seperate thread, otherwise buffers may not be release on
					// completion events
//					IbvWC wc = clientEndpoint.getWcEvents().take();
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
							long wc_id=wc.getWr_id();
							ByteBuffer recvedMsg= clientEndpoint.inFlightRecvs.get(wc_id);
							NettyMessage clientRequest = decodeMessageFromBuffer(recvedMsg);
							Class<?> msgClazz = clientRequest.getClass();
							if (msgClazz == NettyMessage.CloseRequest.class) {
								clientClose = true;
								LOG.info("closing the endpoint " + getEndpointStr(clientEndpoint));
							} else if (msgClazz == NettyMessage.PartitionRequest.class) {
								// prepare response and post it
								NettyMessage.PartitionRequest partitionRequest = (NettyMessage.PartitionRequest)
									clientRequest;
								reader = new CreditBasedSequenceNumberingViewReader(partitionRequest
									.receiverId, partitionRequest.credit, requestQueueOnCurrentConnection);
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
									.incrementAndGet(),recvedMsg); // repost this receive with different work req id
								// TODO(venkat): do something better here, we should not poll reader
							} else if (msgClazz == NettyMessage.TaskEventRequest.class) {
								NettyMessage.TaskEventRequest request = (NettyMessage.TaskEventRequest) clientRequest;
								LOG.error("Unhandled request type TaskEventRequest");
								RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, clientEndpoint.workRequestId
									.incrementAndGet(),recvedMsg); // post next
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
									.incrementAndGet(),recvedMsg); // post next
								// receive
								// TODO (venkat): Handle it
//							outboundQueue.cancel(request.receiverId);
							} else if (msgClazz == NettyMessage.AddCredit.class) {
								NettyMessage.AddCredit request = (NettyMessage.AddCredit) clientRequest;
//								NetworkSequenceViewReader reader = readers.get(request.receiverId);
//								LOG.info("Add credit: credit {} on the {}",request.credit,clientEndpoint
// .getEndpointStr());
//								requestQueueOnCurrentConnection.canReceiveCredit(false);
								requestQueueOnCurrentConnection.addCredit(request.receiverId, request.credit);
								RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, clientEndpoint.workRequestId
									.incrementAndGet(),recvedMsg); // post next
//								requestQueueOnCurrentConnection.canReceiveCredit(true);
								// receive
								// TODO (venkat): Handle it
								// outboundQueue.addCredit(request.receiverId, request.credit);
							} else {
								RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, clientEndpoint.workRequestId
									.incrementAndGet(),recvedMsg); // post next
								// receive
								LOG.warn("Received unexpected client request: {}", clientRequest);
							}
						}
//						clientEndpoint.getReceiveBuffer().clear();
					} else if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_SEND) {
						if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
							LOG.error("Server:Send failed for WR id {} with code {}. reposting new send request " +
								"request {}", wc.getWr_id(), IbvWC.IbvWcStatus.valueOf(wc.getStatus()), getEndpointStr
								(clientEndpoint));
//							RdmaSendReceiveUtil.postSendReq(clientEndpoint, ++workRequestId);
						}
						NettyMessage.BufferResponse response;
						long wc_id = wc.getWr_id();
//							LOG.info("send work request completed {}", wc_id);
						response = inFlightRequests.remove(wc_id);
						if (response != null) {
//								LOG.info("releasing buffer on send completion for WR {} address {}", wc_id, response
//									.getBuffer().memoryAddress());
							response.releaseBuffer();
						}

						// send completed, so clear the buffer
//						clientEndpoint.getSendBuffer().clear();
						// Send succeed does not require any action
					} else {
						LOG.error("failed to match any condition " + wc.getOpcode());
					}
					// TODO: create requested handler
//				}
//				LOG.info("Server for client endpoint closed. src: " + clientEndpoint.getSrcAddr() + " dst: " +
//					clientEndpoint.getDstAddr());
//				requestQueueOnCurrentConnection.releaseAllResources();
//				clientEndpoint.close();
			} catch (Exception e) {
				LOG.error("error handling client request ", e);
			}
		}
	}

	private class RDMAWriter implements Runnable {
		private PartitionRequestQueue requestQueueOnCurrentConnection;

		private RdmaShuffleServerEndpoint clientEndpoint;
		private ConcurrentHashMap<Long, NettyMessage.BufferResponse> inFlight;


		public RDMAWriter(PartitionRequestQueue requestQueueOnCurrentConnection, RdmaShuffleServerEndpoint
			clientEndpoint,
						  ConcurrentHashMap<Long, NettyMessage.BufferResponse> inFlight) {
			this.requestQueueOnCurrentConnection = requestQueueOnCurrentConnection;
			this.clientEndpoint = clientEndpoint;
			this.inFlight = inFlight;
		}

		@Override
		public void run() {
			while (!clientEndpoint.isClosed()) {
				try {
					NetworkSequenceViewReader reader = requestQueueOnCurrentConnection.getAvailableReaders().take();
					NettyMessage response = requestQueueOnCurrentConnection.getResponseMessage(reader);

					if (response == null) {
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
						long workRequestId = clientEndpoint.workRequestId.incrementAndGet();
//								LOG.info("Add buffer to inFlight: wr {} memory address: {}", workRequestId, (
//									(NettyMessage.BufferResponse) response).getBuffer().memoryAddress());
						inFlight.put(workRequestId, (NettyMessage.BufferResponse) response);
						RdmaSendReceiveUtil.postSendReqForBufferResponse(clientEndpoint, workRequestId,
							(NettyMessage.BufferResponse) response);
					} else {
						clientEndpoint.getSendBuffer().put(response.write(bufferPool).nioBuffer());
						RdmaSendReceiveUtil.postSendReq(clientEndpoint, clientEndpoint.workRequestId
							.incrementAndGet());
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
