package org.apache.flink.runtime.io.network.rdma;

import com.esotericsoftware.minlog.Log;
import com.ibm.disni.RdmaServerEndpoint;
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvWC;
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
										taskEventDispatcher, NettyBufferPool bufferPool, Map<Long, IbvMr> registerdMRs) {
		this.serverEndpoint = serverEndpoint;
		this.partitionProvider = partitionProvider;
		this.taskEventDispatcher = taskEventDispatcher;
		this.bufferPool = bufferPool;
		this.registerdMRs= registerdMRs;
	}

	@Override
	public void run() {
		while (!stopped) {
			try {
				RdmaShuffleServerEndpoint clientEndpoint = serverEndpoint.accept();
				RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, 0);
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

	private NettyMessage readPartition( NetworkSequenceViewReader reader) throws
		IOException,
		InterruptedException {
		InputChannel.BufferAndAvailability next = null;
			next = reader.getNextBuffer();
			if (next == null) {
				Throwable cause = reader.getFailureCause();
				if (cause != null) {
					NettyMessage.ErrorResponse msg = new NettyMessage.ErrorResponse(
						new ProducerFailedException(cause),
						reader.getReceiverId());
					return msg;
				}else{
					// False available is set
					return null;
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
				return msg;
			}
	}

	private class HandleClientConnection implements Runnable {
		RdmaShuffleServerEndpoint clientEndpoint;
		HandleClientConnection(RdmaShuffleServerEndpoint clientEndpoint) {
			this.clientEndpoint = clientEndpoint;
		}
		private final ConcurrentHashMap<Long,NettyMessage.BufferResponse> inFlight = new ConcurrentHashMap<>();
		@Override
		public void run() {
			try {
//				LOG.info("Server accepted connection src " + clientEndpoint.getSrcAddr() + " dst: " + clientEndpoint
//					.getDstAddr());
				boolean clientClose = false;
				NetworkSequenceViewReader reader = null;
				while (!clientClose) {
					// we need to do writing in seperate thread, otherwise buffers may not be release on
					// completion events
					IbvWC wc = clientEndpoint.getWcEvents().take();
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
								LOG.info("closing the endpoint "+getEndpointStr(clientEndpoint));
							} else if (msgClazz == NettyMessage.PartitionRequest.class) {
								// prepare response and post it
								NettyMessage.PartitionRequest partitionRequest = (NettyMessage.PartitionRequest)
									clientRequest;
								reader = new SequenceNumberingViewReader(partitionRequest
									.receiverId);
								LOG.info("received partition request: " + partitionRequest.receiverId + " with " +
										"initial credit: " + partitionRequest.credit + "at endpoint: " +
										getEndpointStr(clientEndpoint));
								reader.addCredit(partitionRequest.credit);
								reader.requestSubpartitionView(
									partitionProvider,
									partitionRequest.partitionId,
									partitionRequest.queueIndex);
								// we need to post receive for next message. for example credit
								// TODO: We should do executor service here
								new Thread(new RDMAWriter(reader,clientEndpoint,inFlight)).start();
								RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, clientEndpoint.workRequestId.incrementAndGet());
								// TODO(venkat): do something better here, we should not poll reader
							} else if (msgClazz == NettyMessage.TaskEventRequest.class) {
								NettyMessage.TaskEventRequest request = (NettyMessage.TaskEventRequest) clientRequest;
								LOG.error("Unhandled request type TaskEventRequest");
								RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, clientEndpoint.workRequestId.incrementAndGet()); // post next
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
								RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, clientEndpoint.workRequestId.incrementAndGet()); // post next
								// receive
								// TODO (venkat): Handle it
//							outboundQueue.cancel(request.receiverId);
							} else if (msgClazz == NettyMessage.AddCredit.class) {
								NettyMessage.AddCredit request = (NettyMessage.AddCredit) clientRequest;
//								NetworkSequenceViewReader reader = readers.get(request.receiverId);
								if (reader!=null) {
									LOG.info("Add credit: credit {} on the reader {}",request.credit,reader);
									reader.addCredit(request.credit);
								}
								RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, clientEndpoint.workRequestId.incrementAndGet()); // post next
								// receive
								// TODO (venkat): Handle it
								// outboundQueue.addCredit(request.receiverId, request.credit);
							} else {
								RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, clientEndpoint.workRequestId.incrementAndGet()); // post next
								// receive
								LOG.warn("Received unexpected client request: {}", clientRequest);
							}
						}
						clientEndpoint.getReceiveBuffer().clear();
					} else if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_SEND) {
						if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
							LOG.error("Server:Send failed. reposting new send request request"+getEndpointStr(clientEndpoint));
//							RdmaSendReceiveUtil.postSendReq(clientEndpoint, ++workRequestId);
						}
						NettyMessage.BufferResponse response;
						synchronized (inFlight) {
							long wc_id=wc.getWr_id();
							response = inFlight.remove(wc_id);
							if (response !=null) {
								response.releaseBuffer();
//								LOG.info("releasing buffer on send completion for WR {}",wc_id);
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
				clientEndpoint.close();
			} catch (Exception e) {
				LOG.error("error handling client request ", e);
			}
		}
	}

	private class RDMAWriter implements Runnable{
		private  NetworkSequenceViewReader reader;
		private RdmaShuffleServerEndpoint clientEndpoint;
		private ConcurrentHashMap<Long,NettyMessage.BufferResponse> inFlight;
		public RDMAWriter(NetworkSequenceViewReader reader,RdmaShuffleServerEndpoint clientEndpoint,ConcurrentHashMap<Long,NettyMessage.BufferResponse> inFlight){
			this.reader= reader;
			this.clientEndpoint = clientEndpoint;
			this.inFlight = inFlight;
		}

		@Override
		public void run() {
			while (!clientEndpoint.isClosed()) {
			try {
//					for (NetworkSequenceViewReader reader : readers.values()) {
////						if (reader.isReleased()){ Should remove reader to exit, currently we are only doing single reader
//						// so it should be fine for now.
////							readers.remove()
////						}
//						if (reader.isReleased()){
//							break;
//						}
						while (!reader.isReleased()) {
							if (reader.isRegisteredAsAvailable()) {
								if (((SequenceNumberingViewReader) reader).hasCredit()) {
									NettyMessage response = readPartition(reader);
									if (response == null) {
										// False reader available is set, exit the reader here and write to the
										// next reader with credit
										break;
//									response = new NettyMessage.CloseRequest();
									}
									((SequenceNumberingViewReader) reader).decrementCredit();
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
										long workRequestId= clientEndpoint.workRequestId.incrementAndGet();
										synchronized (inFlight) {
//											LOG.info("Add buffer to inFlight "+ workRequestId);
											inFlight.put(workRequestId, (NettyMessage.BufferResponse) response);
										}
										RdmaSendReceiveUtil.postSendReqForBufferResponse(clientEndpoint,workRequestId , (NettyMessage.BufferResponse) response);
									} else {
										clientEndpoint.getSendBuffer().put(response.write(bufferPool).nioBuffer());
										RdmaSendReceiveUtil.postSendReq(clientEndpoint, clientEndpoint.workRequestId.incrementAndGet());
									}
								}
							}else {
								synchronized (reader) {
									reader.wait();
								}
							}
						}
				}catch(Exception e){
					LOG.error("failed to writing out the data ",e);
				}
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
