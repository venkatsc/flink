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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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
	private long workRequestId = 0;
	private static final Logger LOG = LoggerFactory.getLogger(RdmaServerRequestHandler.class);

	private final NettyBufferPool bufferPool;
	private NettyMessage.NettyMessageDecoder decoder = new NettyMessage.NettyMessageDecoder(false);
	private final ResultPartitionProvider partitionProvider;
	private final TaskEventDispatcher taskEventDispatcher;
	private final Map<Long,NettyMessage.BufferResponse> inFlight = new HashMap<>();

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

	private NettyMessage readPartition( NetworkSequenceViewReader reader) throws
		IOException,
		InterruptedException {
		InputChannel.BufferAndAvailability next = null;
//		while (!reader.isReleased()) {
			next = reader.getNextBuffer();
			if (next == null) {
//				if (!reader.isReleased()) {
//					continue;
//				}
				Throwable cause = reader.getFailureCause();
				if (cause != null) {
//					LOG.info("Sending error message ",cause);
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
//					LOG.info("server more available: true on {}",reader);
					reader.setRegisteredAsAvailable(true);
				} else {
//					LOG.info("server more available: false on {}",reader);
					reader.setRegisteredAsAvailable(false);
				}
//				LOG.info("Sending BufferResponse message");
				NettyMessage.BufferResponse msg = new NettyMessage.BufferResponse(
					next.buffer(),
					reader.getSequenceNumber(),
					reader.getReceiverId(),
					next.buffersInBacklog(), next.moreAvailable());
				return msg;
			}
//		}
//		return null;
	}

	private class HandleClientConnection implements Runnable {
		RdmaShuffleServerEndpoint clientEndpoint;
		private final Map<InputChannelID, NetworkSequenceViewReader> readers = new HashMap<>();

		HandleClientConnection(RdmaShuffleServerEndpoint clientEndpoint) {
			this.clientEndpoint = clientEndpoint;
		}

		@Override
		public void run() {
			try {
//				LOG.info("Server accepted connection src " + clientEndpoint.getSrcAddr() + " dst: " + clientEndpoint
//					.getDstAddr());
				new Thread(new RDMAWriter(readers,clientEndpoint)).start();
				boolean clientClose = false;
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
								NetworkSequenceViewReader reader = readers.get(partitionRequest.receiverId);
								if (reader == null) {
									reader = new SequenceNumberingViewReader(partitionRequest.receiverId);
									reader.requestSubpartitionView(
										partitionProvider,
										partitionRequest.partitionId,
										partitionRequest.queueIndex);
//									LOG.info("received partition request: " + partitionRequest.receiverId + " with " +
//										"initial credit: " + partitionRequest.credit + "at endpoint: " +
//										getEndpointStr(clientEndpoint));
									reader.addCredit(partitionRequest.credit);
									readers.put(partitionRequest.receiverId, reader);
								}
								// we need to post receive for next message. for example credit
								RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId);
								// TODO(venkat): do something better here, we should not poll reader
							} else if (msgClazz == NettyMessage.TaskEventRequest.class) {
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
								NetworkSequenceViewReader reader = readers.get(request.receiverId);
								reader.addCredit(request.credit);
								RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId); // post next
								// receive
								// TODO (venkat): Handle it
//								LOG.error("Unhandled request type AddCredit");
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
						NettyMessage.BufferResponse response;
						synchronized (inFlight) {
							response = inFlight.remove(wc.getWr_id());
						}
						if (response !=null) {
							response.releaseBuffer();
							response.releaseTempBuf();
						}
						// send completed, so clear the buffer
						clientEndpoint.getSendBuffer().clear();
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
		private Map<InputChannelID, NetworkSequenceViewReader> readers = new HashMap<>();
		private RdmaShuffleServerEndpoint clientEndpoint;
		public RDMAWriter(Map<InputChannelID,NetworkSequenceViewReader> readers,RdmaShuffleServerEndpoint clientEndpoint){
			this.readers= readers;
			this.clientEndpoint = clientEndpoint;
		}

		@Override
		public void run() {
			while (!clientEndpoint.isClosed()) {
			try {
					for (NetworkSequenceViewReader reader : readers.values()) {
						while (((SequenceNumberingViewReader) reader).hasCredit()) {
							if (reader.isRegisteredAsAvailable()) {
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
									LOG.error(" Sending partition with seq. number: " + tmpResp.sequenceNumber + " receiver Id " + tmpResp.receiverId);
								} else {
									LOG.info("skip: sending error message/close request");
								}
//								clientEndpoint.getSendBuffer().put(response.write(bufferPool).nioBuffer());
//							clientEndpoint.getReceiveBuffer().clear();
//							RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId); // post next
								// receive

								// hold references of the response until the send completes
								if (response instanceof NettyMessage.BufferResponse) {
									response.write(bufferPool); // creates the header info
									RdmaSendReceiveUtil.postSendReqForBufferResponse(clientEndpoint, ++workRequestId, (NettyMessage.BufferResponse) response);
									synchronized (inFlight) {
										inFlight.put(workRequestId, (NettyMessage.BufferResponse) response);
									}
								} else {
									clientEndpoint.getSendBuffer().put(response.write(bufferPool).nioBuffer());
									RdmaSendReceiveUtil.postSendReq(clientEndpoint, ++workRequestId);
								}
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
