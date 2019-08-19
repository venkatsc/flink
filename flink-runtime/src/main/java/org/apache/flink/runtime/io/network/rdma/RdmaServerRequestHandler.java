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

public class RdmaServerRequestHandler implements Runnable{
	private RdmaShuffleServerEndpoint clientEndpoint;
	private boolean stopped = false;
	private RdmaServerEndpoint<RdmaShuffleServerEndpoint> serverEndpoint;
	private int workRequestId=0;
	private static final Logger LOG = LoggerFactory.getLogger(RdmaServerRequestHandler.class);

	private final NettyBufferPool bufferPool;
	private NettyMessage.NettyMessageDecoder decoder = new NettyMessage.NettyMessageDecoder(false);
	private final ResultPartitionProvider partitionProvider;
	private final TaskEventDispatcher taskEventDispatcher;
	private final Map<InputChannelID, NetworkSequenceViewReader> readers = new HashMap<>();


	public RdmaServerRequestHandler(RdmaServerEndpoint<RdmaShuffleServerEndpoint> serverEndpoint,ResultPartitionProvider partitionProvider,TaskEventDispatcher taskEventDispatcher,NettyBufferPool bufferPool){
		this.serverEndpoint=  serverEndpoint;
		this.partitionProvider= partitionProvider;
		this.taskEventDispatcher= taskEventDispatcher;
		this.bufferPool = bufferPool;
	}

	@Override
	public void run() {
		while (!stopped) {
			try {
			// TODO: accept should be seperated into other thread
			clientEndpoint = serverEndpoint.accept();
			LOG.info("Server accepted connection src "+ clientEndpoint.getSrcAddr() + " dst: "+clientEndpoint.getDstAddr());
			RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId);
			boolean clientClose = false;
				while (!clientClose) {
					IbvWC wc = null;
					wc = clientEndpoint.getWcEvents().take();
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
								LOG.info("received partition request");
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
								RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId); // post next
								// receive
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
			} catch (Exception ie) {
				Log.error("Failed to handle request",ie);
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

	public void stop(){
		stopped=true;
	}
}
