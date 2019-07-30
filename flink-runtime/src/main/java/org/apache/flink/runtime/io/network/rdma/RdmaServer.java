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
import com.ibm.disni.verbs.IbvRecvWR;
import com.ibm.disni.verbs.IbvSendWR;
import com.ibm.disni.verbs.IbvSge;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;
import org.apache.commons.cli.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;

public class RdmaServer implements RdmaEndpointFactory<RdmaShuffleServerEndpoint> {
	private static final Logger LOG = LoggerFactory.getLogger(RdmaServer.class);
	private RdmaActiveEndpointGroup<RdmaShuffleServerEndpoint> endpointGroup;
	private final RdmaConfig rdmaConfig;
	private int workRequestId = 1;
	private RdmaServerEndpoint<RdmaShuffleServerEndpoint> serverEndpoint;
	private InetSocketAddress address;
	private RdmaShuffleServerEndpoint clientEndpoint;
	private PartitionRequestServerHandler serverHandler;

	public RdmaShuffleServerEndpoint createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
		endpointGroup = new RdmaActiveEndpointGroup<RdmaShuffleServerEndpoint>(1000, true, 128, 4, 128);
		endpointGroup.init(this);
		//create a server endpoint
		serverEndpoint = endpointGroup.createServerEndpoint();

		//we can call bind on a server endpoint, just like we do with sockets
//		InetAddress ipAddress = InetAddress.getByName(host);
		address = new InetSocketAddress(rdmaConfig.getServerAddress(), rdmaConfig.getServerPort());
		try {
			serverEndpoint.bind(address, 10);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new RdmaShuffleServerEndpoint(endpointGroup, idPriv, serverSide, 100, serverHandler);
	}

	public RdmaServer(RdmaConfig rdmaConfig, PartitionRequestServerHandler serverHandler) {
		this.rdmaConfig = rdmaConfig;
		this.serverHandler = serverHandler;
	}

	private short getAvailableReceiveBufferCount(RdmaShuffleServerEndpoint clientEndpoint) throws IOException,
		InterruptedException {

		ByteBuffer availableClientBuffCount = clientEndpoint.getAvailableFreeReceiveBuffers();
		availableClientBuffCount.clear();
		LinkedList<IbvSge> recvSges = new LinkedList<IbvSge>();
		IbvSge recvSGE = new IbvSge();
		recvSGE.setAddr(clientEndpoint.getAvailableFreeReceiveBuffersRegisteredMemory().getAddr());
		recvSGE.setLength(clientEndpoint.getAvailableFreeReceiveBuffersRegisteredMemory().getLength());
		recvSGE.setLkey(clientEndpoint.getAvailableFreeReceiveBuffersRegisteredMemory().getLkey());
		recvSges.add(recvSGE);

		IbvRecvWR recvWR = new IbvRecvWR();
		recvWR.setWr_id(workRequestId++);
		recvWR.setSg_list(recvSges);

		System.out.println("posting empty buffer count receive request ");
		LinkedList<IbvRecvWR> recvWRs = new LinkedList<>();
		recvWRs.add(recvWR);
		clientEndpoint.postRecv(recvWRs).execute().free();

		// send receive post notification to the client
		LinkedList<IbvSge> sges = new LinkedList<IbvSge>();
		IbvSge sendSGE = new IbvSge();
		sendSGE.setAddr(clientEndpoint.getAvailableFreeReceiveBuffersNotificationRegisteredMemory().getAddr());
		sendSGE.setLength(clientEndpoint.getAvailableFreeReceiveBuffersNotificationRegisteredMemory().getLength());
		sendSGE.setLkey(clientEndpoint.getAvailableFreeReceiveBuffersNotificationRegisteredMemory().getLkey());
		sges.add(sendSGE);

		// Create send Work Request (WR)
		IbvSendWR sendWR = new IbvSendWR();
		sendWR.setWr_id(workRequestId++);
		sendWR.setSg_list(sges);
		sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
		sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);

		LinkedList<IbvSendWR> sendWRs1 = new LinkedList<>();
		sendWRs1.add(sendWR);
		System.out.println("Sending credit request notification to the client");
		ByteBuffer sendBuf = clientEndpoint.getAvailableFreeReceiveBuffersNotification();
		sendBuf.asShortBuffer().put((short) 0);
		clientEndpoint.postSend(sendWRs1).execute().free();

		IbvWC ibvWC = clientEndpoint.getWcEvents().take();
		IbvWC ibvWC1 = clientEndpoint.getWcEvents().take();

		if (ibvWC.getStatus() == 0) {
			System.out.println("buffers received");
		} else {
			System.out.println("failed to get buffers");
		}
		sendBuf.clear();

		return availableClientBuffCount.getShort();
	}

	public void run() throws Exception {
		//create a EndpointGroup. The RdmaActiveEndpointGroup contains CQ processing and delivers CQ event to the
		// endpoint.dispatchCqEvent() method.
		System.out.println("SimpleServer::servers bound to address " + address.toString());

		//we can accept new connections
		clientEndpoint = serverEndpoint.accept();
		RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId);
		//we have previously passed our own endpoint factory to the group, therefore new endpoints will be of type
		// CustomServerEndpoint
		System.out.println("SimpleServer::client connection accepted");

		int i = 0;
		String message;

		while (true) {
			i++;

			System.out.println("\n\nserver getting the wc");
			IbvWC wc = clientEndpoint.getWcEvents().take();
			RdmaSendReceiveUtil.repostOnFailure(wc, clientEndpoint, workRequestId);
			System.out.println("server wc " + wc.getOpcode() + " wr_id" + wc.getWr_id());
			int msg = clientEndpoint.getReceiveBuffer().asIntBuffer().get(0);
			System.out.println("sending request " + i + " client status " + msg);
			clientEndpoint.getReceiveBuffer().clear();
			if (i == 50) {
				message = "finish\\0";
				System.out.println("posting immediate");
				ByteBuffer sendBuffer = clientEndpoint.getSendBuffer();
				sendBuffer.asCharBuffer().put(message);
				RdmaSendReceiveUtil.postSendReq(clientEndpoint, ++workRequestId);
				IbvWC wcc = clientEndpoint.getWcEvents().take();
				RdmaSendReceiveUtil.repostOnFailure(wcc, clientEndpoint, workRequestId);
				System.out.println("Sent message " + message);
				sendBuffer.clear();
				RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId); // post receive for next message
				// msg 127 should be received next
				continue;
			}
			if (msg == 1) { //client is ready to get the data
				message = "Hello from the server " + i;
//				message= "finish\\0";
				System.out.println("Message sending started ");
				ByteBuffer sendBuffer = clientEndpoint.getSendBuffer();
				sendBuffer.asCharBuffer().put(message);
				RdmaSendReceiveUtil.postSendReq(clientEndpoint, ++workRequestId);
				IbvWC wcSend = clientEndpoint.getWcEvents().take();
				RdmaSendReceiveUtil.repostOnFailure(wcSend, clientEndpoint, workRequestId);
				System.out.println("WCSend type " + wcSend.getOpcode() + " wr_id " + wcSend.getWr_id());
				RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId); // post receive for next message
				System.out.println("Message sending finished ");
				sendBuffer.clear();
				System.out.println("Sent message " + message);
			} else if (msg == 127) {
				System.out.println("Closing server connection");
				break;
			}
		}
		//close everything
		System.out.println("group closed");
//		System.exit(0);
	}

	public static void main(String[] args) throws Exception {
		CmdLineCommon cmdLine = new CmdLineCommon("RdmaServer");
		try {
			cmdLine.parse(args);
		} catch (ParseException e) {
			cmdLine.printHelp();
			System.exit(-1);
		}
		RdmaConfig rdmaConfig = new RdmaConfig(InetAddress.getByName(cmdLine.getIp()), cmdLine.getPort());
		RdmaServer server = new RdmaServer(rdmaConfig, null); //TODO (venkat): it should not be null
		server.run();
	}

	public void shutdown() {
		try {
			clientEndpoint.close();
			System.out.println("client endpoint closed");
			serverEndpoint.close();
			System.out.println("server endpoint closed");
			endpointGroup.close();
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
	}
}
