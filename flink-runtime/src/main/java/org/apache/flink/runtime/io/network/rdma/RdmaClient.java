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

//import com.ibm.disni.CmdLineCommon;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
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

public class RdmaClient implements RdmaEndpointFactory<RdmaShuffleClientEndpoint> {
	private static final Logger LOG = LoggerFactory.getLogger(RdmaClient.class);
	RdmaActiveEndpointGroup<RdmaShuffleClientEndpoint> endpointGroup;
	private final RdmaConfig rdmaConfig;
	private int workRequestId = 1;

	public RdmaShuffleClientEndpoint getEndpoint() {
		return endpoint;
	}

	private RdmaShuffleClientEndpoint endpoint;
	private PartitionRequestClientHandler clientHandler;

	public RdmaClient(RdmaConfig rdmaConfig, PartitionRequestClientHandler clientHandler) {
		this.rdmaConfig = rdmaConfig;
		this.clientHandler = clientHandler;
	}

	public RdmaShuffleClientEndpoint createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
		endpointGroup = new RdmaActiveEndpointGroup<RdmaShuffleClientEndpoint>(1000, true, 128, 4, 128);
		endpointGroup.init(this);
		//we have passed our own endpoint factory to the group, therefore new endpoints will be of type
		// CustomClientEndpoint
		//let's create a new client endpoint
		endpoint = endpointGroup.createEndpoint();
		return new RdmaShuffleClientEndpoint(endpointGroup, idPriv, serverSide, 100, clientHandler);
	}

	private void postReceiveBuffers(short numberOfBuffers, RdmaShuffleClientEndpoint endpoint) throws IOException,
		InterruptedException {
		ByteBuffer sendBuf = endpoint.getAvailableFreeReceiveBuffers();
		sendBuf.clear();

		LinkedList<IbvSge> sges = new LinkedList<IbvSge>();
		IbvSge sendSGE = new IbvSge();
		sendSGE.setAddr(endpoint.getAvailableFreeReceiveBuffersRegisteredMemory().getAddr());
		sendSGE.setLength(endpoint.getAvailableFreeReceiveBuffersRegisteredMemory().getLength());
		sendSGE.setLkey(endpoint.getAvailableFreeReceiveBuffersRegisteredMemory().getLkey());
		sges.add(sendSGE);

		System.out.println("posting credit from client");

		// Create send Work Request (WR)
		IbvSendWR sendWR = new IbvSendWR();
		sendWR.setWr_id(workRequestId++);
		sendWR.setSg_list(sges);
		sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
		sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);

		sendBuf.asShortBuffer().put(numberOfBuffers);

		LinkedList<IbvSendWR> sendWRs = new LinkedList<>();
		sendWRs.add(sendWR);

		endpoint.postSend(sendWRs).execute().free();
		IbvWC ibvWC = endpoint.getWcEvents().take();
		if (ibvWC.getStatus() == 0) {
			System.out.println("credit posted from client");
			return;
		} else {
			System.out.println("credit failed from client " + ibvWC.getVendor_err());
			postReceiveBuffers(numberOfBuffers, endpoint);
		}
	}

	public void run() throws Exception {
		//create a EndpointGroup. The RdmaActiveEndpointGroup contains CQ processing and delivers CQ event to the
		// endpoint.dispatchCqEvent() method.
		System.out.println("Starting client");
		//connect to the server
//		InetAddress ipAddress = InetAddress.getByName(host);
		InetSocketAddress address = new InetSocketAddress(rdmaConfig.getServerAddress(), rdmaConfig.getServerPort());
		endpoint.connect(address, 1000);
		// Post receive request
//		RdmaSendReceiveUtil.postReceiveReq(endpoint,++workRequestId);
		// TODO: give time for server to post Receive Work request RWR
		System.out.println("SimpleClient::client channel set up ");
		int i = 0;
		while (true) {
			i++;
			RdmaSendReceiveUtil.postReceiveReq(endpoint, ++workRequestId);
			ByteBuffer sendBuf = endpoint.getSendBuffer();
			sendBuf.asIntBuffer().put(1);
			RdmaSendReceiveUtil.postSendReq(endpoint, ++workRequestId);
			System.out.println("\n\nclient iteration " + i);
			IbvWC wc1 = endpoint.getWcEvents().take(); // receive finished
			RdmaSendReceiveUtil.repostOnFailure(wc1, endpoint, workRequestId);
			IbvWC wc = endpoint.getWcEvents().take(); // sent finished
			RdmaSendReceiveUtil.repostOnFailure(wc, endpoint, workRequestId);
			//the response should be received in this buffer, let's print it
			ByteBuffer recvBuf = endpoint.getreceiveBuffer();
			String message = recvBuf.asCharBuffer().toString();
			if (message.startsWith("finish\\0")) {
				sendBuf.clear();
//				RdmaSendReceiveUtil.postReceiveReq(endpoint,++workRequestId);
				System.out.println("Closing client connection");
				endpoint.getSendBuffer().asIntBuffer().put(127);
				RdmaSendReceiveUtil.postSendReq(endpoint, ++workRequestId);
				break;
			}
			System.out.println("Message from the server (recv): " + message);
			recvBuf.clear();
		}

//		System.exit(0);
	}

	public static void main(String[] args) throws Exception {
		CmdLineCommon cmdLine = new CmdLineCommon("RdmaClient");
		try {
			cmdLine.parse(args);
		} catch (ParseException e) {
			cmdLine.printHelp();
			System.exit(-1);
		}
		RdmaConfig rdmaConfig = new RdmaConfig(InetAddress.getByName(cmdLine.getIp()), cmdLine.getPort());
		RdmaClient client = new RdmaClient(rdmaConfig, null); // TODO: need to pass client partition handler
		client.run();
	}

	public void shutdown() {
		try {
			endpoint.close();
			System.out.println("client endpoint closed");
			System.out.println("server endpoint closed");
			endpointGroup.close();
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
	}
}


