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
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;
import org.apache.commons.cli.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.apache.flink.runtime.io.network.netty.NettyBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;

public class RdmaClient implements RdmaEndpointFactory<RdmaShuffleClientEndpoint> {
	private static final Logger LOG = LoggerFactory.getLogger(RdmaClient.class);
	RdmaActiveEndpointGroup<RdmaShuffleClientEndpoint> endpointGroup;
	private final NettyConfig rdmaConfig;
	private int workRequestId = 1;
	private NettyBufferPool bufferPool;

	public RdmaShuffleClientEndpoint getEndpoint() {
		return endpoint;
	}

	private RdmaShuffleClientEndpoint endpoint;
	private PartitionRequestClientHandler clientHandler;

	public RdmaClient(NettyConfig rdmaConfig, PartitionRequestClientHandler clientHandler, NettyBufferPool bufferPool) {
		this.rdmaConfig = rdmaConfig;
		this.clientHandler = clientHandler;
		this.bufferPool = bufferPool;
	}

	public RdmaShuffleClientEndpoint createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
		endpointGroup = new RdmaActiveEndpointGroup<RdmaShuffleClientEndpoint>(1000, true, 128, 4, 128);
		endpointGroup.init(this);
		// we have passed our own endpoint factory to the group, therefore new endpoints will be of type
		// CustomClientEndpoint
		// let's create a new client endpoint
		endpoint = endpointGroup.createEndpoint();
		return new RdmaShuffleClientEndpoint(endpointGroup, idPriv, serverSide, 100, clientHandler, bufferPool);
	}

	public void start() throws IOException {
		endpoint = endpointGroup.createEndpoint();
		InetSocketAddress address = new InetSocketAddress(rdmaConfig.getServerAddress(), rdmaConfig.getServerPort());
		try {
			endpoint.connect(address, 1000);
		}catch (Exception e){
			LOG.error("failed to start the client ",e);
			throw new IOException("client failed to start");
		}

		System.out.println("SimpleClient::client channel set up ");
		// start and post a receive
		RdmaSendReceiveUtil.postReceiveReq(endpoint, ++workRequestId);
//		RdmaMessage.PartitionRequest request = new RdmaMessage.PartitionRequest();
//		request.writeTo(endpoint.getSendBuffer());
//		RdmaSendReceiveUtil.postSendReq(endpoint, ++workRequestId);
//		while (i <= 50) {
	}



//	public static void main(String[] args) throws Exception {
//		CmdLineCommon cmdLine = new CmdLineCommon("RdmaClient");
//		try {
//			cmdLine.parse(args);
//		} catch (ParseException e) {
//			cmdLine.printHelp();
//			System.exit(-1);
//		}
//		RdmaConfig rdmaConfig = new RdmaConfig(InetAddress.getByName(cmdLine.getIp()), cmdLine.getPort());
//		RdmaClient client = new RdmaClient(rdmaConfig, null, null); // TODO: need to pass client partition handler
//		client.run();
//	}

	public void stop() {
		try {
			System.out.println("server endpoint closed");
			endpointGroup.close();
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
	}
}
