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
import com.ibm.disni.verbs.RdmaCmId;
import org.apache.commons.cli.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.flink.runtime.io.network.netty.NettyBufferPool;

public class RdmaServer implements RdmaEndpointFactory<RdmaShuffleServerEndpoint> {
	private static final Logger LOG = LoggerFactory.getLogger(RdmaServer.class);
	private RdmaActiveEndpointGroup<RdmaShuffleServerEndpoint> endpointGroup;
	private final RdmaConfig rdmaConfig;
	private int workRequestId = 1;
	private RdmaServerEndpoint<RdmaShuffleServerEndpoint> serverEndpoint;
	private InetSocketAddress address;

	public RdmaShuffleServerEndpoint getClientEndpoint() {
		return clientEndpoint;
	}

	private RdmaShuffleServerEndpoint clientEndpoint;
	private PartitionRequestServerHandler serverHandler;
	private NettyBufferPool bufferPool;

	/**
	 * Creates the Queue pair endpoint and waits for the incoming connections
	 *
	 * @param idPriv
	 * @param serverSide
	 * @return
	 * @throws IOException
	 */
	public RdmaShuffleServerEndpoint createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
		return new RdmaShuffleServerEndpoint(endpointGroup, idPriv, serverSide, 100, serverHandler);
	}

	public RdmaServer(RdmaConfig rdmaConfig, PartitionRequestServerHandler serverHandler, NettyBufferPool bufferPool) {
		this.rdmaConfig = rdmaConfig;
		this.serverHandler = serverHandler;
		this.bufferPool = bufferPool;
	}

	public void run() throws Exception {
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

		// we can accept new connections
		clientEndpoint = serverEndpoint.accept();

		RdmaSendReceiveUtil.postReceiveReq(clientEndpoint, ++workRequestId);
		// we have previously passed our own endpoint factory to the group, therefore new endpoints will be of type
		// CustomServerEndpoint
		// System.out.println("SimpleServer::client connection accepted");

		int i = 0;
		String message;
		// close everything
		System.out.println("group closed");
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
		RdmaServer server = new RdmaServer(rdmaConfig, null,new NettyBufferPool(8)); //TODO (venkat): it should not be null
		server.run();
	}

	public void shutdown() {
		try {
			// clientEndpoint.close();
			// System.out.println("client endpoint closed");
			serverEndpoint.close();
			System.out.println("server endpoint closed");
			endpointGroup.close();
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
	}
}
