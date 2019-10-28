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
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.RdmaCmId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;


import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;

public class RdmaClient implements RdmaEndpointFactory<RdmaShuffleClientEndpoint> {
	private static final Logger LOG = LoggerFactory.getLogger(RdmaClient.class);
	RdmaActiveEndpointGroup<RdmaShuffleClientEndpoint> endpointGroup;
	private final NettyConfig rdmaConfig;
	private int workRequestId = 1;
	private NettyBufferPool bufferPool;
	private Map<Long,IbvMr> registeredMrs;

	private RdmaShuffleClientEndpoint endpoint;
	private PartitionRequestClientHandler clientHandler;

	private NetworkBufferPool networkBufferPool;

	public RdmaClient(NettyConfig rdmaConfig, PartitionRequestClientHandler clientHandler, NettyBufferPool bufferPool, NetworkBufferPool networkBufferPool, Map<Long, IbvMr> registeredMRs) throws IOException {
		this.rdmaConfig = rdmaConfig;
		this.clientHandler = clientHandler;
		this.bufferPool = bufferPool;
		endpointGroup = new RdmaActiveEndpointGroup<RdmaShuffleClientEndpoint>(1000, true, 2000, 2, 1000);
		endpointGroup.init(this);
		endpointGroup.getConnParam().setRnr_retry_count((byte)7);
		this.networkBufferPool=networkBufferPool;
		this.registeredMrs=registeredMRs;
	}


	public RdmaShuffleClientEndpoint createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
		// we have passed our own endpoint factory to the group, therefore new endpoints will be of type
		// CustomClientEndpoint
		// let's create a new client endpoint
		return new RdmaShuffleClientEndpoint(endpointGroup, idPriv, serverSide, rdmaConfig.getMemorySegmentSize()+100, clientHandler, bufferPool);
	}

//	private void registerMemoryRegions(RdmaShuffleClientEndpoint endpoint) throws IOException {
//		long start = System.nanoTime();
//		for (int i = 0; i < networkBufferPool.getTotalNumberOfMemorySegments(); i++) {
//			MemorySegment segment=networkBufferPool.requestMemorySegment();
//			endpoint.registerMemory(segment.getAddress(),segment.size()).execute().getMr();
//			networkBufferPool.recycle(segment);
//		}
//		long end = System.nanoTime();
//		System.out.println("Client: Memory resgistration time for (in seconds): " + ((end
//			- start) / (1000.0*1000*1000)));
//	}

	public RdmaShuffleClientEndpoint start(InetSocketAddress address) throws IOException {
		endpoint = endpointGroup.createEndpoint();
		endpoint.setRegisteredMRs(registeredMrs);
		try {
			endpoint.connect(address, 1000);
		}catch (Exception e){
			LOG.error("failed to start the client ",e);
			throw new IOException("client failed to start",e);
		}

		LOG.info("SimpleClient::client channel set up ");
		// start and post a receive
		return endpoint;
	}

	public void stop() {
		try {
			LOG.info("client endpoint closed. src: "+ endpoint.getSrcAddr() + " dst: " +endpoint.getDstAddr());
			endpoint.terminate();
			endpointGroup.close();
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
	}
}
