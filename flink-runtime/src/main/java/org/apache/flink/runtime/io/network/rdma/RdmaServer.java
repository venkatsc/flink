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

import com.esotericsoftware.minlog.Log;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.RdmaServerEndpoint;
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;
import org.apache.commons.cli.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.partition.ProducerFailedException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

public class RdmaServer implements RdmaEndpointFactory<RdmaShuffleServerEndpoint> {
	private static final Logger LOG = LoggerFactory.getLogger(RdmaServer.class);
	private RdmaActiveEndpointGroup<RdmaShuffleServerEndpoint> endpointGroup;
	private final NettyConfig rdmaConfig;
	private RdmaServerEndpoint<RdmaShuffleServerEndpoint> serverEndpoint;
	private InetSocketAddress address;
	private boolean stopped = false;

	public RdmaShuffleServerEndpoint getClientEndpoint() {
		return clientEndpoint;
	}

	private RdmaShuffleServerEndpoint clientEndpoint;
	private NettyBufferPool bufferPool;
	private NettyMessage.NettyMessageDecoder decoder = new NettyMessage.NettyMessageDecoder(false);
	private ResultPartitionProvider partitionProvider;
	private TaskEventDispatcher taskEventDispatcher;
	private RdmaServerRequestHandler handler;
	private NetworkBufferPool networkBufferPool;

	/**
	 * Creates the Queue pair endpoint and waits for the incoming connections
	 *
	 * @param idPriv
	 * @param serverSide
	 * @return
	 * @throws IOException
	 */
	public RdmaShuffleServerEndpoint createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
		return new RdmaShuffleServerEndpoint(endpointGroup, idPriv, serverSide, rdmaConfig.getMemorySegmentSize() +
			100);
	}

	public RdmaServer(NettyConfig rdmaConfig, NettyBufferPool bufferPool, NetworkBufferPool networkBufferPool) {
		this.rdmaConfig = rdmaConfig;
		this.bufferPool = bufferPool;
		this.networkBufferPool = networkBufferPool;
	}

	public void setPartitionProvider(ResultPartitionProvider partitionProvider) {
		this.partitionProvider = partitionProvider;
	}

	public void setTaskEventDispatcher(TaskEventDispatcher taskEventDispatcher) {
		this.taskEventDispatcher = taskEventDispatcher;
	}

	private void registerMemoryRegions(RdmaServerEndpoint<RdmaShuffleServerEndpoint> endpoint, Map<Long, IbvMr>
		registerdMRs) throws IOException {
		long start = System.nanoTime();
		for (int i = 0; i < networkBufferPool.getTotalNumberOfMemorySegments(); i++) {
			MemorySegment segment = networkBufferPool.requestMemorySegment();
			IbvMr mr = endpoint.registerMemory(segment.getAddress(), segment.size()).execute().getMr();
			registerdMRs.put(segment.getAddress(), mr);
			networkBufferPool.recycle(segment);
		}
		NetworkBufferPool.setHeaderBufferMR(endpoint.registerMemory(NetworkBufferPool.getBackingHeaderBuffer())
			.execute().getMr());
		long end = System.nanoTime();
		LOG.info("Server: Memory resgistration time for (in seconds): " + ((end
			- start) / (1000.0 * 1000 * 1000)));
	}

	public void start(Map<Long, IbvMr> registerdMRs) throws IOException {
		// create a EndpointGroup. The RdmaActiveEndpointGroup contains CQ processing and delivers CQ event to the
		// endpoint.dispatchCqEvent() method.
		endpointGroup = new RdmaActiveEndpointGroup<RdmaShuffleServerEndpoint>(1000, false, 4000,4, 2, 20000);
		endpointGroup.init(this);
		endpointGroup.getConnParam().setRnr_retry_count((byte) 7);
		// create a server endpoint
		serverEndpoint = endpointGroup.createServerEndpoint();

		// we can call bind on a server endpoint, just like we do with sockets
		// InetAddress ipAddress = InetAddress.getByName(host);
		address = new InetSocketAddress(rdmaConfig.getServerAddress(), rdmaConfig.getServerPort());
		try {
			serverEndpoint.bind(address, 10);
			registerMemoryRegions(serverEndpoint, registerdMRs);

		} catch (Exception e) {
			e.printStackTrace();
		}
		LOG.info("SimpleServer::servers bound to address " + address.toString());

		handler = new RdmaServerRequestHandler(serverEndpoint, partitionProvider, taskEventDispatcher, bufferPool,
			registerdMRs);
		Thread server = new Thread(handler);
		server.start();
		LOG.info("Server handler thread start at " + address.toString());
		// we can accept client connections untill this server is stopped
	}

	public int getPort() {
		return this.rdmaConfig.getServerPort();
	}

	public void stop() {
		try {
			handler.stop();
			serverEndpoint.close();
		} catch (Exception e) {
			LOG.error("Failed to stop server ", e);
		}
	}
}
