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

import com.ibm.disni.verbs.IbvMr;
import org.apache.commons.collections.map.HashedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.PartitionRequestClientIf;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;

public class RdmaConnectionManager implements ConnectionManager {
	private static final Logger LOG = LoggerFactory.getLogger(RdmaConnectionManager.class);
    public static final int DATA_MSG_HEADER_SIZE = 39;
	private final RdmaServer server;
	private final NettyConfig rdmaConfig;
	private final NetworkBufferPool networkBufferPool; //off-heap buffers
    private RdmaClient rdmaClient;
//	private final RdmaClient client;
	private Map<Long,IbvMr> registeredMRs=new HashedMap();

	private final NettyBufferPool bufferPool = new NettyBufferPool(8); // TODO (venkat): we might want to allocate
	// pool of buffers per
	// connection
	private PartitionRequestClientFactory partitionRequestClientFactory;

	public RdmaConnectionManager(NettyConfig rdmaConfig, NetworkBufferPool networkBufferPool) {
		this.rdmaConfig=rdmaConfig;
		this.networkBufferPool=networkBufferPool;
		this.server = new RdmaServer(rdmaConfig,bufferPool,networkBufferPool);
	}

	@Override
	public void start(ResultPartitionProvider partitionProvider, TaskEventDispatcher taskEventDispatcher) throws
		IOException {
		server.setPartitionProvider(partitionProvider);
		server.setTaskEventDispatcher(taskEventDispatcher);
		server.start(registeredMRs);
		rdmaClient= new RdmaClient(rdmaConfig, new PartitionRequestClientHandler(), bufferPool,networkBufferPool,registeredMRs);
//		client.start(); // client just initializes, only starts when the createPartitionRequestClient called used ConnectionId
		// instead of netty config
		this.partitionRequestClientFactory = new PartitionRequestClientFactory(new PartitionRequestClientHandler(), bufferPool,rdmaConfig,rdmaClient);
	}

	@Override
	public PartitionRequestClientIf createPartitionRequestClient(ConnectionID connectionId, InputChannel channel) throws IOException,
		InterruptedException {
		try {
			PartitionRequestClientIf partitionRequestClient = partitionRequestClientFactory.createPartitionRequestClient(connectionId,channel);
			return partitionRequestClient;
		} catch (Exception e) {
			LOG.error("Error occurred. Rethrowing it as IOException ",e);
			throw new IOException(e);
		}
	}

	@Override
	public void closeOpenChannelConnections(ConnectionID connectionId) {
		partitionRequestClientFactory.closeOpenChannelConnections(connectionId);
	}

	@Override
	public int getNumberOfActiveConnections() {
		return partitionRequestClientFactory.getNumberOfActiveClients();
	}

	@Override
	public int getDataPort() {
			return server.getPort();
	}

	@Override
	public void shutdown() throws IOException {
//		client.stop();
		server.stop();
	}
}
