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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.PartitionRequestClientIf;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.netty.NettyBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

public class RdmaConnectionManager implements ConnectionManager {
	private static final Logger LOG = LoggerFactory.getLogger(RdmaConnectionManager.class);

	private final RdmaServer server;

	private final RdmaClient client;

	private final NettyBufferPool bufferPool = new NettyBufferPool(8); // TODO (venkat): we might want to allocate
	// pool of buffers per
	// connection
	private PartitionRequestClientFactory partitionRequestClientFactory;

	public RdmaConnectionManager(NettyConfig rdmaConfig) {

		this.server = new RdmaServer(rdmaConfig,bufferPool);
		this.client = new RdmaClient(rdmaConfig, new PartitionRequestClientHandler(), bufferPool);
//		this.bufferPool = new NettyBufferPool(rdmaConfig.getNumberOfArenas());

	}

	@Override
	public void start(ResultPartitionProvider partitionProvider, TaskEventDispatcher taskEventDispatcher) throws
		IOException {
		server.setPartitionProvider(partitionProvider);
		server.setTaskEventDispatcher(taskEventDispatcher);
		server.start();
//		client.start(); // client just initializes, only starts when the createPartitionRequestClient called used ConnectionId
		// instead of netty config
		this.partitionRequestClientFactory = new PartitionRequestClientFactory(client);
	}

	@Override
	public PartitionRequestClientIf createPartitionRequestClient(ConnectionID connectionId) throws IOException,
		InterruptedException {
		try {
			PartitionRequestClientIf partitionRequestClient = partitionRequestClientFactory.createPartitionRequestClient(connectionId);
			LOG.info("creating partition client for connection id "+ connectionId.toString());
			if (partitionRequestClient==null){
				LOG.error("empty part partition client");
			}
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
		client.stop();
		server.stop();
	}
}
