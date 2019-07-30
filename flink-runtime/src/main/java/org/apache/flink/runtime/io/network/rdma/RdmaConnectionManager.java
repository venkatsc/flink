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

import java.io.IOException;

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

public class RdmaConnectionManager implements ConnectionManager {

	private final RdmaServer server;

	private final RdmaClient client;

	private PartitionRequestServerHandler serverHandler;

	//	private final NettyBufferPool bufferPool; // TODO (venkat): we might want to allocate pool of buffers per
	// connection
	private final PartitionRequestClientFactory partitionRequestClientFactory;

	public RdmaConnectionManager(RdmaConfig rdmaConfig) {

		this.server = new RdmaServer(rdmaConfig, serverHandler);
		this.client = new RdmaClient(rdmaConfig, new PartitionRequestClientHandler());
//		this.bufferPool = new NettyBufferPool(rdmaConfig.getNumberOfArenas());

		this.partitionRequestClientFactory = new PartitionRequestClientFactory(client);
	}

	@Override
	public void start(ResultPartitionProvider partitionProvider, TaskEventDispatcher taskEventDispatcher) throws
		IOException {
		PartitionRequestQueue queueOfPartitionQueues = new PartitionRequestQueue();
		this.serverHandler = new PartitionRequestServerHandler(
			partitionProvider, taskEventDispatcher, queueOfPartitionQueues, false);
	}

	@Override
	public PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId) throws IOException,
		InterruptedException {
		try {
			return partitionRequestClientFactory.createPartitionRequestClient(connectionId);
		} catch (Exception e) {
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
		return -1;
	}

	@Override
	public void shutdown() throws IOException {
		client.shutdown();
		server.shutdown();
	}
}
