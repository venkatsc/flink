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

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.PartitionRequestClientIf;
import org.apache.flink.runtime.io.network.netty.NettyBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

//import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
//import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
//import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Factory for {@link PartitionRequestClient} instances.
 *
 * <p>Instances of partition requests clients are shared among several {@link RemoteInputChannel}
 * instances.
 */
class PartitionRequestClientFactory {
	private final HashMap<ConnectionID,RdmaClient> rdmaClients = new HashMap<>();
	private final ConcurrentMap<ConnectionID, Object> clients = new ConcurrentHashMap<ConnectionID, Object>();
	private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestClientFactory.class);
	private final NettyBufferPool bufferPool;
	private PartitionRequestClientHandler clientHandler ;
	private  NettyConfig rdmaCfg;

	PartitionRequestClientFactory(PartitionRequestClientHandler clientHandler, NettyBufferPool bufferPool, NettyConfig rdmaCfg) {
		this.clientHandler = clientHandler;
		this.bufferPool = bufferPool;
		this.rdmaCfg= rdmaCfg;
	}

	/**
	 * Atomically establishes a TCP connection to the given remote address and
	 * creates a {@link PartitionRequestClient} instance for this connection.
	 */
	PartitionRequestClientIf createPartitionRequestClient(ConnectionID connectionId) throws Exception {
		Object entry;
		PartitionRequestClient client = null;
		entry = clients.get(connectionId);
		if (entry != null) {
			// Existing channel or connecting channel
			if (entry instanceof PartitionRequestClient) {
				client = (PartitionRequestClient) entry;
			}
		} else {
			// No channel yet. Create one, but watch out for a race.
			// We create a "connecting future" and atomically add it to the map.
			// Only the thread that really added it establishes the channel.
			// The others need to wait on that original establisher's future.
			RdmaClient rdmaClient = new RdmaClient(rdmaCfg, clientHandler, bufferPool);
			rdmaClients.put(connectionId,rdmaClient);
			rdmaClient.start(connectionId.getAddress());
			client = new PartitionRequestClient(
				rdmaClient.getEndpoint(), clientHandler, connectionId, this);
//					if (disposeRequestClient) {
//						partitionRequestClient.disposeIfNotUsed();
//					}
			clients.putIfAbsent(connectionId, client);
		}
		// Make sure to increment the reference count before handing a client
		// out to ensure correct bookkeeping for channel closing.
		if (!client.incrementReferenceCounter()) {
			destroyPartitionRequestClient(connectionId, client);
			client = null;
		}
		return client;
	}

	public void closeOpenChannelConnections(ConnectionID connectionId) {
		RdmaClient rdmaClient=rdmaClients.get(connectionId);
		LOG.info("Asked to close the client connection");
		if (rdmaClient!=null){
			LOG.info("closing the client connection");
			rdmaClient.stop();
		}
	}

	int getNumberOfActiveClients() {
		return clients.size();
	}

	/**
	 * Removes the client for the given {@link ConnectionID}.
	 */
	void destroyPartitionRequestClient(ConnectionID connectionId, PartitionRequestClient client) {
		clients.remove(connectionId, client);
	}
}
