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

import java.io.IOException;
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
	private final HashMap<ConnectionID,RdmaShuffleClientEndpoint> clientEndpoints = new HashMap<>();
	private final ConcurrentMap<ConnectionID, PartitionRequestClient> clients = new ConcurrentHashMap<ConnectionID, PartitionRequestClient>();
	private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestClientFactory.class);
	private final NettyBufferPool bufferPool;
	private PartitionRequestClientHandler clientHandler ;
	private  NettyConfig rdmaCfg;
	private RdmaClient rdmaClient;

	PartitionRequestClientFactory(PartitionRequestClientHandler clientHandler, NettyBufferPool bufferPool, NettyConfig rdmaCfg,RdmaClient rdmaClient) {
		this.clientHandler = clientHandler;
		this.bufferPool = bufferPool;
		this.rdmaCfg= rdmaCfg;
		this.rdmaClient=rdmaClient;
	}

	/**
	 * Atomically establishes a TCP connection to the given remote address and
	 * creates a {@link PartitionRequestClient} instance for this connection.
	 */
	PartitionRequestClientIf createPartitionRequestClient(ConnectionID connectionId) throws Exception {
		Object entry;
		PartitionRequestClient client = clients.get(connectionId);;
		if (client == null) {
			// No channel yet. Create one, but watch out for a race.
			// We create a "connecting future" and atomically add it to the map.
			// Only the thread that really added it establishes the channel.
			// The others need to wait on that original establisher's future.
			synchronized (clients) { // let us start client connections in synchronous way, so that we don't have same
				// target connection multiple times
				client = clients.get(connectionId);;
				if (client==null){
					RdmaShuffleClientEndpoint endpoint = rdmaClient.start(connectionId.getAddress());
					clientEndpoints.put(connectionId, endpoint);
					client = new PartitionRequestClient(
						endpoint, clientHandler, connectionId, this);
//					if (disposeRequestClient) {
//						partitionRequestClient.disposeIfNotUsed();
//					}
					rdmaClient.start(connectionId.getAddress());
					clients.putIfAbsent(connectionId, client);
					LOG.info("creating partition client {} for connection id {}", endpoint.getEndpointStr(), connectionId.toString());
				}

			}
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
		RdmaShuffleClientEndpoint endpoint=clientEndpoints.get(connectionId);
		LOG.info("Asked to close the client connection");
		if (endpoint!=null){
			LOG.info("closing the client connection");
			try {
				endpoint.close();
			} catch (IOException e) {
				LOG.error("Failed to close client connection",e);
			} catch (InterruptedException e) {
				LOG.error("Failed to close client connection",e);
			}
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
