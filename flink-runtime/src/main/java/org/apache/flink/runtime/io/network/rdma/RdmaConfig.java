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

import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class RdmaConfig {
	private static final Logger LOG = LoggerFactory.getLogger(RdmaConfig.class);
	private final InetAddress serverAddress;
	private int serverPort;

	private int memorySegmentSize;
	private int cqSize;
	private Configuration config;

	public RdmaConfig(
		InetAddress serverAddress,
		int serverPort,
		int memorySegmentSize,
		Configuration config, int cqSize) {

		this.serverAddress = checkNotNull(serverAddress);

		checkArgument(serverPort >= 0 && serverPort <= 65536, "Invalid port number.");
		this.serverPort = serverPort;

		checkArgument(memorySegmentSize > 0, "Invalid memory segment size.");
		this.memorySegmentSize = memorySegmentSize;
		checkArgument(cqSize > 0, "Invalid completion queue size.");
		this.cqSize = cqSize;
		this.config = checkNotNull(config);

		LOG.info(this.toString());
	}

	// TODO: remove (only for stand-alone running of server
	public RdmaConfig(
		InetAddress serverAddress,
		int serverPort) {
		this.serverAddress = serverAddress;
		this.serverPort = serverPort;
	}

	public InetAddress getServerAddress() {
		return serverAddress;
	}

	public int getServerPort() {
		return serverPort;
	}

	public int getMemorySegmentSize() {
		return memorySegmentSize;
	}

	public int getCqSize() {
		return cqSize;
	}

	public Configuration getConfig() {
		return config;
	}

}
