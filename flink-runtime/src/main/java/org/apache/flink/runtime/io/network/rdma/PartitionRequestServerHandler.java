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

import static org.apache.flink.runtime.io.network.rdma.NettyMessage.PartitionRequest;
import static org.apache.flink.runtime.io.network.rdma.NettyMessage.TaskEventRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
//import org.apache.flink.runtime.io.network.netty.CreditBasedSequenceNumberingViewReader;
import org.apache.flink.runtime.io.network.rdma.NettyMessage.AddCredit;
import org.apache.flink.runtime.io.network.rdma.NettyMessage.CancelPartitionRequest;
import org.apache.flink.runtime.io.network.rdma.NettyMessage.CloseRequest;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

/**
 * Channel handler to initiate data transfers and dispatch backwards flowing task events.
 */
class PartitionRequestServerHandler {

	private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestServerHandler.class);

	private final ResultPartitionProvider partitionProvider;

	private final TaskEventDispatcher taskEventDispatcher;

	private final PartitionRequestQueue outboundQueue;

	private final boolean creditBasedEnabled;

	PartitionRequestServerHandler(
		ResultPartitionProvider partitionProvider,
		TaskEventDispatcher taskEventDispatcher,
		PartitionRequestQueue outboundQueue,
		boolean creditBasedEnabled) {

		this.partitionProvider = partitionProvider;
		this.taskEventDispatcher = taskEventDispatcher;
		this.outboundQueue = outboundQueue;
		this.creditBasedEnabled = creditBasedEnabled;
	}

	protected void channelRead(RdmaShuffleServerEndpoint endpoint, NettyMessage msg) throws Exception {
		try {
			Class<?> msgClazz = msg.getClass();

			// ----------------------------------------------------------------
			// Intermediate result partition requests
			// ----------------------------------------------------------------
			if (msgClazz == PartitionRequest.class) {
				PartitionRequest request = (PartitionRequest) msg;

				LOG.debug("Read channel on {}: {}.", endpoint.getSrcAddr(), request);

				try {
					NetworkSequenceViewReader reader;
//					if (creditBasedEnabled) {
//						reader = new CreditBasedSequenceNumberingViewReader(
//							request.receiverId,
//							request.credit,
//							outboundQueue);
//					} else {
					reader = new SequenceNumberingViewReader(
						request.receiverId,
						outboundQueue);
//					}

					reader.requestSubpartitionView(
						partitionProvider,
						request.partitionId,
						request.queueIndex);

					outboundQueue.notifyReaderCreated(reader);
				} catch (PartitionNotFoundException notFound) {
					respondWithError(endpoint, notFound, request.receiverId);
				}
			}
			// ----------------------------------------------------------------
			// Task events
			// ----------------------------------------------------------------
			else if (msgClazz == TaskEventRequest.class) {
				TaskEventRequest request = (TaskEventRequest) msg;

				if (!taskEventDispatcher.publish(request.partitionId, request.event)) {
					respondWithError(endpoint, new IllegalArgumentException("Task event receiver not found."), request
						.receiverId);
				}
			} else if (msgClazz == CancelPartitionRequest.class) {
				CancelPartitionRequest request = (CancelPartitionRequest) msg;

				outboundQueue.cancel(request.receiverId);
			} else if (msgClazz == CloseRequest.class) {
				outboundQueue.close();
			} else if (msgClazz == AddCredit.class) {
				AddCredit request = (AddCredit) msg;

				outboundQueue.addCredit(request.receiverId, request.credit);
			} else {
				LOG.warn("Received unexpected client request: {}", msg);
			}
		} catch (Throwable t) {
			respondWithError(endpoint, t);
		}
	}

	private void respondWithError(RdmaShuffleServerEndpoint endpoint, Throwable error) {
		endpoint.write(new NettyMessage.ErrorResponse(error));
	}

	private void respondWithError(RdmaShuffleServerEndpoint endpoint, Throwable error, InputChannelID sourceId) {
		LOG.debug("Responding with error: {}.", error.getClass());

		endpoint.write(new NettyMessage.ErrorResponse(error, sourceId));
	}
}
