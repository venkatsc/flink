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

package org.apache.flink.runtime.io.network;

import java.io.IOException;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;

public interface PartitionRequestClientIf {

	ChannelFuture requestSubpartition(
		final ResultPartitionID partitionId,
		final int subpartitionIndex,
		final RemoteInputChannel inputChannel,
		int delayMs) throws IOException;

	/**
	 * Sends a task event backwards to an intermediate result partition producer.
	 * <p>
	 * Backwards task events flow between readers and writers and therefore
	 * will only work when both are running at the same time, which is only
	 * guaranteed to be the case when both the respective producer and
	 * consumer task run pipelined.
	 */
	void sendTaskEvent(ResultPartitionID partitionId, TaskEvent event, final RemoteInputChannel inputChannel) throws IOException;

	void notifyCreditAvailable(RemoteInputChannel inputChannel);

    void close(RemoteInputChannel inputChannel) throws IOException;
}
