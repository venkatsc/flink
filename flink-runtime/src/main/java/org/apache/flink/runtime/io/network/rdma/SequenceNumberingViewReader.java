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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;

/**
 * Simple wrapper for the subpartition view used in the old network mode.
 *
 * <p>It also keeps track of available buffers and notifies the outbound
 * handler about non-emptiness, similar to the {@link LocalInputChannel}.
 */
class SequenceNumberingViewReader implements BufferAvailabilityListener, NetworkSequenceViewReader {
	private static final Logger LOG = LoggerFactory.getLogger(SequenceNumberingViewReader.class);


	private final Object requestLock = new Object();

	private final InputChannelID receiverId;

	private volatile ResultSubpartitionView subpartitionView;

	private int sequenceNumber = -1;
	private int credit= 0;
	private boolean isRegisteredAvailable;

	SequenceNumberingViewReader(InputChannelID receiverId) {
		this.receiverId = receiverId;
//		this.requestQueue = requestQueue;
	}

	@Override
	public void requestSubpartitionView(
		ResultPartitionProvider partitionProvider,
		ResultPartitionID resultPartitionId,
		int subPartitionIndex) throws IOException {

		synchronized (requestLock) {
			if (subpartitionView == null) {
				// This this call can trigger a notification we have to
				// schedule a separate task at the event loop that will
				// start consuming this. Otherwise the reference to the
				// view cannot be available in getNextBuffer().
				this.subpartitionView = partitionProvider.createSubpartitionView(
					resultPartitionId,
					subPartitionIndex,
					this);
			} else {
				throw new IllegalStateException("Subpartition already requested");
			}
		}
	}

	@Override
	public void addCredit(int credit) {
		synchronized (this) {
			this.credit = credit;
			this.notifyAll();
		}
	}

	public void decrementCredit(){
		this.credit--;
	}
	public boolean hasCredit(){
		return credit>0 ;
	}

	@Override
	public void setRegisteredAsAvailable(boolean isRegisteredAvailable) {
		synchronized (this) {
			this.isRegisteredAvailable = isRegisteredAvailable;
			this.notifyAll();
		}
	}

	@Override
	public boolean isRegisteredAsAvailable() {
		synchronized (this) {
			return isRegisteredAvailable;
		}
	}

	@Override
	public boolean isAvailable() {
		return subpartitionView.isAvailable();
	}

	@Override
	public InputChannelID getReceiverId() {
		return receiverId;
	}

	@Override
	public int getSequenceNumber() {
		return sequenceNumber;
	}

	@Override
	public BufferAndAvailability getNextBuffer() throws IOException, InterruptedException {
		BufferAndBacklog next = subpartitionView.getNextBuffer();
		if (next != null) {
			sequenceNumber++;
			return new BufferAndAvailability(next.buffer(), next.isMoreAvailable(), next.buffersInBacklog());
		} else {
			return null;
		}
	}

	@Override
	public void notifySubpartitionConsumed() throws IOException {
		subpartitionView.notifySubpartitionConsumed();
	}

	@Override
	public boolean isReleased() {
		return subpartitionView.isReleased();
	}

	@Override
	public Throwable getFailureCause() {
		return subpartitionView.getFailureCause();
	}

	@Override
	public void releaseAllResources() throws IOException {
		subpartitionView.releaseAllResources();
	}

	@Override
	public void notifyDataAvailable() {
//		LOG.debug("Received data available notification "+this);//		requestQueue.notifyReaderNonEmpty(this); // TODO (venkat): Might read the data before available
		synchronized (this) {
//			this.setRegisteredAsAvailable(true);
			this.notifyAll();
		}
	}

	@Override
	public String toString() {
		return "SequenceNumberingViewReader{" +
			"requestLock=" + requestLock +
			", receiverId=" + receiverId +
			", sequenceNumber=" + sequenceNumber +
			", isAvailable=" + isAvailable() +
			", credit="+credit+
			'}';
	}
}
