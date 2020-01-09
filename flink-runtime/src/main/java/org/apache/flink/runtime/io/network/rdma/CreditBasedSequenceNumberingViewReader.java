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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.rdma.PartitionRequestQueue;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;

import java.io.IOException;
// import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple wrapper for the subpartition view used in the new network credit-based mode.
 *
 * <p>It also keeps track of available buffers and notifies the outbound
 * handler about non-emptiness, similar to the {@link LocalInputChannel}.
 */
class CreditBasedSequenceNumberingViewReader implements BufferAvailabilityListener, NetworkSequenceViewReader {

	private final Object requestLock = new Object();

	private final InputChannelID receiverId;

	private final PartitionRequestQueue requestQueue;

	private volatile ResultSubpartitionView subpartitionView;

	// private AtomicBoolean canReceiveCredit = new AtomicBoolean();
	/**
	 * The status indicating whether this reader is already enqueued in the pipeline for transferring
	 * data or not.
	 *
	 * <p>It is mainly used to avoid repeated registrations but should be accessed by a single
	 * thread only since there is no synchronisation.
	 */
	private AtomicBoolean isRegisteredAsAvailable = new AtomicBoolean(false);

	/** The number of available buffers for holding data on the consumer side. */
	private AtomicInteger numCreditsAvailable;
	private AtomicInteger totalCredit;
	private boolean initialized = false;

	private int sequenceNumber = -1;

	CreditBasedSequenceNumberingViewReader(
		InputChannelID receiverId,
		int initialCredit,
		PartitionRequestQueue requestQueue) {

		this.receiverId = receiverId;
		this.numCreditsAvailable = new AtomicInteger(initialCredit);
		this.totalCredit= new AtomicInteger(initialCredit);
		this.requestQueue = requestQueue;
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
				initialized=true;

			} else {
				throw new IllegalStateException("Subpartition already requested");
			}
		}
	}

	@Override
	public void addCredit(int creditDeltas) {
		numCreditsAvailable.addAndGet(creditDeltas);
		totalCredit.addAndGet(creditDeltas);
	}

	@Override
	public int getAvailableCredit() {
		return numCreditsAvailable.get();
	}

	@Override
	public void setRegisteredAsAvailable(boolean isRegisteredAvailable) {
//		synchronized (this) {
			this.isRegisteredAsAvailable.getAndSet(isRegisteredAvailable);
//		}
	}

	@Override
	public boolean isRegisteredAsAvailable() {
//		synchronized (this) {
			return isRegisteredAsAvailable.get();
//		}
	}

	/**
	 * Returns true only if the next buffer is an event or the reader has both available
	 * credits and buffers.
	 */
	@Override
	public boolean isAvailable() {
		// BEWARE: this must be in sync with #isAvailable(BufferAndBacklog)!
		return hasBuffersAvailable() &&
			(numCreditsAvailable.get() > 0 || subpartitionView.nextBufferIsEvent());
	}

	/**
	 * Check whether this reader is available or not (internal use, in sync with
	 * {@link #isAvailable()}, but slightly faster).
	 *
	 * <p>Returns true only if the next buffer is an event or the reader has both available
	 * credits and buffers.
	 *
	 * @param bufferAndBacklog
	 * 		current buffer and backlog including information about the next buffer
	 */
	private boolean isAvailable(BufferAndBacklog bufferAndBacklog) {
		// BEWARE: this must be in sync with #isAvailable()!
		return hasBuffersAvailable() &&
			(numCreditsAvailable.get() > 0 || subpartitionView.nextBufferIsEvent());
	}

	@Override
	public InputChannelID getReceiverId() {
		return receiverId;
	}

	@Override
	public int getSequenceNumber() {
		return sequenceNumber;
	}

	@VisibleForTesting
	int getNumCreditsAvailable() {
		return numCreditsAvailable.get();
	}

	@VisibleForTesting
	boolean hasBuffersAvailable() {
		return subpartitionView.isAvailable();
	}

	@Override
	public BufferAndAvailability getNextBuffer() throws IOException, InterruptedException {
		if (numCreditsAvailable.decrementAndGet() < 0) {
			numCreditsAvailable.addAndGet(1);
			return null;
		}
		BufferAndBacklog next = subpartitionView.getNextBuffer();
		if (next != null) {
			sequenceNumber++;
			return new BufferAndAvailability(
				next.buffer(), isAvailable(next), next.buffersInBacklog());
		} else {
			numCreditsAvailable.addAndGet(1);
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
		if (initialized) { // should only notify after this reader initialization complete
			requestQueue.notifyReaderNonEmpty(this);
		}
	}

	@Override
	public String toString() {
		return "CreditBasedSequenceNumberingViewReader{" +
			"requestLock=" + requestLock +
			", receiverId=" + receiverId +
			", sequenceNumber=" + sequenceNumber +
			", numCreditsAvailable=" + numCreditsAvailable +
			", totalCreditReceived=" + totalCredit +
			", isRegisteredAsAvailable=" + isRegisteredAsAvailable +
			'}';
	}
}
