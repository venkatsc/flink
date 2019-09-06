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

package org.apache.flink.runtime.io.network.api.reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;

import java.io.IOException;

/**
 * A record-oriented reader.
 *
 * <p>This abstract base class is used by both the mutable and immutable record readers.
 *
 * @param <T> The type of the record that can be read with this record reader.
 */
abstract class AbstractRecordReader<T extends IOReadableWritable> extends AbstractReader implements ReaderBase {
	private static final Logger LOG = LoggerFactory.getLogger(AbstractReader.class);

	private final RecordDeserializer<T>[] recordDeserializers;

	private RecordDeserializer<T> currentRecordDeserializer;

	private boolean isFinished;

	/**
	 * Creates a new AbstractRecordReader that de-serializes records from the given input gate and
	 * can spill partial records to disk, if they grow large.
	 *
	 * @param inputGate The input gate to read from.
	 * @param tmpDirectories The temp directories. USed for spilling if the reader concurrently
	 *                       reconstructs multiple large records.
	 */
	@SuppressWarnings("unchecked")
	protected AbstractRecordReader(InputGate inputGate, String[] tmpDirectories) {
		super(inputGate);

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[inputGate.getNumberOfInputChannels()];
		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<T>(tmpDirectories);
		}
	}

	protected boolean getNextRecord(T target) throws IOException, InterruptedException {
		if (isFinished) {
			return false;
		}

		while (true) {
			if (currentRecordDeserializer != null) {
				DeserializationResult result = currentRecordDeserializer.getNextRecord(target);

				if (result.isBufferConsumed()) {
					final Buffer currentBuffer = currentRecordDeserializer.getCurrentBuffer();

					currentBuffer.recycleBuffer();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					return true;
				}
			}

			final BufferOrEvent bufferOrEvent = inputGate.getNextBufferOrEvent().orElseThrow(IllegalStateException::new);

			if (bufferOrEvent.isBuffer()) {
				currentRecordDeserializer = recordDeserializers[bufferOrEvent.getChannelIndex()];
				currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
			}
			else {
				// sanity check for leftover data in deserializers. events should only come between
				// records, not in the middle of a fragment
				if (recordDeserializers[bufferOrEvent.getChannelIndex()].hasUnfinishedData()) {
					throw new IOException(
						"Received an event in channel " + bufferOrEvent.getChannelIndex() + " while still having "
							+ "data from a record. This indicates broken serialization logic. "
							+ "If you are using custom serialization code (Writable or Value types), check their "
							+ "serialization routines. In the case of Kryo, check the respective Kryo serializer.");
				}
				LOG.info("Received event");
				if (handleEvent(bufferOrEvent.getEvent())) {
					if (inputGate.isFinished()) {
						isFinished = true;
						return false;
					}
					else if (hasReachedEndOfSuperstep()) {
						return false;
					}
					// else: More data is coming...
				}
			}
		}
	}

	public void clearBuffers() {
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycleBuffer();
			}
			deserializer.clear();
		}
	}
}
