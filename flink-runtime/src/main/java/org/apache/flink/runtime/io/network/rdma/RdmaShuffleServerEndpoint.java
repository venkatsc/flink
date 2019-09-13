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

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;

/**
 * Server sends are big chunks of serialized records but
 * Server receives are small sized (event) messages. So, sends should NOT
 * create copies of the data. With this in mind, this class
 * uses pre-allocated off-heap memory for the sends and does not care
 * much about receives and allocates some direct buffers for the receive calls
 **/
public class RdmaShuffleServerEndpoint extends RdmaActiveEndpoint {
	private static final Logger LOG = LoggerFactory.getLogger(RdmaShuffleServerEndpoint.class);
	private int workRequestId = 0;
	private int bufferSize; // Todo: set default buffer size


	private ByteBuffer receiveBuffer; // Todo: add buffer manager with multiple buffers
	private IbvMr registeredReceiveMemory;
	private NetworkBufferPool bufferPool; // un-divided network buffers (all network buffers)
	private HashMap<Long,IbvMr> sendMrs = new HashMap<>();


	private ArrayBlockingQueue<IbvWC> wcEvents = new ArrayBlockingQueue<>(1000);

	public RdmaShuffleServerEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> group, RdmaCmId idPriv,
									 boolean serverSide, int bufferSize, NetworkBufferPool bufferPool) throws
		IOException {
		super(group, idPriv, serverSide);
		this.bufferSize = bufferSize; // Todo: validate buffer size
		this.bufferPool = bufferPool;
	}

	@Override
	public void dispatchCqEvent(IbvWC wc) throws IOException {
		synchronized (this) {
			wcEvents.add(wc);
		}
	}

	public void init() throws IOException {
		super.init();
		int totalSegments=bufferPool.getTotalNumberOfMemorySegments();

		if (bufferPool.getAllMemorySegments().size() != bufferPool.getTotalNumberOfMemorySegments()) {
			throw new IOException("All segments " + bufferPool.getAllMemorySegments().size() + " did not " +
				"match total segments " + bufferPool.getTotalNumberOfMemorySegments());
		}

		for (MemorySegment segment: bufferPool.getAllMemorySegments()){
			IbvMr mr = registerMemory(segment.getAddress(),segment.size()).execute().getMr();
			sendMrs.put(segment.getAddress(),mr);
		}

//		this.sendBuffer = ByteBuffer.allocateDirect(bufferSize); // allocate buffer
		this.receiveBuffer = ByteBuffer.allocateDirect(bufferSize);
		this.registeredReceiveMemory = registerMemory(receiveBuffer).execute().getMr();
//		this.registeredSendMemory = registerMemory(sendBuffer).execute().getMr(); // register the send buffer
	}

	public ByteBuffer getReceiveBuffer() {
		return this.receiveBuffer;
	}

	public IbvMr getRegisteredReceiveMemory() {
		return registeredReceiveMemory;
	}

	public ArrayBlockingQueue<IbvWC> getWcEvents() {
		synchronized (this) {
			return wcEvents;
		}
	}

	public IbvMr getRegisteredSendMemoryByAddress(long address){
		return sendMrs.get(address);
	}

	public void stop() {
		try {
			LOG.info("Server endpoint closed. src: " + this.getSrcAddr() + " dst: " + this.getDstAddr());
			this.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
