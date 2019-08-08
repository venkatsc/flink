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
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;

public class RdmaShuffleClientEndpoint extends RdmaActiveEndpoint {
	private static final Logger LOG = LoggerFactory.getLogger(RdmaShuffleClientEndpoint.class);

	private int bufferSize; // Todo: set default buffer size
	private ByteBuffer receiveBuffer; // Todo: add buffer manager with multiple buffers
	private IbvMr registeredReceiveMemory; // Registered memory for the above buffer

	private ByteBuffer sendBuffer;
	private IbvMr registeredSendMemory;

	private ByteBuffer availableFreeReceiveBuffers;
	private IbvMr availableFreeReceiveBuffersRegisteredMemory;
	private PartitionRequestClientHandler clientHandler;

	private ArrayBlockingQueue<IbvWC> wcEvents = new ArrayBlockingQueue<>(1000);
	private static int workRequestId;

	public RdmaShuffleClientEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> group, RdmaCmId idPriv,
									 boolean serverSide, int bufferSize, PartitionRequestClientHandler clientHandler)
		throws IOException {
		super(group, idPriv, serverSide);
		this.bufferSize = bufferSize; // Todo: validate buffer size
		this.clientHandler = clientHandler;
	}

	@Override
	public void dispatchCqEvent(IbvWC wc) throws IOException {
		synchronized (this) {
			wcEvents.add(wc);
		}
	}

	public void init() throws IOException {
		super.init();
		this.receiveBuffer = ByteBuffer.allocateDirect(bufferSize); // allocate buffer
		this.registeredReceiveMemory = registerMemory(receiveBuffer).execute().getMr(); // register the send buffer

		this.sendBuffer = ByteBuffer.allocateDirect(bufferSize); // allocate buffer
		this.registeredSendMemory = registerMemory(sendBuffer).execute().getMr(); // register the send buffer

		this.availableFreeReceiveBuffers = ByteBuffer.allocateDirect(2); // TODO: assumption of less receive buffers
		this.availableFreeReceiveBuffersRegisteredMemory = registerMemory(availableFreeReceiveBuffers).execute()
			.getMr();
	}

	public ByteBuffer getreceiveBuffer() {
		return this.receiveBuffer;
	}

	public ByteBuffer getSendBuffer() {
		return this.sendBuffer;
	}

	public IbvMr getRegisteredReceiveMemory() {
		return registeredReceiveMemory;
	}

	public IbvMr getRegisteredSendMemory() {
		return registeredSendMemory;
	}

	public ArrayBlockingQueue<IbvWC> getWcEvents() {
		synchronized (this) {
			return wcEvents;
		}
	}

	public ByteBuffer getAvailableFreeReceiveBuffers() {
		return availableFreeReceiveBuffers;
	}

	public IbvMr getAvailableFreeReceiveBuffersRegisteredMemory() {
		return availableFreeReceiveBuffersRegisteredMemory;
	}

	public void write(RdmaMessage msg) {
		// TODO (venkat):imp : pass buffer allocator
		ByteBuf buf = null;
		try {
			msg.writeTo(null);
			sendBuffer.put(buf.nioBuffer());
			RdmaSendReceiveUtil.postSendReq(this, ++workRequestId);
			IbvWC wcSend = this.getWcEvents().take();
			if (wcSend.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
				LOG.error("failed to send the request " + wcSend.getStatus());
				// LOG the failure
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void read() {
		try {
			RdmaSendReceiveUtil.postReceiveReq(this, ++workRequestId);
			IbvWC wc = this.getWcEvents().take();
			if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
				LOG.error("failed to send the request " + wc.getStatus());
				// LOG the failure
			}
			NettyMessage.NettyMessageDecoder decoder = new RdmaMessage.NettyMessageDecoder(false);
			NettyMessage msg = (NettyMessage) decoder.decode(null, Unpooled.wrappedBuffer(this.receiveBuffer));
			clientHandler.decodeMsg(msg, false, this);
		} catch (Throwable e) {
			e.printStackTrace();
		}
		// read the data from QP and decode
//		retur;
	}

	public void terminate() {
		try {
			this.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
