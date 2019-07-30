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

public class RdmaShuffleServerEndpoint extends RdmaActiveEndpoint {
	private static final Logger LOG = LoggerFactory.getLogger(RdmaShuffleServerEndpoint.class);
	private int workRequestId = 0;
	private int bufferSize; // Todo: set default buffer size
	private ByteBuffer sendBuffer; // Todo: add buffer manager with multiple buffers
	private IbvMr registeredSendMemory; // Registered memory for the above buffer

	private ByteBuffer availableFreeReceiveBuffers;
	private IbvMr availableFreeReceiveBuffersRegisteredMemory;

	private ByteBuffer availableFreeReceiveBuffersNotification;
	private IbvMr availableFreeReceiveBuffersNotificationRegisteredMemory;

	private ByteBuffer receiveBuffer; // Todo: add buffer manager with multiple buffers
	private IbvMr registeredReceiveMemory;

	private PartitionRequestServerHandler requestServerHandler;

	private ArrayBlockingQueue<IbvWC> wcEvents = new ArrayBlockingQueue<>(1000);

	public RdmaShuffleServerEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> group, RdmaCmId idPriv,
									 boolean serverSide, int bufferSize, PartitionRequestServerHandler
										 requestServerHandler) throws IOException {
		super(group, idPriv, serverSide);
		this.bufferSize = bufferSize; // Todo: validate buffer size
		this.requestServerHandler = requestServerHandler;
	}

	@Override
	public void dispatchCqEvent(IbvWC wc) throws IOException {
		synchronized (this) {
			wcEvents.add(wc);
		}
	}

	public void init() throws IOException {
		super.init();
		this.sendBuffer = ByteBuffer.allocateDirect(bufferSize); // allocate buffer
		this.receiveBuffer = ByteBuffer.allocateDirect(bufferSize);
		this.registeredReceiveMemory = registerMemory(receiveBuffer).execute().getMr();
		this.registeredSendMemory = registerMemory(sendBuffer).execute().getMr(); // register the send buffer
		this.availableFreeReceiveBuffers = ByteBuffer.allocateDirect(2); // TODO: assumption of less receive buffers
		this.availableFreeReceiveBuffersRegisteredMemory = registerMemory(availableFreeReceiveBuffers).execute()
			.getMr();
		this.availableFreeReceiveBuffersNotification = ByteBuffer.allocateDirect(2); // TODO: assumption of less
		// receive buffers
		this.availableFreeReceiveBuffersNotificationRegisteredMemory = registerMemory
			(availableFreeReceiveBuffersNotification).execute().getMr();
	}

	public ByteBuffer getSendBuffer() {
		return this.sendBuffer;
	}

	public IbvMr getRegisteredSendMemory() {
		return registeredSendMemory;
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

	public ByteBuffer getAvailableFreeReceiveBuffers() {
		return availableFreeReceiveBuffers;
	}

	public IbvMr getAvailableFreeReceiveBuffersRegisteredMemory() {
		return availableFreeReceiveBuffersRegisteredMemory;
	}

	public ByteBuffer getAvailableFreeReceiveBuffersNotification() {
		return availableFreeReceiveBuffersNotification;
	}

	public IbvMr getAvailableFreeReceiveBuffersNotificationRegisteredMemory() {
		return availableFreeReceiveBuffersNotificationRegisteredMemory;
	}

	// TODO (venkat):imp : implement below methods
	public void write(NettyMessage msg) {
		// TODO (venkat):imp : pass buffer allocator
		ByteBuf buf = null;
		try {
			buf = msg.write(null);
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
			NettyMessage.NettyMessageDecoder decoder = new NettyMessage.NettyMessageDecoder(false);
			NettyMessage msg = (NettyMessage) decoder.decode(null, Unpooled.wrappedBuffer(this.getReceiveBuffer()));
			requestServerHandler.channelRead(this, msg);
		} catch (Exception e) {
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
