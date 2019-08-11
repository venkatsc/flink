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

	public ByteBuffer getReceiveBuffer() {
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

	public void terminate() {
		try {
			this.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public RdmaMessage writeAndRead(RdmaMessage msg){
		RdmaMessage response= null;
		try {
			msg.writeTo(this.getSendBuffer());
			RdmaSendReceiveUtil.postSendReq(this, ++workRequestId);
			for (int i=0;i<2;i++) {
				IbvWC wc = this.getWcEvents().take();
				if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_RECV) {
					if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
						System.out.println("Receive posting failed. reposting new receive request");
						RdmaSendReceiveUtil.postReceiveReq(this, ++workRequestId);
					} else { // first receive succeeded. Read the data and repost the next message
						byte ID =this.getReceiveBuffer().get();
						switch(ID){
						    case RdmaMessage.BufferResponse.ID:
								response = RdmaMessage.BufferResponse.readFrom(this.getReceiveBuffer());
								break;
							default: LOG.error(" Un-identified request type "+ID);
						}
						this.getReceiveBuffer().clear();
						RdmaSendReceiveUtil.postReceiveReq(this, ++workRequestId);
					}
				} else if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_SEND) {
					if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
						System.out.println("Send failed. reposting new send request request");
						RdmaSendReceiveUtil.postSendReq(this, ++workRequestId);
					}
					this.getSendBuffer().clear();
					// Send succeed does not require any action
				} else {
					System.out.println("failed to match any condition " + wc.getOpcode());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return response;
	}
}
