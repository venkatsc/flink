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

import org.apache.flink.runtime.io.network.netty.NettyBufferPool;

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
	private boolean isClosed = false;

	private ArrayBlockingQueue<IbvWC> wcEvents = new ArrayBlockingQueue<>(1000);
	private static int workRequestId;
	private NettyBufferPool bufferPool;

	public RdmaShuffleClientEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> group, RdmaCmId idPriv,
									 boolean serverSide, int bufferSize, PartitionRequestClientHandler clientHandler,
									 NettyBufferPool bufferPool)
		throws IOException {
		super(group, idPriv, serverSide);
		this.bufferSize = bufferSize; // Todo: validate buffer size
		this.clientHandler = clientHandler;
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

	public NettyMessage writeAndRead(NettyMessage msg){
		NettyMessage response= null;
		try {
			ByteBuf buf = msg.write(bufferPool);
			int takeEventsCount;
			if (msg instanceof NettyMessage.CloseRequest){
				takeEventsCount=1; // Don't block for response
			}else{
				takeEventsCount=2;
			}
			this.getSendBuffer().put(buf.nioBuffer());
			RdmaSendReceiveUtil.postSendReq(this, ++workRequestId);
			for (int i=0;i<takeEventsCount;i++) {
				IbvWC wc = this.getWcEvents().take();
				LOG.info("Took completion event {} ",i);
				if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_RECV) {
					if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
						LOG.error("Receive posting failed. reposting new receive request");
						RdmaSendReceiveUtil.postReceiveReq(this, ++workRequestId);
					} else { // first receive succeeded. Read the data and repost the next message
						this.getReceiveBuffer().getInt(); // discard frame length
						this.getReceiveBuffer().getInt(); // discard magic number
						byte ID =this.getReceiveBuffer().get();
						switch(ID){
							case NettyMessage.BufferResponse.ID:
								response = NettyMessage.BufferResponse.readFrom(Unpooled.wrappedBuffer(this.receiveBuffer));
//								LOG.error(" Response received with seq.number: "+ ((NettyMessage.BufferResponse) response).sequenceNumber);
								break;
							default: LOG.error(" Un-identified request type "+ID);
						}
						this.getReceiveBuffer().clear();
						RdmaSendReceiveUtil.postReceiveReq(this, ++workRequestId);
					}
				} else if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_SEND) {
					if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
						LOG.error("Send failed. reposting new send request request");
						RdmaSendReceiveUtil.postSendReq(this, ++workRequestId);
					}
					this.getSendBuffer().clear();
					// Send succeed does not require any action
				} else {
					LOG.error("failed to match any condition " + wc.getOpcode());
				}
			}
		} catch (Exception e) {
			try {
				LOG.error("failed client read "+getEndpointStr(this), e);
			} catch (Exception e1) {
				LOG.error("failed get endpoint", e);
			}
//			throw new IOException(e);
		}
		return response;
	}

	private String getEndpointStr(RdmaShuffleClientEndpoint clientEndpoint) throws  Exception{
		return  "src: " + clientEndpoint.getSrcAddr() + " dst: " +
			clientEndpoint.getDstAddr();
	}

	public ByteBuffer getAvailableFreeReceiveBuffers() {
		return availableFreeReceiveBuffers;
	}

	public IbvMr getAvailableFreeReceiveBuffersRegisteredMemory() {
		return availableFreeReceiveBuffersRegisteredMemory;
	}

	public void terminate() {
		try {
			isClosed = true;
			this.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean isClosed() {
		return isClosed;
	}
}
