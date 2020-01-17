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
import sun.nio.ch.DirectBuffer;

import scala.Byte;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.apache.flink.runtime.io.network.netty.NettyBufferPool;

public class RdmaShuffleClientEndpoint extends RdmaActiveEndpoint {
	private static final Logger LOG = LoggerFactory.getLogger(RdmaShuffleClientEndpoint.class);
	public static int inFlightSendRequests = 5000;
	private ArrayBlockingQueue<ByteBuffer> freeSendBuffer = new ArrayBlockingQueue<>(inFlightSendRequests);

	private int bufferSize; // Todo: set default buffer size
//	private ByteBuffer receiveBuffer; // Todo: add buffer manager with multiple buffers
//	private IbvMr registeredReceiveMemory; // Registered memory for the above buffer

	public Map<Long, IbvMr> registeredSendMrs = new HashMap<Long, IbvMr>();

	// should be made public
	public final Map<Long,ByteBuf> receivedBuffers = new ConcurrentHashMap<>();
	public Map<Long,ByteBuf> inFlight = new ConcurrentHashMap<Long,ByteBuf>();
	private Map<Long,ByteBuffer> inFlightSendBufs = new ConcurrentHashMap<>();

	public void addToInFlightSend(long id, ByteBuffer sendBuffer){
		inFlightSendBufs.put(id,sendBuffer);
	}

	public ByteBuffer removeAndGetFromInFlightSend(long id){
		return inFlightSendBufs.remove(id);
	}

	private ByteBuffer sendBuffer;
	private IbvMr registeredSendMemory;

//	private ByteBuffer availableFreeReceiveBuffers;
//	private IbvMr availableFreeReceiveBuffersRegisteredMemory;
	private PartitionRequestClientHandler clientHandler;
	private boolean isClosed = false;

	private Map<Long,IbvMr> registeredMRs;

	private BlockingQueue<IbvWC> wcEvents = new LinkedBlockingQueue<>();
	AtomicLong workRequestId = new AtomicLong(1);
	private NettyBufferPool bufferPool;


	public Map<Long, IbvMr> getRegisteredMRs() {
		return registeredMRs;
	}

	public void setRegisteredMRs(Map<Long, IbvMr> registeredMRs) {
		this.registeredMRs = registeredMRs;
	}
//	private IbvMr wholeMR;

//	private class LastEvent {
//		private IbvWC lastEvent;
//		public IbvWC get(){
//			return lastEvent;
//		}
//
//		public void set(IbvWC wc){
//			lastEvent=wc;
//		}
//	}

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
//			int newOpCode = wc.getOpcode();
//			IbvWC old = lastEvent.get();
//			if(old == null){
//				lastEvent.set(wc.clone());
//			} else if (old.getOpcode() == newOpCode){
//				throw new RuntimeException("*******client got "+ IbvWC.IbvWcOpcode.valueOf(newOpCode) +" event twice in a row. last id = "+old.getWr_id()+", current id "+old.getWr_id()+"***********");
//			}
//			lastEvent.set(wc.clone());
		try {
			wcEvents.put(RdmaSendReceiveUtil.cloneWC(wc));
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	public void init() throws IOException {
		super.init();
		LOG.info("Allocating client rdma buffers");
//		this.receiveBuffer = ByteBuffer.allocateDirect(bufferSize); // allocate buffer
//		this.registeredReceiveMemory = registerMemory(receiveBuffer).execute().getMr(); // register the send buffer
		for (int i = 0; i < inFlightSendRequests; i++) {
			ByteBuffer sendBuffer = ByteBuffer.allocateDirect(100);
			freeSendBuffer.add(sendBuffer);
			this.registeredSendMrs.put(((DirectBuffer) sendBuffer).address(), registerMemory(sendBuffer).execute().getMr());
		}
//		this.sendBuffer = ByteBuffer.allocateDirect(bufferSize); // allocate buffer
//		this.registeredSendMemory = registerMemory(sendBuffer).execute().getMr(); // register the send buffer
//		this.wholeMR = registerMemory().execute().getMr();

//		LOG.info("Client rkey: %d lkey: %d handle:%d\n",wholeMR.getRkey(),wholeMR.getLkey(),wholeMR.getHandle());
//		this.availableFreeReceiveBuffers = ByteBuffer.allocateDirect(2); // TODO: assumption of less receive buffers
//		this.availableFreeReceiveBuffersRegisteredMemory = registerMemory(availableFreeReceiveBuffers).execute()
//			.getMr();
	}

//	public IbvMr getWholeMR(){
//		return wholeMR;
//	}
//	public ByteBuffer getReceiveBuffer() {
//		return this.receiveBuffer;
//	}

	public ByteBuffer getSendBuffer() throws InterruptedException {
		return this.freeSendBuffer.take();
	}

	public void recycleSendBuffer(ByteBuffer buffer){
		this.freeSendBuffer.add(buffer);
	}

//	public IbvMr getRegisteredReceiveMemory() {
//		return registeredReceiveMemory;
//	}

//	public IbvMr getRegisteredSendMemory() {
//		return registeredSendMemory;
//	}

	public BlockingQueue<IbvWC> getWcEvents() {
			return wcEvents;
	}

	public NettyBufferPool getNettyBufferpool(){
		return bufferPool;
	}

//	public NettyMessage writeAndRead(NettyMessage msg){
//		NettyMessage response= null;
//		try {
//			ByteBuf buf = msg.write(bufferPool);
//			int takeEventsCount;
//			if (msg instanceof NettyMessage.CloseRequest){
//				takeEventsCount=1; // Don't block for response
//			}else{
//				takeEventsCount=2;
//			}
//			this.getSendBuffer().put(buf.nioBuffer());
//			RdmaSendReceiveUtil.postSendReq(this, ++workRequestId);
//			for (int i=0;i<takeEventsCount;i++) {
//				IbvWC wc = this.getWcEvents().take();
//				LOG.info("Took completion event {} ",i);
//				if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_RECV) {
//					if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
//						LOG.error("Receive posting failed. reposting new receive request");
////						RdmaSendReceiveUtil.postReceiveReq(this, ++workRequestId);
//					} else { // first receive succeeded. Read the data and repost the next message
//						this.getReceiveBuffer().getInt(); // discard frame length
//						this.getReceiveBuffer().getInt(); // discard magic number
//						byte ID =this.getReceiveBuffer().get();
//						switch(ID){
//							case NettyMessage.BufferResponse.ID:
//								response = NettyMessage.BufferResponse.readFrom(Unpooled.wrappedBuffer(this.receiveBuffer));
////								LOG.error(" Response received with seq.number: "+ ((NettyMessage.BufferResponse) response).sequenceNumber);
//								break;
//							default: LOG.error(" Un-identified request type "+ID);
//						}
//						this.getReceiveBuffer().clear();
//						RdmaSendReceiveUtil.postReceiveReq(this, ++workRequestId);
//					}
//				} else if (IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) == IbvWC.IbvWcOpcode.IBV_WC_SEND) {
//					if (wc.getStatus() != IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()) {
//						LOG.error("RdmaShuffle: Send failed. reposting new send request request");
//						RdmaSendReceiveUtil.postSendReq(this, ++workRequestId);
//					}
//					this.getSendBuffer().clear();
//					// Send succeed does not require any action
//				} else {
//					LOG.error("failed to match any condition " + wc.getOpcode());
//				}
//			}
//		} catch (Exception e) {
//			try {
//				LOG.error("failed client read "+getEndpointStr(), e);
//			} catch (Exception e1) {
//				LOG.error("failed get endpoint", e);
//			}
////			throw new IOException(e);
//		}
//		return response;
//	}

	public String getEndpointStr() {
		try {
			return "src: " + this.getSrcAddr() + " dst: " +
				this.getDstAddr();
		}catch (Exception e){
			LOG.error("Failed to get the address on client endpoint");
		}
		return "";
	}

//	public ByteBuffer getAvailableFreeReceiveBuffers() {
//		return availableFreeReceiveBuffers;
//	}
//
//	public IbvMr getAvailableFreeReceiveBuffersRegisteredMemory() {
//		return availableFreeReceiveBuffersRegisteredMemory;
//	}

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
	public String toString(){
	 return this.getEndpointStr();
	}

	@Override
	public boolean isClosed() {
		return isClosed;
	}
}
