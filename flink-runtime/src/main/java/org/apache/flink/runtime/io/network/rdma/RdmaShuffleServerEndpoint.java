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
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class RdmaShuffleServerEndpoint extends RdmaActiveEndpoint {
	private static final Logger LOG = LoggerFactory.getLogger(RdmaShuffleServerEndpoint.class);
	private int bufferSize =100; // Todo: set default buffer size
	private ByteBuffer sendBuffer; // Todo: add buffer manager with multiple buffers
//	private IbvMr wholeMR;

	private Map<Long,IbvMr> registeredMRs;

	private IbvMr registeredSendMemory; // Registered memory for the above buffer

	private ByteBuffer receiveBuffer; // Todo: add buffer manager with multiple buffers
	private IbvMr registeredReceiveMemory;


	public Map<Long, IbvMr> getRegisteredMRs() {
		return registeredMRs;
	}

	public void setRegisteredMRs(Map<Long, IbvMr> registeredMRs) {
		this.registeredMRs = registeredMRs;
	}

	public IbvMr getRegisteredSendMemory() {
		return registeredSendMemory;
	}

	public IbvMr getRegisteredReceiveMemory() {
		return registeredReceiveMemory;
	}
	AtomicLong workRequestId = new AtomicLong(1);

	RdmaServerRequestHandler.HandleClientConnection handlerClientConnection;

	public void setConectionHandler(RdmaServerRequestHandler.HandleClientConnection connectionHandler) {
		handlerClientConnection = connectionHandler;
	}

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

	private BlockingQueue<IbvWC> wcEvents = new LinkedBlockingQueue<>();

	public RdmaShuffleServerEndpoint(RdmaActiveEndpointGroup<? extends RdmaActiveEndpoint> group, RdmaCmId idPriv,
									 boolean serverSide, int bufferSize) throws IOException {
		super(group, idPriv, serverSide);
		this.bufferSize = bufferSize; // Todo: validate buffer size
	}

	@Override
	public void dispatchCqEvent(IbvWC wc) throws IOException {
			handlerClientConnection.handleWC(wc);
		}
	public void init() throws IOException {
		super.init();
		LOG.info("Allocating server rdma buffers");
		this.sendBuffer = ByteBuffer.allocateDirect(bufferSize); // allocate buffer
		this.receiveBuffer = ByteBuffer.allocateDirect(bufferSize);
//		this.wholeMR = registerMemory().execute().getMr();
//		LOG.info("server rkey: %d lkey: %d handle:%d\n",wholeMR.getRkey(),wholeMR.getLkey(),wholeMR.getHandle());
		this.registeredReceiveMemory = registerMemory(receiveBuffer).execute().getMr();
		this.registeredSendMemory = registerMemory(sendBuffer).execute().getMr(); // register the send buffer
//		this.availableFreeReceiveBuffers = ByteBuffer.allocateDirect(2); // TODO: assumption of less receive buffers
//		this.availableFreeReceiveBuffersRegisteredMemory = registerMemory(availableFreeReceiveBuffers).execute()
//			.getMr();
//		this.availableFreeReceiveBuffersNotification = ByteBuffer.allocateDirect(2); // TODO: assumption of less
		// receive buffers
//		this.availableFreeReceiveBuffersNotificationRegisteredMemory = registerMemory
//			(availableFreeReceiveBuffersNotification).execute().getMr();
	}

	public ByteBuffer getSendBuffer() {
		return this.sendBuffer;
	}

//	public IbvMr getWholeMR(){
//		return wholeMR;
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
	public ByteBuffer getReceiveBuffer() {
		return this.receiveBuffer;
	}

	public BlockingQueue<IbvWC> getWcEvents() {
			return wcEvents;
	}

	@Override
	public String toString(){
		return this.getEndpointStr();
	}

  public void stop() {
		try {
			LOG.info("Server endpoint closed. src: "+ this.getSrcAddr() + " dst: " +this.getDstAddr());
			this.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
