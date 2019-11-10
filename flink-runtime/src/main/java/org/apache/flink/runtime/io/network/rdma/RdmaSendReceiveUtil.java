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
import com.ibm.disni.verbs.IbvRecvWR;
import com.ibm.disni.verbs.IbvSendWR;
import com.ibm.disni.verbs.IbvSge;
import com.ibm.disni.verbs.SVCPostRecv;
import com.ibm.disni.verbs.SVCPostSend;
import com.ibm.disni.verbs.StatefulVerbCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.buffer.ReadOnlySlicedNetworkBuffer;

public class RdmaSendReceiveUtil {
	private static final Logger LOG = LoggerFactory.getLogger(RdmaSendReceiveUtil.class);

	public static void postSendReqForBufferResponse(RdmaActiveEndpoint endpoint, long workReqId, NettyMessage
		.BufferResponse response) throws IOException {

		if (endpoint instanceof RdmaShuffleServerEndpoint) {
			RdmaShuffleServerEndpoint clientEndpoint = (RdmaShuffleServerEndpoint) endpoint;
//			LOG.info("posting server send wr_id " + workReqId+ " against src: " + endpoint.getSrcAddr() + " dest: "
// +endpoint.getDstAddr());
			LinkedList<IbvSge> sges = new LinkedList<IbvSge>();

//			int magic = response.getHeaderBuf().readInt();
//			if (magic !=NettyMessage.MAGIC_NUMBER){
//				LOG.error("Server sending wrong magic number and it should never happen. Magic: {}",magic);
//			}
			ByteBuf buf = response.getBuffer();
			MemorySegment segment;
			long dataAddress;
			int dataLen;
			if (buf instanceof NetworkBuffer) {
				segment = ((NetworkBuffer) buf).getMemorySegment();
				dataAddress = buf.memoryAddress();
				dataLen = buf.writerIndex() - buf.readerIndex();
			} else if (buf instanceof ReadOnlySlicedNetworkBuffer) {
				segment = ((ReadOnlySlicedNetworkBuffer) buf).getMemorySegment();
				dataAddress = segment.getAddress() + ((ReadOnlySlicedNetworkBuffer) buf).getMemorySegmentOffset();
				dataLen = buf.writerIndex() - buf.readerIndex();
			} else {
				throw new IOException("Received unidentified network buffer type");
			}
			// header is at the end of segment
			int start = response.headerBufPosition * RdmaConnectionManager.DATA_MSG_HEADER_SIZE;
			long address = ((DirectBuffer) response.getHeaderBuffer()).address() + start;
//			LOG.info("SRUtil: Header start address {}, seq: {} on connection {}", address, response.getHeaderBuffer()
//				.getInt(start + 25),clientEndpoint.getEndpointStr());
// segment.getAddress() + start,
//				segment.getAddress() + segment.size(),buf.writerIndex(),segment.getIntBigEndian(start+4),buf.order());

			IbvSge headerSGE = new IbvSge();
			headerSGE.setAddr(address);
			headerSGE.setLength(RdmaConnectionManager.DATA_MSG_HEADER_SIZE);
			headerSGE.setLkey(NetworkBufferPool.getHeaderMR().getLkey());
			sges.add(headerSGE);
			// actual data
			IbvSge dataSGE = new IbvSge();
			dataSGE.setAddr(dataAddress);
			dataSGE.setLength(dataLen);
//			dataSGE.setAddr(segment.getAddress());
//			dataSGE.setLength(segment.size());
			dataSGE.setLkey(clientEndpoint.getRegisteredMRs().get(segment.getAddress()).getLkey());
			sges.add(dataSGE);

//			IbvSge sendSGE1 = new IbvSge();
//			if (response.getTempBuffer()!=null){ // the buffer is not direct buffer and hence rewritten
//				sendSGE1.setAddr(response.getTempBuffer().memoryAddress());
//				sendSGE1.setLength(response.getTempBuffer().readableBytes());
//				sendSGE1.setLkey(clientEndpoint.getWholeMR().getLkey());
//				sges.add(sendSGE1);
////				ByteBuf buf1 =response.getTempBuffer();
////				LOG.info("sending temp buffer readable:{} capacity:{}",buf1.readableBytes(),buf1.capacity());
//			}else{
//				sendSGE1.setAddr(buffer.getMemorySegment().getAddress() + buffer.getMemorySegmentOffset());
//				sendSGE1.setLength(buffer.writerIndex());
//				sendSGE1.setLkey(clientEndpoint.getRegisteredMRs().get(buffer.getMemorySegment().getAddress() )
// .getLkey());
//				sges.add(sendSGE1);
////				LOG.info("sending network buffer readable:{} capacity:{} writer index: {} getsize: {}",buffer
// .readableBytes(),buffer.capacity(),buffer.writerIndex(),buffer.getSize());
//			}

			// Create send Work Request (WR)
			IbvSendWR sendWR = new IbvSendWR();
			sendWR.setWr_id(workReqId);
			sendWR.setSg_list(sges);
//			if (finish){
//				sendWR.setOpcode(IbvSendWR.IBV_WR_SEND_WITH_IMM);
//			}else {
			sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
//			}
			sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);

			LinkedList<IbvSendWR> sendWRs = new LinkedList<>();
			sendWRs.add(sendWR);
			clientEndpoint.postSend(sendWRs).execute().free();
//			synchronized (inFlightVerbs) {
//				inFlightVerbs.put(workReqId, sendVerb);
//			}
		}
	}

	public static void postSendReq(RdmaActiveEndpoint endpoint, long workReqId) throws IOException {

		if (endpoint instanceof RdmaShuffleServerEndpoint) {
			RdmaShuffleServerEndpoint clientEndpoint = (RdmaShuffleServerEndpoint) endpoint;
//			LOG.info("posting server send wr_id " + workReqId+ " against src: " + endpoint.getSrcAddr() + " dest: "
// +endpoint.getDstAddr());
			LinkedList<IbvSge> sges = new LinkedList<IbvSge>();
			IbvSge sendSGE = new IbvSge();
			sendSGE.setAddr(((DirectBuffer) clientEndpoint.getSendBuffer()).address());
			sendSGE.setLength(clientEndpoint.getSendBuffer().capacity());
			sendSGE.setLkey(clientEndpoint.getRegisteredSendMemory().getLkey());
			sges.add(sendSGE);

			// Create send Work Request (WR)
			IbvSendWR sendWR = new IbvSendWR();
			sendWR.setWr_id(workReqId);
			sendWR.setSg_list(sges);
//			if (finish){
//				sendWR.setOpcode(IbvSendWR.IBV_WR_SEND_WITH_IMM);
//			}else {
			sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
//			}
			sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);

			LinkedList<IbvSendWR> sendWRs = new LinkedList<>();
			sendWRs.add(sendWR);
			clientEndpoint.postSend(sendWRs).execute().free();
		} else if (endpoint instanceof RdmaShuffleClientEndpoint) {
			RdmaShuffleClientEndpoint clientEndpoint = (RdmaShuffleClientEndpoint) endpoint;
//			LOG.info("posting client send wr_id " + workReqId+ " against src: " + endpoint.getSrcAddr() + " dest: "
// +endpoint.getDstAddr());
			LinkedList<IbvSge> sges = new LinkedList<IbvSge>();
			IbvSge sendSGE = new IbvSge();
			sendSGE.setAddr(((DirectBuffer) clientEndpoint.getSendBuffer()).address());
			sendSGE.setLength(clientEndpoint.getSendBuffer().capacity());
			sendSGE.setLkey(clientEndpoint.getRegisteredSendMemory().getLkey());
			sges.add(sendSGE);
			// Create send Work Request (WR)
			IbvSendWR sendWR = new IbvSendWR();
			sendWR.setWr_id(workReqId);
			sendWR.setSg_list(sges);
//			if (finish) {
//				sendWR.setOpcode(IbvSendWR.IBV_WR_SEND_WITH_IMM);
//			} else {
			sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
//			}
			sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);

			LinkedList<IbvSendWR> sendWRs = new LinkedList<>();
			sendWRs.add(sendWR);
			clientEndpoint.postSend(sendWRs).execute().free();
		}
	}

	public static void postReceiveReq(RdmaActiveEndpoint endpoint, long workReqId) throws IOException {

		if (endpoint instanceof RdmaShuffleServerEndpoint) {
//			LOG.info("posting server receive wr_id " + workReqId + " against src: " + endpoint.getSrcAddr() + " dest:
// " +endpoint.getDstAddr());
			RdmaShuffleServerEndpoint clientEndpoint = (RdmaShuffleServerEndpoint) endpoint;
			LinkedList<IbvSge> sges = new LinkedList<IbvSge>();
			IbvSge recvSGE = new IbvSge();
			recvSGE.setAddr(((DirectBuffer) clientEndpoint.getReceiveBuffer()).address());
			recvSGE.setLength(clientEndpoint.getReceiveBuffer().capacity());
			recvSGE.setLkey(clientEndpoint.getRegisteredReceiveMemory().getLkey());
			sges.add(recvSGE);

			IbvRecvWR recvWR = new IbvRecvWR();
			recvWR.setWr_id(workReqId);
			recvWR.setSg_list(sges);

			LinkedList<IbvRecvWR> recvWRs = new LinkedList<>();
			recvWRs.add(recvWR);
			endpoint.postRecv(recvWRs).execute().free();
		} else if (endpoint instanceof RdmaShuffleClientEndpoint) {
//			LOG.info("posting client receive wr_id " + workReqId + " against src: " + endpoint.getSrcAddr() + " dest:
// " +endpoint.getDstAddr());
			RdmaShuffleClientEndpoint clientEndpoint = (RdmaShuffleClientEndpoint) endpoint;
			LinkedList<IbvSge> sges = new LinkedList<IbvSge>();
			IbvSge recvSGE = new IbvSge();
			recvSGE.setAddr(((DirectBuffer) clientEndpoint.getReceiveBuffer()).address());
			recvSGE.setLength(clientEndpoint.getReceiveBuffer().capacity());
			recvSGE.setLkey(clientEndpoint.getRegisteredReceiveMemory().getLkey());
			sges.add(recvSGE);

			IbvRecvWR recvWR = new IbvRecvWR();
			recvWR.setWr_id(workReqId);
			recvWR.setSg_list(sges);

			LinkedList<IbvRecvWR> recvWRs = new LinkedList<>();
			recvWRs.add(recvWR);
			endpoint.postRecv(recvWRs).execute().free();
		}
	}

	public static void postReceiveReqWithChannelBuf(RdmaActiveEndpoint endpoint, long workReqId, ByteBuf buffer)
		throws IOException {

		if (endpoint instanceof RdmaShuffleClientEndpoint) {
//			LOG.info("posting client receive wr_id " + workReqId + " against src: " + endpoint.getSrcAddr() + " dest:
// " +endpoint.getDstAddr());
			RdmaShuffleClientEndpoint clientEndpoint = (RdmaShuffleClientEndpoint) endpoint;
			LinkedList<IbvSge> sges = new LinkedList<IbvSge>();
			IbvSge recvSGE = new IbvSge();
			recvSGE.setAddr(buffer.memoryAddress());
			recvSGE.setLength(buffer.capacity());
			recvSGE.setLkey(clientEndpoint.getRegisteredMRs().get(buffer.memoryAddress()).getLkey());
			sges.add(recvSGE);

			IbvRecvWR recvWR = new IbvRecvWR();
			recvWR.setWr_id(workReqId);
			recvWR.setSg_list(sges);

			LinkedList<IbvRecvWR> recvWRs = new LinkedList<>();
			recvWRs.add(recvWR);
			endpoint.postRecv(recvWRs).execute().free();
			}
	}

//	public static void repostOnFailure(IbvWC wc1, RdmaShuffleServerEndpoint endpoint, int workRequestId) throws
//		IOException, InterruptedException {
//		while (wc1.getStatus() != 0) {
//			if (wc1.getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_SEND.ordinal()) { // send failure
//				System.out.println("Failed to post send. Reposting");
//				RdmaSendReceiveUtil.postSendReq(endpoint, ++workRequestId);
//				wc1 = endpoint.getWcEvents().take();
//			} else if (wc1.getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_RECV.ordinal()) {
//				System.out.println("opcode wc " + wc1.getOpcode());
//				System.out.println("Failed to post receive");
//				System.out.println("Error vendor: " + wc1.getVendor_err() + " err: " + wc1.getErr());
//
//				RdmaSendReceiveUtil.postReceiveReq(endpoint, ++workRequestId);
//				wc1 = endpoint.getWcEvents().take();
//			} else {
//				System.out.println("Failed request and unintended opcode " + wc1.getOpcode());
//			}
//		}
//	}
//
//	public static void repostOnFailure(IbvWC wc1, RdmaShuffleClientEndpoint endpoint, int workRequestId) throws
//		IOException, InterruptedException {
//		while (wc1.getStatus() != 0) {
//			if (wc1.getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_SEND.ordinal()) { // send failure
//				System.out.println("Failed to post send. Reposting");
//				RdmaSendReceiveUtil.postSendReq(endpoint, ++workRequestId);
//				wc1 = endpoint.getWcEvents().take();
//			} else if (wc1.getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_RECV.ordinal()) {
//				System.out.println("opcode wc " + wc1.getOpcode());
//				System.out.println("Failed to post receive");
//				System.out.println("Error vendor: " + wc1.getVendor_err() + " err: " + wc1.getErr());
//
//				RdmaSendReceiveUtil.postReceiveReq(endpoint, ++workRequestId);
//				wc1 = endpoint.getWcEvents().take();
//			} else {
//				System.out.println("Failed request and unintended opcode " + wc1.getOpcode());
//			}
//		}
//	}
}
