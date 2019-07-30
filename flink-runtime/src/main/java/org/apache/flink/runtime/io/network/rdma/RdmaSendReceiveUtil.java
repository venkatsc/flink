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
import com.ibm.disni.verbs.IbvWC;

import java.io.IOException;
import java.util.LinkedList;

public class RdmaSendReceiveUtil {

	public static void postSendReq(RdmaActiveEndpoint endpoint, int workReqId) throws IOException {

		if (endpoint instanceof RdmaShuffleServerEndpoint) {
			RdmaShuffleServerEndpoint clientEndpoint = (RdmaShuffleServerEndpoint) endpoint;
			System.out.println("posting wr_id " + workReqId);
			LinkedList<IbvSge> sges = new LinkedList<IbvSge>();
			IbvSge sendSGE = new IbvSge();
			sendSGE.setAddr(clientEndpoint.getRegisteredSendMemory().getAddr());
			sendSGE.setLength(clientEndpoint.getRegisteredSendMemory().getLength());
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
			System.out.println("posting wr_id " + workReqId);
			LinkedList<IbvSge> sges = new LinkedList<IbvSge>();
			IbvSge sendSGE = new IbvSge();
			sendSGE.setAddr(clientEndpoint.getRegisteredSendMemory().getAddr());
			sendSGE.setLength(clientEndpoint.getRegisteredSendMemory().getLength());
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

	public static void postReceiveReq(RdmaActiveEndpoint endpoint, int workReqId) throws IOException {

		if (endpoint instanceof RdmaShuffleServerEndpoint) {
			System.out.println("posting wr_id " + workReqId);
			RdmaShuffleServerEndpoint clientEndpoint = (RdmaShuffleServerEndpoint) endpoint;
			LinkedList<IbvSge> sges = new LinkedList<IbvSge>();
			IbvSge recvSGE = new IbvSge();
			recvSGE.setAddr(clientEndpoint.getRegisteredReceiveMemory().getAddr());
			recvSGE.setLength(clientEndpoint.getRegisteredReceiveMemory().getLength());
			recvSGE.setLkey(clientEndpoint.getRegisteredReceiveMemory().getLkey());
			sges.add(recvSGE);

			IbvRecvWR recvWR = new IbvRecvWR();
			recvWR.setWr_id(workReqId);
			recvWR.setSg_list(sges);

			LinkedList<IbvRecvWR> recvWRs = new LinkedList<>();
			recvWRs.add(recvWR);
			endpoint.postRecv(recvWRs).execute().free();
		} else if (endpoint instanceof RdmaShuffleClientEndpoint) {
			System.out.println("posting wr_id " + workReqId);
			RdmaShuffleClientEndpoint clientEndpoint = (RdmaShuffleClientEndpoint) endpoint;
			LinkedList<IbvSge> sges = new LinkedList<IbvSge>();
			IbvSge recvSGE = new IbvSge();
			recvSGE.setAddr(clientEndpoint.getRegisteredReceiveMemory().getAddr());
			recvSGE.setLength(clientEndpoint.getRegisteredReceiveMemory().getLength());
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

	public static void repostOnFailure(IbvWC wc1, RdmaShuffleServerEndpoint endpoint, int workRequestId) throws
		IOException, InterruptedException {
		while (wc1.getStatus() != 0) {
			if (wc1.getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_SEND.ordinal()) { // send failure
				System.out.println("Failed to post send. Reposting");
				RdmaSendReceiveUtil.postSendReq(endpoint, ++workRequestId);
				wc1 = endpoint.getWcEvents().take();
			} else if (wc1.getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_RECV.ordinal()) {
				System.out.println("opcode wc " + wc1.getOpcode());
				System.out.println("Failed to post receive");
				System.out.println("Error vendor: " + wc1.getVendor_err() + " err: " + wc1.getErr());

				RdmaSendReceiveUtil.postReceiveReq(endpoint, ++workRequestId);
				wc1 = endpoint.getWcEvents().take();
			} else {
				System.out.println("Failed request and unintended opcode " + wc1.getOpcode());
			}
		}
	}

	public static void repostOnFailure(IbvWC wc1, RdmaShuffleClientEndpoint endpoint, int workRequestId) throws
		IOException, InterruptedException {
		while (wc1.getStatus() != 0) {
			if (wc1.getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_SEND.ordinal()) { // send failure
				System.out.println("Failed to post send. Reposting");
				RdmaSendReceiveUtil.postSendReq(endpoint, ++workRequestId);
				wc1 = endpoint.getWcEvents().take();
			} else if (wc1.getOpcode() == IbvWC.IbvWcOpcode.IBV_WC_RECV.ordinal()) {
				System.out.println("opcode wc " + wc1.getOpcode());
				System.out.println("Failed to post receive");
				System.out.println("Error vendor: " + wc1.getVendor_err() + " err: " + wc1.getErr());

				RdmaSendReceiveUtil.postReceiveReq(endpoint, ++workRequestId);
				wc1 = endpoint.getWcEvents().take();
			} else {
				System.out.println("Failed request and unintended opcode " + wc1.getOpcode());
			}
		}
	}
}
