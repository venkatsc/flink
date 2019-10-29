package org.apache.flink.runtime.io.network.rdma.exception;

import java.io.IOException;

public class OutOfCredit extends IOException {
	public OutOfCredit(String message) {
		super(message,null);
	}
}
