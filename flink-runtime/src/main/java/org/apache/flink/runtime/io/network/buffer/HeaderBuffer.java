package org.apache.flink.runtime.io.network.buffer;

public class HeaderBuffer {
	private int startIndex;
	public HeaderBuffer(int startIndex){
		this.startIndex = startIndex;
	}
}
