package com.conning.idGenerator.redis;

public class ServerNode {
	private String host;
	private int port;

	public ServerNode(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public String getHost() {
		return this.host;
	}

	public int getPort() {
		return this.port;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(this.host).append(":").append(this.port);
		return builder.toString();
	}
}
