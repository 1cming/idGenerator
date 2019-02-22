package com.conning.idGenerator;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;

public final class IDGenerator {
	private String zkServers;
	private String seqName;
	private volatile long currentOffset;
	private volatile long maxOffset;
	private Bootstrap bootstrap;
	private CuratorFramework zkClient;

	public IDGenerator(String zkServers, String seqName) {
		this.zkServers = zkServers;
		this.seqName = seqName;
	}

	public IDGenerator() {
	}

	public void init() {
		if (StringUtils.isBlank(this.seqName)) {
			throw new IllegalArgumentException("缺少seqName");
		}
		if (this.zkClient == null) {
			if (StringUtils.isBlank(this.zkServers)) {
				throw new IllegalArgumentException("缺少zookeeper服务地址");
			}
			this.bootstrap = new Bootstrap();
			this.bootstrap.start(this.zkServers, this.seqName);
		} else {
			this.bootstrap = new Bootstrap();
			this.bootstrap.start(this.zkClient, this.seqName);
		}
	}

	public void destroy() {
		if (this.bootstrap != null) {
			this.bootstrap.shutdown();
		}
	}

	public synchronized long next() {
		if (this.currentOffset == this.maxOffset) {
			this.maxOffset = generateMaxOffset();
			this.currentOffset = (this.maxOffset - this.bootstrap.getOffset());
		}
		this.currentOffset += 1L;
		return this.currentOffset;
	}

	private long generateMaxOffset() {
		return this.bootstrap.incr();
	}

	public String getSeqName() {
		return this.seqName;
	}

	public void setSeqName(String seqName) {
		this.seqName = seqName;
	}

	public String getZkServers() {
		return this.zkServers;
	}

	public void setZkServers(String zkServers) {
		this.zkServers = zkServers;
	}

	public CuratorFramework getZkClient() {
		return this.zkClient;
	}

	public void setZkClient(CuratorFramework zkClient) {
		this.zkClient = zkClient;
	}
}