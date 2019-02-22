package com.helijia.idGenerator;

import org.apache.curator.framework.CuratorFramework;

import com.helijia.idGenerator.redis.ServerNodeManager;
import com.helijia.idGenerator.zk.ZKDataNode;

public class Bootstrap {

	private ZKDataNode zkDataNode;
	private ServerNodeManager serverNodeManger;

	public void start(String zkServers, String seqName) {
		this.zkDataNode = new ZKDataNode(zkServers, seqName);
		this.zkDataNode.init();
		this.serverNodeManger = new ServerNodeManager(seqName);
		this.serverNodeManger.init(this.zkDataNode.getInitValue());
		this.zkDataNode.addListener(this.serverNodeManger);
	}

	public void start(CuratorFramework zkClient, String seqName) {
		this.zkDataNode = new ZKDataNode(zkClient, seqName);
		this.zkDataNode.init();
		this.serverNodeManger = new ServerNodeManager(seqName);
		this.serverNodeManger.init(this.zkDataNode.getInitValue());
		this.zkDataNode.addListener(this.serverNodeManger);
	}

	public void shutdown() {
		this.zkDataNode.destroy();
	}

	public long incr() {
		return this.serverNodeManger.incr();
	}

	public int getOffset() {
		return this.serverNodeManger.getStep();
	}
}
