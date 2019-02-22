package com.helijia.idGenerator.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.helijia.idGenerator.redis.ServerNodeListener;

public class ZKDataNode {
	private static final Logger LOG = LoggerFactory.getLogger(ZKDataNode.class);
	private String zkServers;
	private String seqName;
	private CuratorFramework curator;
	private PathChildrenCache pathCache;
	private ZKDataNodeListener listener;

	public ZKDataNode(String zkServers, String seqName) {
		this.zkServers = zkServers;
		this.seqName = seqName;
	}

	public ZKDataNode(CuratorFramework zkClient, String seqName) {
		this.curator = zkClient;
		this.seqName = seqName;
	}

	public void init() {
		try {
			if (this.curator == null) {
				LOG.info("利用{}初始化ZKDataNode", this.zkServers);

				this.curator = CuratorFrameworkFactory.builder().connectString(this.zkServers)
						.retryPolicy(new RetryNTimes(5, 1000)).connectionTimeoutMs(5000).defaultData("".getBytes())
						.build();
				this.curator.start();
			} else {
				LOG.info("zkClient已经初始化 " + this.curator + " - " + this.curator.getState());
			}
			String fullPath = appendCaredPath();
			this.pathCache = new PathChildrenCache(this.curator, "/idgenerator", false);
			this.listener = new ZKDataNodeListener(fullPath);
			this.pathCache.getListenable().addListener(this.listener);
			this.pathCache.start();
			Stat stat = (Stat) this.curator.checkExists().forPath(fullPath);
			if (stat == null) {
				throw new IllegalArgumentException(this.seqName + "不存在");
			}
		} catch (Exception e) {
			if ((this.curator != null) && (this.zkServers != null) && (this.zkServers.length() > 0)) {
				CloseableUtils.closeQuietly(this.curator);
			}
			if (this.pathCache != null) {
				CloseableUtils.closeQuietly(this.pathCache);
			}
			throw new RuntimeException("初始化ZKNodeManager失败", e);
		}
	}

	public void destroy() {
		LOG.info("关闭curator=" + this.curator + " - " + (this.curator != null ? this.curator.getState() : ""));
		if ((this.curator != null) && (this.curator.getState() == CuratorFrameworkState.STARTED)) {
			this.curator.close();
		}
	}

	public String getInitValue() {
		String json = null;
		try {
			String caredPath = appendCaredPath();
			byte[] originalData = (byte[]) this.curator.getData().forPath(caredPath);
			json = new String(originalData, "UTF-8");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return json;
	}

	public String appendCaredPath() {
		StringBuilder caredPathBuilder = new StringBuilder("/idgenerator").append("/").append(this.seqName);
		String caredPath = caredPathBuilder.toString();
		return caredPath.toString();
	}

	public String getSeqName() {
		return this.seqName;
	}

	public void addListener(ServerNodeListener serverNodeListener) {
		this.listener.addServerNodeListener(serverNodeListener);
	}
}
