package com.conning.idGenerator.zk;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.conning.idGenerator.redis.ServerNodeListener;

public class ZKDataNodeListener implements PathChildrenCacheListener {
	private static final Logger LOG = LoggerFactory.getLogger(ZKDataNodeListener.class);
	private String caredPath;
	private ServerNodeListener listener;

	public ZKDataNodeListener(String caredPath) {
		this.caredPath = caredPath;
	}

	public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
		PathChildrenCacheEvent.Type eventType = event.getType();
		ChildData obj = event.getData();
		if (obj == null) {
			LOG.info("时间{}发生childData空值", eventType);
			return;
		}
		String path = obj.getPath();
		if (!StringUtils.equals(path, this.caredPath)) {
			LOG.info("don`t care this event: {} occured at {}", eventType, path);
			return;
		}
		byte[] dataArr = null;
		String data = null;
		switch (eventType) {
		case CHILD_UPDATED:
			dataArr = (byte[]) client.getData().forPath(path);
			data = dataArr == null ? "" : new String(dataArr);
			this.listener.eventOccured(ServerNodeListener.ServerNodeEventType.UPDATE, data);
			break;
		case CHILD_REMOVED:
			dataArr = (byte[]) client.getData().forPath(path);
			data = dataArr == null ? "" : new String(dataArr);
			this.listener.eventOccured(ServerNodeListener.ServerNodeEventType.REMOVE, data);
			break;
		default:
			LOG.info("{} occured at {} with content {}", new Object[] { eventType, path, data });
		}
	}

	public void addServerNodeListener(ServerNodeListener listener) {
		this.listener = listener;
	}
}