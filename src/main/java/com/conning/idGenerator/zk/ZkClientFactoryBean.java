package com.conning.idGenerator.zk;

import java.util.concurrent.CountDownLatch;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

public class ZkClientFactoryBean implements FactoryBean<CuratorFramework>, InitializingBean, DisposableBean {
	private static Logger LOG = LoggerFactory.getLogger(ZkClientFactoryBean.class);
	private String connectString;
	private int retryNum = 3;
	private int sleepMsBetweenRetries = 1000;
	private int connectionTimeoutMs = 5000;
	private CuratorFramework zkClient;

	public void afterPropertiesSet() throws Exception {
		LOG.info("connectString -> " + this.connectString + " connectionTimeoutMs ->" + this.connectionTimeoutMs);
		final CountDownLatch connectedLatch = new CountDownLatch(1);

		this.zkClient = CuratorFrameworkFactory.builder().connectString(this.connectString)
				.retryPolicy(new RetryNTimes(this.retryNum, this.sleepMsBetweenRetries))
				.connectionTimeoutMs(this.connectionTimeoutMs).defaultData("".getBytes()).build();
		this.zkClient.getCuratorListenable().addListener(new CuratorListener() {
			public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
				if ((connectedLatch.getCount() > 0L) && (event.getWatchedEvent() != null)
						&& (event.getWatchedEvent().getState() == Watcher.Event.KeeperState.SyncConnected)) {
					ZkClientFactoryBean.LOG.info(" eventReceived started ");
					connectedLatch.countDown();
				}
			}
		});
		this.zkClient.getUnhandledErrorListenable().addListener(new UnhandledErrorListener() {
			public void unhandledError(String message, Throwable e) {
				e.printStackTrace();
				ZkClientFactoryBean.LOG.error(message, e);
			}
		});
		this.zkClient.start();
		LOG.info("start connectedLatch.await() ");
		connectedLatch.await();
		LOG.info("end connectedLatch.await() -> " + this.zkClient.getState());
	}

	public void destroy() throws Exception {
		this.zkClient.close();
	}

	public CuratorFramework getObject() throws Exception {
		return this.zkClient;
	}

	public Class<?> getObjectType() {
		return CuratorFramework.class;
	}

	public boolean isSingleton() {
		return true;
	}

	public String getConnectString() {
		return this.connectString;
	}

	public void setConnectString(String connectString) {
		this.connectString = connectString;
	}

	public int getRetryNum() {
		return this.retryNum;
	}

	public void setRetryNum(int retryNum) {
		this.retryNum = retryNum;
	}

	public int getSleepMsBetweenRetries() {
		return this.sleepMsBetweenRetries;
	}

	public void setSleepMsBetweenRetries(int sleepMsBetweenRetries) {
		this.sleepMsBetweenRetries = sleepMsBetweenRetries;
	}

	public int getConnectionTimeoutMs() {
		return this.connectionTimeoutMs;
	}

	public void setConnectionTimeoutMs(int connectionTimeoutMs) {
		this.connectionTimeoutMs = connectionTimeoutMs;
	}
}
