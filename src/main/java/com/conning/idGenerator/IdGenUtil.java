package com.conning.idGenerator;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

public class IdGenUtil implements DisposableBean {
	
	private static Logger LOG = LoggerFactory.getLogger(IdGenUtil.class);
	
	private CuratorFramework zkClient;
	private String prefix = "";
	private String suffix = ".id";
	private final Map<String, IDGenerator> idGeneratorMap = new HashMap<>();
	private final ReadWriteLock lock = new ReentrantReadWriteLock(false);

	public long next(Class<?> clazz) {
		return next(clazz.getSimpleName());
	}

	public long next(String name) {
		String seqName = this.prefix + name + this.suffix;

		IDGenerator idGenerator = null;
		this.lock.readLock().lock();
		try {
			idGenerator = (IDGenerator) this.idGeneratorMap.get(seqName);
		} finally {
			this.lock.readLock().unlock();
		}
		if (idGenerator == null) {
			this.lock.writeLock().lock();
			try {
				idGenerator = (IDGenerator) this.idGeneratorMap.get(seqName);
				if (idGenerator == null) {
					LOG.info("开始初始化ID -> seqName=" + seqName + " ,zkClient=" + this.zkClient + " - "
							+ (this.zkClient != null ? this.zkClient.getState() : ""));
					idGenerator = new IDGenerator();
					idGenerator.setZkClient(this.zkClient);
					idGenerator.setSeqName(seqName);
					idGenerator.init();
					this.idGeneratorMap.put(seqName, idGenerator);
					LOG.info("成功初始化ID -> seqName=" + seqName);
				}
			} finally {
				this.lock.writeLock().unlock();
			}
		}
		return idGenerator.next();
	}

	public String nextAsStr(Class<?> clazz) {
		return Long.toString(next(clazz));
	}

	public String nextAsStr(String name) {
		return Long.toString(next(name));
	}

	public void destroy() throws Exception {
		for (Map.Entry<String, IDGenerator> entry : this.idGeneratorMap.entrySet()) {
			try {
				LOG.info("关闭seqName=" + (String) entry.getKey());
				((IDGenerator) entry.getValue()).destroy();
			} catch (Exception localException) {
			}
		}
	}

	public void setZkClient(CuratorFramework zkc) {
		this.zkClient = zkc;
	}

	public void setPrefix(String p) {
		this.prefix = p;
	}

	public void setSuffix(String s) {
		this.suffix = s;
	}
}