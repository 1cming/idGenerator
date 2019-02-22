package com.conning.idGenerator.redis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.conning.idGenerator.ConfigContext;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class ServerNodeManager implements ServerNodeListener {
	private static final Logger LOG = LoggerFactory.getLogger(ServerNodeManager.class);
	private static final Logger LOGToFile = LoggerFactory.getLogger("conning.idgenerator.redisValueLog");
	private String seqName;
	private volatile ConfigContext context;
	private static final int RETRY = 3;

	public ServerNodeManager(String seqName) {
		this.seqName = seqName;
	}

	public long incr() {
		long max = 0L;
		for (int i = 0; i < 3; i++) {
			try {
				List<JedisPool> pools = getShuffledNodesPool();
				Iterator<JedisPool> localIterator = pools.iterator();
				for (;;) {
					JedisPool pool = null;
					Jedis jedis = null;
					if (localIterator.hasNext()) {
						pool = (JedisPool) localIterator.next();
						jedis = null;
					}
					try {
						jedis = pool.getResource();
						if (StringUtils.equals(jedis.get(this.seqName), "-1")) {
							Thread.sleep(50L);
							if (jedis != null) {
								jedis.close();
							}
						} else {
							max = jedis.incrBy(this.seqName, this.context.getStep() * pools.size()).longValue();

							LOG.info("{}:{} 序列{}增加至{}",
									new Object[] { jedis.getClient().getHost(),
											Integer.valueOf(jedis.getClient().getPort()), this.seqName,
											Long.valueOf(max) });
							LOGToFile.info("{}:{} 序列{}增加至{}",
									new Object[] { jedis.getClient().getHost(),
											Integer.valueOf(jedis.getClient().getPort()), this.seqName,
											Long.valueOf(max) });
							if (jedis != null) {
								jedis.close();
							}
						}
					} catch (JedisConnectionException localJedisConnectionException) {
						if (jedis != null) {
							jedis.close();
						}
					} finally {
						if (jedis != null) {
							jedis.close();
						}
					}
				}
			} catch (Exception e) {
			}
		}
		return max;
	}

	private List<JedisPool> getShuffledNodesPool() {
		List<JedisPool> poolList = new ArrayList<>();
		poolList.addAll(this.context.getPools());
		Collections.shuffle(poolList);
		return poolList;
	}

	public void init(String json) {
		int dbIndex = parseDbIndex(json);

		int step = parseStep(json);
		List<ServerNode> serverNodes = parseServerNodes(json);

		List<JedisPool> pools = initPools(serverNodes, dbIndex);

		reconstructWorld(pools, dbIndex, step);
	}

	private int parseDbIndex(String json) {
		LOG.info("parse json {}", json);
		JSONObject jsonObj = JSON.parseObject(json);
		int dbIndex = jsonObj.getIntValue("dbIndex");
		return dbIndex;
	}

	private int parseStep(String json) {
		LOG.info("parse json {}", json);
		JSONObject jsonObj = JSON.parseObject(json);
		int step = jsonObj.getIntValue("step");
		return step;
	}

	private List<ServerNode> parseServerNodes(String json) {
		LOG.info("parse json {}", json);
		JSONObject jsonObj = JSON.parseObject(json);
		JSONArray jsonArr = JSON.parseArray(jsonObj.getString("servers"));
		List<ServerNode> serverNodes = new ArrayList<>(jsonArr.size());
		for (int i = 0; i < jsonArr.size(); i++) {
			String ipAndPort = jsonArr.getString(i);
			ServerNode serverNode = null;
			if (StringUtils.contains(ipAndPort, ":")) {
				String[] strArr = StringUtils.split(ipAndPort, ":");
				serverNode = new ServerNode(strArr[0], Integer.parseInt(strArr[1]));
			} else {
				serverNode = new ServerNode(ipAndPort, 6379);
			}
			if (serverNode != null) {
				serverNodes.add(serverNode);
			}
			LOG.info(serverNode.toString());
		}
		return serverNodes;
	}

	private List<JedisPool> initPools(List<ServerNode> serverNodes, int dbIndex) {
		if ((serverNodes == null) || (serverNodes.isEmpty())) {
			return null;
		}
		LOG.info("初始化redis pools");
		List<JedisPool> pools = new ArrayList<>(serverNodes.size());
		for (ServerNode serverNode : serverNodes) {
			JedisPool pool = new JedisPool(getPoolConfig(), serverNode.getHost(), serverNode.getPort(), 2000, null,
					dbIndex);
			pools.add(pool);
		}
		return pools;
	}

	private void reconstructWorld(List<JedisPool> tempPool, int dbIndex, int step) {
		this.context = new ConfigContext.ConfigContextBuilder(tempPool, step).setDbIndex(dbIndex).build();
	}

	public void eventOccured(ServerNodeListener.ServerNodeEventType event, String nodeData) {
		if (StringUtils.isBlank(nodeData)) {
			return;
		}
		switch (event) {
		case UPDATE:
			init(nodeData);
			break;
		case REMOVE:
			destroy();
			break;
		}
	}

	private void destroy() {
		LOG.info("destorying context");

		this.context = null;
	}

	public int getStep() {
		return this.context.getStep();
	}

	public GenericObjectPoolConfig getPoolConfig() {
		GenericObjectPoolConfig config = new GenericObjectPoolConfig();

		config.setTestOnBorrow(false);
		config.setTestOnReturn(false);
		config.setTestWhileIdle(true);

		config.setMinIdle(8);

		config.setMaxWaitMillis(10L);

		return config;
	}
}
