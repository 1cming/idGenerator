package com.conning.idGenerator;

import java.util.List;

import redis.clients.jedis.JedisPool;

public class ConfigContext {
	private int dbIndex;
	private List<JedisPool> pools;
	private int step;

	public ConfigContext(ConfigContextBuilder builder) {
		this.dbIndex = builder.dbIndex;
		this.pools = builder.pools;
		this.step = builder.step;
	}

	public int getDbIndex() {
		return this.dbIndex;
	}

	public List<JedisPool> getPools() {
		return this.pools;
	}

	public int getStep() {
		return this.step;
	}

	public static class ConfigContextBuilder {
		private int dbIndex;
		private List<JedisPool> pools;
		private int step;

		public ConfigContextBuilder(List<JedisPool> pools, int step) {
			this.pools = pools;
			this.step = step;
		}

		public ConfigContext build() {
			return new ConfigContext(this);
		}

		public ConfigContextBuilder setDbIndex(int dbIndex) {
			this.dbIndex = dbIndex;
			return this;
		}
	}
}
