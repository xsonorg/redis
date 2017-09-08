package org.xson.thirdparty.redis;

import java.util.Map;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;

public class JedisClusterHandler extends AbstractClientOperation {

	protected JedisClusterHandler() {
	}

	protected JedisCluster pool = null;

	public void start(JedisConfig jedisConfig) throws Throwable {
		pool = new JedisCluster(jedisConfig.clusters, jedisConfig.clusterConnectionTimeout, jedisConfig.clusterSoTimeout,
				jedisConfig.clusterMaxRedirections, jedisConfig.poolConfig);
	}

	public void stop() {
		if (null != pool) {
			pool.close();
		}
	}

	protected void recycle(Object jedis) {
		// TODO
	}

	@Override
	public boolean testConnection() {
		JedisCommands jedis = null;
		boolean ret = false;
		try {
			String echo = pool.echo(testString);
			if (null != echo && testString.equalsIgnoreCase(echo)) {
				ret = true;
			}
		} catch (Exception e) {
			throw new JedisRuntimeException("test connection error.", e);
		} finally {
			recycle(jedis);
		}
		return ret;
	}

	@Override
	public String set(String key, String value) {
		JedisCommands jedis = null;
		String ret = null;
		try {
			ret = pool.set(key, value);
		} catch (Exception e) {
			throw new JedisRuntimeException("set operation exception, key[" + key + "], value[" + value + "]", e);
		} finally {
			recycle(jedis);
		}
		return ret;
	}

	@Override
	public String set(String key, String value, String nxxx, String expx, long time) {
		JedisCommands jedis = null;
		String ret = null;
		try {
			ret = pool.set(key, value, nxxx, expx, time);
		} catch (Exception e) {
			throw new JedisRuntimeException("set operation exception, key[" + key + "], value[" + value + "]", e);
		} finally {
			recycle(jedis);
		}
		return ret;
	}

	@Override
	public String get(String key) {
		JedisCommands jedis = null;
		String ret = null;
		try {
			ret = pool.get(key);
		} catch (Exception e) {
			throw new JedisRuntimeException("get operation exception, key[" + key + "]", e);
		} finally {
			recycle(jedis);
		}
		return ret;
	}

	@Override
	public Long del(String key) {
		JedisCommands jedis = null;
		Long ret = null;
		try {
			ret = pool.del(key);
		} catch (Exception e) {
			throw new JedisRuntimeException("del operation exception, key[" + key + "]", e);
		} finally {
			recycle(jedis);
		}
		return ret;
	}

	public String hmset(String key, Map<String, String> hash) {
		JedisCommands jedis = null;
		String ret = null;
		try {
			ret = pool.hmset(key, hash);
		} catch (Exception e) {
			throw new JedisRuntimeException("hmset operation exception, key[" + key + "]", e);
		} finally {
			recycle(jedis);
		}
		return ret;
	}

	public Long hdel(String key, String... field) {
		JedisCommands jedis = null;
		Long ret = null;
		try {
			ret = pool.hdel(key, field);
		} catch (Exception e) {
			throw new JedisRuntimeException("hdel operation exception, key[" + key + "]", e);
		} finally {
			recycle(jedis);
		}
		return ret;
	}

	public String hget(String key, String field) {
		JedisCommands jedis = null;
		String ret = null;
		try {
			ret = pool.hget(key, field);
		} catch (Exception e) {
			throw new JedisRuntimeException("hget operation exception, key[" + key + "]", e);
		} finally {
			recycle(jedis);
		}
		return ret;
	}

	public Long hget(String key, String field, String value) {
		JedisCommands jedis = null;
		Long ret = null;
		try {
			ret = pool.hset(key, field, value);
		} catch (Exception e) {
			throw new JedisRuntimeException("hget operation exception, key[" + key + "]", e);
		} finally {
			recycle(jedis);
		}
		return ret;
	}

	public String lpop(String key) {
		JedisCommands jedis = null;
		String ret = null;
		try {
			ret = pool.lpop(key);
		} catch (Exception e) {
			throw new JedisRuntimeException("lpop operation exception, key[" + key + "]", e);
		} finally {
			recycle(jedis);
		}
		return ret;
	}

	public String rpop(String key) {
		JedisCommands jedis = null;
		String ret = null;
		try {
			ret = pool.rpop(key);
		} catch (Exception e) {
			throw new JedisRuntimeException("rpop operation exception, key[" + key + "]", e);
		} finally {
			recycle(jedis);
		}
		return ret;
	}

	public Long lpush(String key, String... string) {
		JedisCommands jedis = null;
		Long ret = null;
		try {
			ret = pool.lpush(key, string);
		} catch (Exception e) {
			throw new JedisRuntimeException("lpush operation exception, key[" + key + "]", e);
		} finally {
			recycle(jedis);
		}
		return ret;
	}

	public Long rpush(String key, String... string) {
		JedisCommands jedis = null;
		Long ret = null;
		try {
			ret = pool.rpush(key, string);
		} catch (Exception e) {
			throw new JedisRuntimeException("rpush operation exception, key[" + key + "]", e);
		} finally {
			recycle(jedis);
		}
		return ret;
	}
}
