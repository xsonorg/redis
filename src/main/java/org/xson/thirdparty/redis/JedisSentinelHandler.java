package org.xson.thirdparty.redis;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Pool;

public class JedisSentinelHandler extends AbstractClientOperation {

	protected JedisSentinelHandler() {
	}

	protected Pool<?> pool = null;

	public void start(JedisConfig jedisConfig) throws Throwable {
		pool = new JedisSentinelPool(jedisConfig.masterName, jedisConfig.sentinels, jedisConfig.poolConfig, jedisConfig.connectionTimeout,
				jedisConfig.soTimeout, jedisConfig.password, jedisConfig.database, jedisConfig.clientName);
	}

	public void stop() {
		if (null != pool) {
			pool.close();
		}
	}

	protected void recycle(Object jedis) {
		if (null != jedis) {
			if (jedis instanceof Jedis) {
				((Jedis) jedis).close();
			}
		}
	}

	@Override
	public boolean testConnection() {
		JedisCommands jedis = null;
		boolean ret = false;
		try {
			jedis = (JedisCommands) pool.getResource();
			String echo = jedis.echo(testString);
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
	public String flushAll() {
		Object jedis = (JedisCommands) pool.getResource();
		if (jedis instanceof Jedis) {
			return ((Jedis) jedis).flushAll();
		}
		return null;
	}

	@Override
	public String flushDB() {
		Object jedis = (JedisCommands) pool.getResource();
		if (jedis instanceof Jedis) {
			return ((Jedis) jedis).flushDB();
		}
		return null;
	}

	@Override
	public String set(byte[] key, byte[] value) {
		BinaryJedisCommands jedis = null;
		String ret = null;
		try {
			jedis = (BinaryJedisCommands) pool.getResource();
			ret = jedis.set(key, value);
		} catch (Exception e) {
			try {
				throw new JedisRuntimeException("set operation exception, key[" + new String(key, keyEncode) + "]", e);
			} catch (UnsupportedEncodingException e1) {
				e1.printStackTrace();
			}
		} finally {
			recycle(jedis);
		}
		return ret;
	}

	@Override
	public String set(byte[] key, byte[] value, byte[] nxxx, byte[] expx, long time) {
		BinaryJedisCommands jedis = null;
		String ret = null;
		try {
			jedis = (BinaryJedisCommands) pool.getResource();
			ret = jedis.set(key, value, nxxx, expx, time);
		} catch (Exception e) {
			try {
				throw new JedisRuntimeException("set operation exception, key[" + new String(key, keyEncode) + "]", e);
			} catch (UnsupportedEncodingException e1) {
				e1.printStackTrace();
			}
		} finally {
			recycle(jedis);
		}
		return ret;
	}

	@Override
	public byte[] get(byte[] key) {
		BinaryJedisCommands jedis = null;
		byte[] ret = null;
		try {
			jedis = (BinaryJedisCommands) pool.getResource();
			ret = jedis.get(key);
		} catch (Exception e) {
			try {
				throw new JedisRuntimeException("get operation exception, key[" + new String(key, keyEncode) + "]", e);
			} catch (UnsupportedEncodingException e1) {
				e1.printStackTrace();
			}
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
			jedis = (JedisCommands) pool.getResource();
			ret = jedis.set(key, value);
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
			jedis = (JedisCommands) pool.getResource();
			ret = jedis.set(key, value, nxxx, expx, time);
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
			jedis = (JedisCommands) pool.getResource();
			ret = jedis.get(key);
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
			jedis = (JedisCommands) pool.getResource();
			ret = jedis.del(key);
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
			jedis = (JedisCommands) pool.getResource();
			ret = jedis.hmset(key, hash);
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
			jedis = (JedisCommands) pool.getResource();
			ret = jedis.hdel(key, field);
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
			jedis = (JedisCommands) pool.getResource();
			ret = jedis.hget(key, field);
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
			jedis = (JedisCommands) pool.getResource();
			ret = jedis.hset(key, field, value);
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
			jedis = (JedisCommands) pool.getResource();
			ret = jedis.lpop(key);
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
			jedis = (JedisCommands) pool.getResource();
			ret = jedis.rpop(key);
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
			jedis = (JedisCommands) pool.getResource();
			ret = jedis.lpush(key, string);
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
			jedis = (JedisCommands) pool.getResource();
			ret = jedis.rpush(key, string);
		} catch (Exception e) {
			throw new JedisRuntimeException("rpush operation exception, key[" + key + "]", e);
		} finally {
			recycle(jedis);
		}
		return ret;
	}

}