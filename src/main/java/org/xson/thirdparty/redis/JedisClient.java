package org.xson.thirdparty.redis;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.xson.thirdparty.redis.JedisConfig.JedisMode;

public class JedisClient {

	private AbstractClientOperation	clientOperation	= null;

	private volatile boolean		running			= false;

	public void start(Properties properties) throws Throwable {
		if (running) {
			return;
		}
		try {
			running = true;
			JedisConfig jedisConfig = JedisConfig.create(properties);
			if (jedisConfig.mode == JedisMode.BASIC || jedisConfig.mode == JedisMode.SHARDED) {
				clientOperation = new JedisDefaultHandler();
			} else if (jedisConfig.mode == JedisMode.SENTINEL) {
				clientOperation = new JedisSentinelHandler();
			} else if (jedisConfig.mode == JedisMode.CLUSTER) {
				clientOperation = new JedisClusterHandler();
			}
			clientOperation.start(jedisConfig);
		} catch (Throwable e) {
			// log.error("JedisClient Failed to start");
			stop();
			throw e;
		}
	}

	public void stop() {
		if (running && null != clientOperation) {
			running = false;
			clientOperation.stop();
			clientOperation = null;
		}
	}

	public String set(byte[] key, byte[] value) throws JedisRuntimeException {
		return clientOperation.set(key, value);
	}

	public String set(byte[] key, byte[] value, byte[] nxxx, byte[] expx, long time) throws JedisRuntimeException {
		return clientOperation.set(key, value, nxxx, expx, time);
	}

	public byte[] get(byte[] key) throws JedisRuntimeException {
		return clientOperation.get(key);
	}

	public String set(String key, String value) throws JedisRuntimeException {
		return clientOperation.set(key, value);
	}

	public String set(String key, String value, String nxxx, String expx, long time) throws JedisRuntimeException {
		return clientOperation.set(key, value, nxxx, expx, time);
	}

	public String get(String key) throws JedisRuntimeException {
		return clientOperation.get(key);
	}

	public Long del(String key) throws JedisRuntimeException {
		return clientOperation.del(key);
	}

	public String hmset(String key, Map<String, String> hash) throws JedisRuntimeException {
		return clientOperation.hmset(key, hash);
	}

	public Long hdel(String key, String... field) throws JedisRuntimeException {
		return clientOperation.hdel(key, field);
	}

	public String hget(String key, String field) throws JedisRuntimeException {
		return clientOperation.hget(key, field);
	}

	public Long hset(String key, String field, String value) throws JedisRuntimeException {
		return clientOperation.hset(key, field, value);
	}

	public List<String> hvals(String key) throws JedisRuntimeException {
		return clientOperation.hvals(key);
	}

	public String lpop(String key) throws JedisRuntimeException {
		return clientOperation.lpop(key);
	}

	public String rpop(String key) throws JedisRuntimeException {
		return clientOperation.rpop(key);
	}

	public Long lpush(String key, String... string) throws JedisRuntimeException {
		return clientOperation.lpush(key, string);
	}

	public Long rpush(String key, String... string) throws JedisRuntimeException {
		return clientOperation.rpush(key, string);
	}

}
