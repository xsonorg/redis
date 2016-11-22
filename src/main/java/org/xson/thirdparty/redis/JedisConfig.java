package org.xson.thirdparty.redis;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Protocol;
import redis.clients.util.Hashing;
import redis.clients.util.Sharded;

public class JedisConfig {

	protected JedisMode		mode							= JedisMode.SHARDED;

	JedisPoolConfig			poolConfig						= null;

	int						maxTotal						= GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
	int						maxIdle							= GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
	int						minIdle							= GenericObjectPoolConfig.DEFAULT_MIN_IDLE;

	boolean					testOnCreate					= GenericObjectPoolConfig.DEFAULT_TEST_ON_CREATE;
	boolean					testOnBorrow					= GenericObjectPoolConfig.DEFAULT_TEST_ON_BORROW;
	boolean					testOnReturn					= GenericObjectPoolConfig.DEFAULT_TEST_ON_RETURN;
	// idle状态监测用异步线程evict进行检查
	boolean					testWhileIdle					= GenericObjectPoolConfig.DEFAULT_TEST_WHILE_IDLE;

	// 当池内没有返回对象时，最大等待时间
	long					maxWaitMillis					= GenericObjectPoolConfig.DEFAULT_MAX_WAIT_MILLIS;
	long					minEvictableIdleTimeMillis		= GenericObjectPoolConfig.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS;
	// test idle 线程的时间间隔
	long					timeBetweenEvictionRunsMillis	= GenericObjectPoolConfig.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS;
	// 一次最多evict的pool里的jedis实例个数
	int						numTestsPerEvictionRun			= GenericObjectPoolConfig.DEFAULT_NUM_TESTS_PER_EVICTION_RUN;

	List<JedisShardInfo>	shardList						= null;

	Hashing					hashing							= Hashing.MURMUR_HASH;

	String					host;
	int						port							= 6379;
	int						connectionTimeout				= Protocol.DEFAULT_TIMEOUT;
	int						soTimeout						= Protocol.DEFAULT_TIMEOUT;
	String					password						= null;
	int						database						= Protocol.DEFAULT_DATABASE;
	String					clientName						= null;

	Set<String>				sentinels						= null;
	String					masterName						= null;

	Set<HostAndPort>		clusters						= null;
	int						clusterConnectionTimeout		= 2000;
	int						clusterSoTimeout				= 2000;
	int						clusterMaxRedirections			= 5;

	public enum JedisMode {
		// 基本模式
		BASIC,
		// 分片模式
		SHARDED,
		// 有监控的M-S
		SENTINEL,
		// 集群
		CLUSTER
	}

	public static JedisConfig create(Properties properties) throws Exception {
		JedisConfig config = new JedisConfig();
		parsePoolConfig(properties, config);
		config.mode = JedisMode.valueOf(properties.getProperty("JedisMode"));// Y
		if (config.mode == JedisMode.BASIC) {
			parseBasicConfig(properties, config);
		} else if (config.mode == JedisMode.SHARDED) {
			parseShardedConfig(properties, config);
		} else if (config.mode == JedisMode.SENTINEL) {
			parseSentinelConfig(properties, config);
		} else if (config.mode == JedisMode.CLUSTER) {
			parseClusterConfig(properties, config);
		}
		return config;
	}

	private static void parsePoolConfig(Properties properties, JedisConfig config) {
		// 连接池参数
		String value = properties.getProperty("Jedis.pool.maxTotal");
		if (null != value && value.trim().length() > 0) {
			config.maxTotal = Integer.parseInt(value);
		}

		value = properties.getProperty("Jedis.pool.maxIdle");
		if (null != value && value.trim().length() > 0) {
			config.maxIdle = Integer.parseInt(value);
		}

		value = properties.getProperty("Jedis.pool.minIdle");
		if (null != value && value.trim().length() > 0) {
			config.minIdle = Integer.parseInt(value);
		}

		value = properties.getProperty("Jedis.pool.testOnCreate");
		if (null != value && value.trim().length() > 0) {
			config.testOnCreate = Boolean.parseBoolean(value);
		}

		value = properties.getProperty("Jedis.pool.testOnBorrow");
		if (null != value && value.trim().length() > 0) {
			config.testOnBorrow = Boolean.parseBoolean(value);
		}

		value = properties.getProperty("Jedis.pool.testOnReturn");
		if (null != value && value.trim().length() > 0) {
			config.testOnReturn = Boolean.parseBoolean(value);
		}

		value = properties.getProperty("Jedis.pool.testWhileIdle");
		if (null != value && value.trim().length() > 0) {
			config.testWhileIdle = Boolean.parseBoolean(value);
		}

		value = properties.getProperty("Jedis.pool.maxWaitMillis");
		if (null != value && value.trim().length() > 0) {
			config.maxWaitMillis = Integer.parseInt(value);
		}

		value = properties.getProperty("Jedis.pool.minEvictableIdleTimeMillis");
		if (null != value && value.trim().length() > 0) {
			config.minEvictableIdleTimeMillis = Integer.parseInt(value);
		}

		value = properties.getProperty("Jedis.pool.timeBetweenEvictionRunsMillis");
		if (null != value && value.trim().length() > 0) {
			config.timeBetweenEvictionRunsMillis = Integer.parseInt(value);
		}

		value = properties.getProperty("Jedis.pool.numTestsPerEvictionRun");
		if (null != value && value.trim().length() > 0) {
			config.numTestsPerEvictionRun = Integer.parseInt(value);
		}

		JedisPoolConfig poolConfig = new JedisPoolConfig();

		poolConfig.setMaxTotal(config.maxTotal);
		poolConfig.setMaxIdle(config.maxIdle);
		poolConfig.setMinIdle(config.minIdle);
		poolConfig.setTestOnBorrow(config.testOnBorrow);
		poolConfig.setTestOnReturn(config.testOnReturn);

		poolConfig.setMaxWaitMillis(config.maxWaitMillis);
		poolConfig.setTestWhileIdle(config.testWhileIdle);
		poolConfig.setMinEvictableIdleTimeMillis(config.minEvictableIdleTimeMillis);
		poolConfig.setTimeBetweenEvictionRunsMillis(config.timeBetweenEvictionRunsMillis);
		poolConfig.setNumTestsPerEvictionRun(config.numTestsPerEvictionRun);

		config.poolConfig = poolConfig;
	}

	private static void parseBasicConfig(Properties properties, JedisConfig config) throws Exception {
		String value = properties.getProperty("Jedis.host");// Y
		if (null != value && value.trim().length() > 0) {
			config.host = value.trim();
		} else {
			throw new IllegalArgumentException("Invalid Jedis.host");
		}

		value = properties.getProperty("Jedis.port");// N
		if (null != value && value.trim().length() > 0) {
			config.port = Integer.parseInt(value);
		}

		value = properties.getProperty("Jedis.connectionTimeout");// N
		if (null != value && value.trim().length() > 0) {
			config.connectionTimeout = Integer.parseInt(value);
		}

		value = properties.getProperty("Jedis.soTimeout");// N
		if (null != value && value.trim().length() > 0) {
			config.soTimeout = Integer.parseInt(value);
		}

		value = properties.getProperty("Jedis.password");// N
		if (null != value && value.trim().length() > 0) {
			config.password = value.trim();
		}

		value = properties.getProperty("Jedis.database");// N
		if (null != value && value.trim().length() > 0) {
			config.database = Integer.parseInt(value);
		}

		value = properties.getProperty("Jedis.clientName");// N
		if (null != value && value.trim().length() > 0) {
			config.clientName = value;
		}
	}

	private static void parseShardedConfig(Properties properties, JedisConfig config) throws Exception {
		String value = properties.getProperty("shard.hash");
		if (null != value && value.trim().length() > 0) {
			// TODO
		}

		int shardCount = 0;
		value = properties.getProperty("shard.count");// Y
		if (null == value || value.length() == 0) {
			throw new IllegalArgumentException("Invalid shard.count");
		}
		shardCount = Integer.parseInt(value);
		// config.shardList = new LinkedList<JedisShardInfo>();
		config.shardList = new ArrayList<JedisShardInfo>();

		for (int i = 1; i <= shardCount; i++) {
			int connectionTimeout = 2000;
			int soTimeout = 2000;
			String host = null;
			int port = Protocol.DEFAULT_PORT;
			String password = null;
			String name = null;
			// int db = 0;
			int weight = Sharded.DEFAULT_WEIGHT;
			value = properties.getProperty("shard" + i + ".host");// Y
			if (null == value || value.length() == 0) {
				throw new IllegalArgumentException("Invalid shardX.host");
			}
			host = value;
			value = properties.getProperty("shard" + i + ".port");
			if (null != value && value.trim().length() > 0) {
				port = Integer.parseInt(value);
			}
			value = properties.getProperty("shard" + i + ".weight");
			if (null != value && value.trim().length() > 0) {
				weight = Integer.parseInt(value);
			}
			value = properties.getProperty("shard" + i + ".password");
			if (null != value && value.trim().length() > 0) {
				password = value.trim();
			}
			value = properties.getProperty("shard" + i + ".name");
			if (null != value && value.trim().length() > 0) {
				name = value.trim();
			}
			value = properties.getProperty("shard" + i + ".timeout");
			if (null != value && value.trim().length() > 0) {
				connectionTimeout = Integer.parseInt(value);
				soTimeout = connectionTimeout;
			}
			JedisShardInfo shardInfo = new JedisShardInfo(host, name, port, soTimeout, weight);
			if (null != password) {
				shardInfo.setPassword(password);
			}
			config.shardList.add(shardInfo);
		}
	}

	private static void parseSentinelConfig(Properties properties, JedisConfig config) throws Exception {
		String value = properties.getProperty("Jedis.masterName");
		if (null != value && value.trim().length() > 0) {
			config.masterName = value.trim();
		}

		value = properties.getProperty("Jedis.sentinels"); // Y
		if (null != value && value.trim().length() > 0) {
			String[] tmp = value.trim().split(",");
			config.sentinels = new HashSet<String>();
			for (int j = 0; j < tmp.length; j++) {
				config.sentinels.add(tmp[j].trim());
			}
		} else {
			throw new IllegalArgumentException("Invalid Jedis.sentinels");
		}
	}

	private static void parseClusterConfig(Properties properties, JedisConfig config) throws Exception {
		String value = properties.getProperty("Jedis.cluster.connectionTimeout");
		if (null != value && value.trim().length() > 0) {
			config.clusterConnectionTimeout = Integer.parseInt(value);
		}

		value = properties.getProperty("Jedis.cluster.soTimeout");
		if (null != value && value.trim().length() > 0) {
			config.clusterSoTimeout = Integer.parseInt(value);
		}

		value = properties.getProperty("Jedis.cluster.maxRedirections");
		if (null != value && value.trim().length() > 0) {
			config.clusterMaxRedirections = Integer.parseInt(value);
		}

		value = properties.getProperty("Jedis.clusters"); // Y
		if (null != value && value.trim().length() > 0) {
			String[] tmp = value.trim().split(",");
			config.clusters = new HashSet<HostAndPort>();
			for (int j = 0; j < tmp.length; j++) {
				String[] hapTmp = tmp[j].trim().split(":");
				config.clusters.add(new HostAndPort(hapTmp[0].trim(), Integer.parseInt(hapTmp[1].trim())));
			}
		} else {
			throw new IllegalArgumentException("Invalid Jedis.clusters");
		}
	}

	public static void main(String[] args) {
		System.out.println("cluster".toUpperCase());
	}
}
