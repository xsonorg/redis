package org.xson.thirdparty.redis;

public abstract class AbstractClientOperation extends JedisCommandAdapter {

	protected static String	testString	= "hello world";

	protected String		keyEncode	= "UTF-8";

	abstract public void start(JedisConfig jedisConfig) throws Throwable;

	abstract public void stop();

	abstract public boolean testConnection();

	protected String flushDB() {
		return null;
	}

	protected String flushAll() {
		return null;
	}

}
