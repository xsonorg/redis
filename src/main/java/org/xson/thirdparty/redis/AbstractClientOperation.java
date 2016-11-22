package org.xson.thirdparty.redis;

public abstract class AbstractClientOperation extends JedisCommandAdapter {

	abstract public void start(JedisConfig jedisConfig) throws Throwable;

	abstract public void stop();

	abstract public boolean testConnection();

	protected static String	testString	= "hello world";

	protected String		keyEncode	= "UTF-8";
}
