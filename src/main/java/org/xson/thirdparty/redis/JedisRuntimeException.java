package org.xson.thirdparty.redis;

public class JedisRuntimeException extends RuntimeException {

	private static final long	serialVersionUID	= 2669008491277369821L;

	public JedisRuntimeException(String message, Throwable cause) {
		super(message, cause);
	}

	public JedisRuntimeException(Throwable cause) {
		super(cause);
	}
}
