package xson.thirdparty.redis;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xson.thirdparty.redis.JedisClient;

public class AppTest {

	JedisClient	client	= JedisClient.getInstance();

	@Before
	public void before() {
		Properties properties = new Properties();
		try {
			properties.load(AppTest.class.getResourceAsStream("/redis.basic.properties"));
			// properties.load(AppTest.class.getResourceAsStream("/redis.sentinel.properties"));
			// properties.load(AppTest.class.getResourceAsStream("/redis.cluster.properties"));
			client.start(properties);
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	@After
	public void after() {
		client.stop();
	}

	@Test
	public void test01() {
		client.set("abc", "aaaaaaaaaaaaaaaaaaa");
		String value = client.get("abc");
		System.out.println("value:" + value);
	}

	@Test
	public void test011() {
		String x = client.set("abc1", "aaaaaaaaaaaaaaaaaaa", "nx", "ex", 100);
		if (!"OK".equalsIgnoreCase(x)) {
			x = client.set("abc1", "aaaaaaaaaaaaaaaaaaa", "xx", "ex", 100);
			System.out.println("xx:" + x);
		} else {
			System.out.println("nx:" + x);
		}
		// System.out.println(x);//OK
		String value = client.get("abc1");
		System.out.println("value:" + value);
	}

	@Test
	public void test02() {
		for (int i = 0; i < 100; i++) {
			client.set("key_" + i, "xxxx_" + i);
			String value = client.get("key_" + i);
			System.out.println("key:" + ("key_" + i) + ", value:" + value);
		}
	}

	@Test
	public void test03() {
		client.set("abc", "aaaaaaaaaaaaaaaaaaa");
		for (int i = 0; i < 100; i++) {
			System.out.println("-----------------");
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String value = client.get("abc");
			System.out.println("value:" + value);
		}
	}

}
