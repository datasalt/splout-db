package com.splout.db.engine;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.splout.db.common.JSONSerDe;
import com.splout.db.hadoop.engine.SploutSQLOutputFormatTester;

@SuppressWarnings("serial")
public class TestRedisOutputFormat extends SploutSQLOutputFormatTester {

	@Test
	public void test() throws Exception {
		runTest(new RedisEngine());
		
		RedisManager manager = new RedisManager();
		manager.init(new File(OUTPUT, "0.db"), BaseTest.getBaseConfiguration(), null);
		
		List l = JSONSerDe.deSer(manager.query("foo1", 1), ArrayList.class);
		Map<String, Object> record = (Map<String, Object>)l.get(0);
		
		assertEquals("foo1", record.get("a"));
		assertEquals(30, record.get("b"));
		
		manager.close();
	}
}
