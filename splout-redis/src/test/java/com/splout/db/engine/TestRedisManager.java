package com.splout.db.engine;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import com.splout.db.engine.EngineManager.EngineException;

public class TestRedisManager {

	final static String TEST_FOLDER = "tmp-" + TestRedisManager.class.getName();
	
	@Before
	public void prepare() throws IOException {
		File file = new File(TEST_FOLDER);
		if(file.exists())
			FileUtils.deleteDirectory(file);
		file.mkdir();
	}
	
	@Test
	public void test() throws IOException, EngineException {
		RedisManager manager = new RedisManager();
		File dbFolder = new File(TEST_FOLDER);
		File dbFile = new File(dbFolder, "0.db");
		
		RDBOutputStream rdb = new RDBOutputStream(new FileOutputStream(dbFile));
		rdb.writeHeader();
		rdb.writeDatabaseSelector(0);
		
		rdb.writeString(RDBString.create("foo1"), RDBString.create("{\"value\":\"bar1\"}"));
		rdb.writeString(RDBString.create("foo2"), RDBString.create("{\"value\":\"bar2\"}"));
		rdb.writeFooter();
		rdb.close();
		
		manager.init(dbFile, BaseTest.getBaseConfiguration(), null);
		
		assertEquals("[]", manager.query("foo", 1));
		assertEquals("[{\"value\":\"bar1\"}]", manager.query("foo1", 1));
		assertEquals("[{\"value\":\"bar2\"}]", manager.query("foo2", 1));
		
		manager.close();
	}
}
