package com.splout.db.engine;

/*
 * #%L
 * Splout Redis
 * %%
 * Copyright (C) 2012 - 2014 Datasalt Systems S.L.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.splout.db.engine.EngineManager.EngineException;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestRedisManager {

  final static String TEST_FOLDER = "tmp-" + TestRedisManager.class.getName();

  @Before
  public void prepare() throws IOException {
    File file = new File(TEST_FOLDER);
    if (file.exists())
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
