package com.splout.db.engine;

/*
 * #%L
 * Splout SQL commons
 * %%
 * Copyright (C) 2012 - 2013 Datasalt Systems S.L.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */


import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.common.TimeoutThread;
import com.splout.db.engine.EngineManager.EngineException;
import com.splout.db.engine.ResultSerializer.SerializationException;
import org.junit.Test;

import java.io.File;
import java.sql.SQLException;

public class TestSQLite4JavaManager extends SQLManagerTester {

	public static String TEST_DB_1 = TestSQLite4JavaManager.class.getName() + ".1.db";
	public static String TEST_DB_2 = TestSQLite4JavaManager.class.getName() + ".2.db";
	public static String TEST_DB_3 = TestSQLite4JavaManager.class.getName() + ".3.db";
	
	@Test
	public void test() throws Exception {
		File dbFile = new File(TEST_DB_1);
		if(dbFile.exists()) {
			dbFile.delete();
		}

		final SQLite4JavaManager sqlite4Java = new SQLite4JavaManager(TEST_DB_1, null);
		basicTest(sqlite4Java);
		sqlite4Java.close();
		dbFile.delete();
	}
	
	@Test
	public void testQuerySizeLimiting() throws SQLException, ClassNotFoundException, JSONSerDeException, EngineException, SerializationException {
		File dbFile = new File(TEST_DB_2);
		if(dbFile.exists()) {
			dbFile.delete();
		}

		final SQLite4JavaManager sqlite4Java = new SQLite4JavaManager(TEST_DB_2, null);
		querySizeLimitingTest(sqlite4Java);
		sqlite4Java.close();
		dbFile.delete();
	}

  @Test
  public void testExceptions() throws SQLException, ClassNotFoundException, JSONSerDeException, EngineException, SerializationException {
    File dbFile = new File(TEST_DB_3);
    if(dbFile.exists()) {
      dbFile.delete();
    }

    final SQLite4JavaManager sqlite4Java = new SQLite4JavaManager(TEST_DB_2, null);
    try {
      sqlite4Java.query("SELECT hex(randomblob(1000))", 0);
      throw new AssertionError("TooManyResultsException expected but not thrown.");
    } catch (EngineManager.TooManyResultsException e) {
    }

    TimeoutThread t = new TimeoutThread(1) {
      SQLiteConnection con;
      @Override
      public void startQuery(SQLiteConnection connection, String query) {
          this.con = connection;
      }

      @Override
      public void endQuery(SQLiteConnection connection) {
        con = null;
      }

      @Override
      public void run() {
        while (true) {
          if (con != null) {
            try {
              con.interrupt();
            } catch (SQLiteException e) {
              throw new AssertionError();
            }
          }
        }
      }
    };
    t.start();
    sqlite4Java.setTimeoutThread(t);
    try {
      sqlite4Java.query("SELECT hex(randomblob(100000))", 1);
      throw new AssertionError("QueryInterruptedException expected but not thrown.");
    } catch(EngineManager.QueryInterruptedException e){
    }
    sqlite4Java.close();
    dbFile.delete();
  }

}
