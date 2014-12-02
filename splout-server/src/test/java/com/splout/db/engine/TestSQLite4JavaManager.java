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

import static org.junit.Assert.*;

import java.io.File;
import java.sql.SQLException;

import org.junit.Test;

import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.engine.ResultSerializer.SerializationException;
import com.splout.db.engine.SQLManagerTester;
import com.splout.db.engine.SQLite4JavaManager;
import com.splout.db.engine.EngineManager.EngineException;

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

		final SQLite4JavaManager sqlite4Java = new SQLite4JavaManager(TEST_DB_1, null, false, 0);
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

		final SQLite4JavaManager sqlite4Java = new SQLite4JavaManager(TEST_DB_2, null, false, 0);
		querySizeLimitingTest(sqlite4Java);
		sqlite4Java.close();
		dbFile.delete();
	}
	
	@Test
	public void testResultPaging() throws ClassNotFoundException, SQLException, JSONSerDeException, EngineException, SerializationException {
    File dbFile = new File(TEST_DB_3);
    if(dbFile.exists()) {
      dbFile.delete();
    }

    final SQLite4JavaManager sqlite4Java = new SQLite4JavaManager(TEST_DB_3, null, true, 60);
    queryPaging(sqlite4Java);
    sqlite4Java.close();
    assertTrue(sqlite4Java.getClient().getServerSideCursors().get(1) == null);
    dbFile.delete();
	}
	
}
