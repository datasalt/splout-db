package com.splout.db.hadoop.engine;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.splout.db.hadoop.TableSpec;
import com.splout.db.hadoop.TableSpec.FieldIndex;

public class TestSploutSQLOutputFormat {

	@Test
	public void testPreSQL() throws Exception {
		final Schema tupleSchema1 = new Schema("schema1", Fields.parse("a:string, b:int"));
		String[] initSQL = new String[] { "init1", "init2" };
		String[] preInsertSQL = new String[] { "CREATE Mytable;", "ME_LO_INVENTO" };
		TableSpec tableSpec = new TableSpec(tupleSchema1, new Field[] { tupleSchema1.getField(0) },
		    new FieldIndex[] { new FieldIndex(tupleSchema1.getField(0), tupleSchema1.getField(1)) },
		    initSQL, preInsertSQL, null, null, null);
		String[] createTables = new SQLite4JavaOutputFormat(10, tableSpec)
		    .getCreateTables(tableSpec);
		assertEquals("init1", createTables[0]);
		assertEquals("init2", createTables[1]);
		assertEquals("CREATE TABLE schema1 (a TEXT, b INTEGER);", createTables[2]);
		assertEquals("CREATE Mytable;", createTables[3]);
		assertEquals("ME_LO_INVENTO", createTables[4]);
	}

	@Test
	public void testPostSQL() throws Exception {
		final Schema tupleSchema1 = new Schema("schema1", Fields.parse("a:string, b:int"));
		String[] afterInsertSQL = new String[] { "afterinsert1", "afterinsert2" };
		String[] finalSQL = new String[] { "DROP INDEX idx_schema1_ab", "CREATE INDEX blablabla" };
		TableSpec tableSpec = new TableSpec(tupleSchema1, new Field[] { tupleSchema1.getField(0) },
		    new FieldIndex[] { new FieldIndex(tupleSchema1.getField(0), tupleSchema1.getField(1)) }, null,
		    null, afterInsertSQL, finalSQL, null);
		String[] createIndex = SploutSQLOutputFormat.getCreateIndexes(tableSpec);
		assertEquals("afterinsert1", createIndex[0]);
		assertEquals("afterinsert2", createIndex[1]);
		assertEquals("CREATE INDEX idx_schema1_ab ON schema1(a, b);", createIndex[2]);
		assertEquals("DROP INDEX idx_schema1_ab", createIndex[3]);
		assertEquals("CREATE INDEX blablabla", createIndex[4]);
	}
}
