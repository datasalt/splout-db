package com.splout.db.common;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.junit.Test;

public class TestCompressorUtil {

	@Test
	public void test() throws IOException {
		File foo = new File("foo");
		foo.mkdir();
		new File(foo, "foo1").mkdir();
		new File(foo, "foo2.tmp").createNewFile();
		new File("foo/foo1/foo3.tmp").createNewFile();
		
		File compressed = new File("foo.zip");
		
		CompressorUtil.createZip(foo, compressed, FileFilterUtils.trueFileFilter(), FileFilterUtils.trueFileFilter());
		
		assertTrue(compressed.exists() && compressed.length() > 0);
		
		File uncompFoo = new File("uncomp-foo");
		
		CompressorUtil.uncompress(compressed, uncompFoo);
		
		assertTrue(uncompFoo.exists());
		assertTrue(uncompFoo.isDirectory());
		assertTrue(new File(uncompFoo, "foo1").exists());
		assertTrue(new File(uncompFoo, "foo1").isDirectory());
		assertTrue(new File(uncompFoo, "foo2.tmp").exists());
		assertTrue(new File(uncompFoo, "foo1/foo3.tmp").exists());
		
		FileUtils.deleteQuietly(compressed);
		FileUtils.deleteDirectory(foo);
		FileUtils.deleteDirectory(uncompFoo);
	}
}
