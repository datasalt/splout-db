package com.splout.db.common;

/*
 * #%L
 * Splout SQL commons
 * %%
 * Copyright (C) 2012 - 2013 Datasalt Systems S.L.
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
		
		assertTrue(compressed.exists());
		
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
