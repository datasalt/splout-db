package com.splout.db.common;

/*
 * #%L
 * Splout SQL Server
 * %%
 * Copyright (C) 2012 Datasalt Systems S.L.
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

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import static org.junit.Assert.assertEquals;

public class TestSploutConfiguration {

  @Test
  public void test() throws UnsupportedEncodingException, IOException {
    // Run twice for making sure we don't leave unexpected state around
    innerTest();
    innerTest();
  }

  public void innerTest() throws UnsupportedEncodingException, IOException {
    File testConfig = new File("testConfig");
    testConfig.mkdir();
    // foo.property=value1 in defaults file
    Files.write("foo.property\tvalue1".getBytes("UTF-8"), new File(testConfig, SploutConfiguration.SPLOUT_PROPERTIES + ".default"));
    assertEquals(SploutConfiguration.get("testConfig").getString("foo.property"), "value1");
    // foo.property=value2 in main file - assert that it overrides default
    Files.write("foo.property\tvalue2".getBytes("UTF-8"), new File(testConfig, SploutConfiguration.SPLOUT_PROPERTIES));
    assertEquals(SploutConfiguration.get("testConfig").getString("foo.property"), "value2");
    FileUtils.deleteDirectory(testConfig);
  }
}
