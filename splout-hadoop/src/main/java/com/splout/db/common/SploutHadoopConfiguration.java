package com.splout.db.common;

/*
 * #%L
 * Splout SQL Hadoop library
 * %%
 * Copyright (C) 2012 Datasalt Systems S.L.
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Configuratio helper class.
 */
public class SploutHadoopConfiguration {

  static Log log = LogFactory.getLog(SploutHadoopConfiguration.class);

  /**
   * Adds the SQLite native libraries to the DistributedCache so that they will be present in the java.library.path
   * of the child's Hadoop task.
   * <p/>
   * Usually you don't need to do this as the task will already try to load them from the job's uncompressed JAR, however
   * it is not assured that all Hadoop versions do the uncompressing of the JAR so in this case it's safer to use this.
   * <p/>
   * This method uses the default "native" folder.
   */
  public static void addSQLite4JavaNativeLibsToDC(Configuration conf) throws IOException, URISyntaxException {
    addSQLite4JavaNativeLibsToDC(conf, new File("native"));
  }

  /**
   * Adds the SQLite native libraries to the DistributedCache so that they will be present in the java.library.path
   * of the child's Hadoop task.
   * <p/>
   * Usually you don't need to do this as the task will already try to load them from the job's uncompressed JAR, however
   * it is not assured that all Hadoop versions do the uncompressing of the JAR so in this case it's safer to use this.
   */
  public static void addSQLite4JavaNativeLibsToDC(Configuration conf, File nativeLibsLocalPath) throws IOException, URISyntaxException {
    Path nativeLibHdfs = new Path("splout-native");
    FileSystem fS = FileSystem.get(conf);
    if (fS.exists(nativeLibHdfs)) {
      fS.delete(nativeLibHdfs, true);
    }
    fS.mkdirs(nativeLibHdfs);
    // Copy native libs to HDFS
    File[] natives = nativeLibsLocalPath.listFiles();
    if (natives == null) {
      throw new RuntimeException("natives lib folder not present in local working directory! Are you in SPLOUT_HOME?");
    }
    for (File nativeLib : natives) {
      FileUtil.copy(nativeLib, fS, nativeLibHdfs, false, conf);
    }
    for (FileStatus nativeLibInHdfs : fS.listStatus(nativeLibHdfs)) {
      // http://hadoop.apache.org/docs/r0.20.2/native_libraries.html#Loading+native+libraries+through+DistributedCache
      DistributedCache.createSymlink(conf);
      URI uriToAdd = new URI(nativeLibInHdfs.getPath().makeQualified(fS) + "#" + nativeLibInHdfs.getPath().getName());
      DistributedCache.addCacheFile(uriToAdd, conf);
      log.info("Adding to distributed cache: " + uriToAdd);
    }
  }
}
