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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.IOFileFilter;

import java.io.*;
import java.util.Collection;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * An utility for creating a ZIP file with arbitrary file tree structure.
 */
public class CompressorUtil {

  public static void createZip(File dir, File out, IOFileFilter filefilter, IOFileFilter dirFilter)
      throws IOException {
    Collection<File> files = FileUtils.listFiles(dir, filefilter, dirFilter);

    out.delete();
    ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(out));
    byte[] buf = new byte[1024];
    for (File f : files) {
      ZipEntry ze = new ZipEntry(getRelativePath(f, dir));
      zos.putNextEntry(ze);
      InputStream is = new FileInputStream(f);
      int cnt;
      while ((cnt = is.read(buf)) >= 0) {
        zos.write(buf, 0, cnt);
      }
      is.close();
      zos.flush();
      zos.closeEntry();
    }
    zos.close();
  }

  // http://stackoverflow.com/questions/5373582/how-to-get-the-relative-path-of-the-file-to-a-folder-using-java
  public static String getRelativePath(File file, File folder) {
    String filePath = file.getAbsolutePath();
    String folderPath = folder.getAbsolutePath();
    if (filePath.startsWith(folderPath)) {
      return filePath.substring(folderPath.length() + 1);
    } else {
      return null;
    }
  }

  public static void uncompress(File file) throws IOException {
    uncompress(file, file.getParentFile());
  }

  public static void uncompress(File file, File dest) throws IOException {
    ZipFile zipFile = new ZipFile(file);
    Enumeration<? extends ZipEntry> entries = zipFile.entries();

    while (entries.hasMoreElements()) {
      ZipEntry entry = entries.nextElement();
      File entryDestination = new File(dest, entry.getName());
      entryDestination.getParentFile().mkdirs();
      InputStream in = zipFile.getInputStream(entry);
      OutputStream out = new FileOutputStream(entryDestination);
      IOUtils.copy(in, out);
      in.close();
      out.close();
    }
  }
}
