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

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

import java.io.*;

/**
 * Simple class that makes it easy to write Thrift objects to disk.
 *
 * @author Joel Meyer - simplified & modified by us
 */
public class ThriftWriter {
  /**
   * File to write to.
   */
  protected final File file;

  /**
   * For writing to the file.
   */
  private BufferedOutputStream bufferedOut;

  /**
   * For binary serialization of objects.
   */
  private TBinaryProtocol binaryOut;

  /**
   * Constructor.
   *
   * @throws FileNotFoundException
   */
  public ThriftWriter(File file) throws FileNotFoundException {
    this.file = file;
    open();
  }

  /**
   * Open the file for writing.
   */
  private void open() throws FileNotFoundException {
    bufferedOut = new BufferedOutputStream(new FileOutputStream(file), 2048);
    binaryOut = new TBinaryProtocol(new TIOStreamTransport(bufferedOut));
  }

  /**
   * Write the object to disk.
   */
  @SuppressWarnings("rawtypes")
  public void write(TBase t) throws IOException {
    try {
      t.write(binaryOut);
      bufferedOut.flush();
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  /**
   * Close the file stream.
   */
  public void close() throws IOException {
    bufferedOut.close();
  }
}
