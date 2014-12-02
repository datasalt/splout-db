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
 * A simple class for reading Thrift objects (of a single type) from a file.
 *
 * @author Joel Meyer - simplified & modified by us
 */
public class ThriftReader {

  /**
   * File containing the objects.
   */
  protected final File file;

  /**
   * For reading the file.
   */
  private BufferedInputStream bufferedIn;

  /**
   * For reading the binary thrift objects.
   */
  private TBinaryProtocol binaryIn;

  /**
   * Constructor.
   *
   * @throws FileNotFoundException
   */
  public ThriftReader(File file) throws FileNotFoundException {
    this.file = file;
    open();
  }

  /**
   * Opens the file for reading. Must be called before {@link read()}.
   */
  private void open() throws FileNotFoundException {
    bufferedIn = new BufferedInputStream(new FileInputStream(file), 2048);
    binaryIn = new TBinaryProtocol(new TIOStreamTransport(bufferedIn));
  }

  /**
   * Checks if another objects is available by attempting to read another byte from the stream.
   */
  public boolean hasNext() throws IOException {
    bufferedIn.mark(1);
    int val = bufferedIn.read();
    bufferedIn.reset();
    return val != -1;
  }

  /**
   * Reads the next object from the file.
   */
  @SuppressWarnings("rawtypes")
  public <T extends TBase> T read(T t) throws IOException {
    try {
      t.read(binaryIn);
    } catch (TException e) {
      throw new IOException(e);
    }
    return (T) t;
  }

  /**
   * Close the file.
   */
  public ThriftReader close() throws IOException {
    bufferedIn.close();
    return this;
  }
}