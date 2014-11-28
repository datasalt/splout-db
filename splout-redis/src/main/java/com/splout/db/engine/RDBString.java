package com.splout.db.engine;

/*
 * #%L
 * Splout SQL commons
 * %%
 * Copyright (C) 2012 - 2014 Datasalt Systems S.L.
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

import java.io.IOException;

public abstract class RDBString {
  abstract void write(RDBOutputStream out) throws IOException;

  public static RDBString uncompressed(byte[] bytes) {
    return new RDBByteString(bytes);
  }

  public static RDBString create(byte[] bytes) throws IOException {
    return uncompressed(bytes);
  }

  public static RDBString create(String string) throws IOException {
    return create(string.getBytes());
  }

  public static RDBString create(int i) {
    return new RDBIntString(i);
  }
}

class RDBByteString extends RDBString {
  byte[] bytes;

  RDBByteString(byte[] bytes) {
    this.bytes = bytes;
  }

  void write(RDBOutputStream out) throws IOException {
    out.writeLength(bytes.length);
    out.write(bytes);
  }
}

class RDBIntString extends RDBString {
  int value;

  RDBIntString(int value) {
    this.value = value;
  }

  void write(RDBOutputStream out) throws IOException {
    if (value >= -(1 << 7) && value <= (1 << 7) - 1) {
      out.write(0xC0);
      out.write(value & 0xFF);
    } else if (value >= -(1 << 15) && value <= (1 << 15) - 1) {
      out.write(0xC1);
      out.write(value & 0xFF);
      out.write((value >> 8) & 0xFF);
    } else {
      out.write(0xC2);
      out.write(value & 0xFF);
      out.write((value >> 8) & 0xFF);
      out.write((value >> 16) & 0xFF);
      out.write((value >> 24) & 0xFF);
    }
  }
}