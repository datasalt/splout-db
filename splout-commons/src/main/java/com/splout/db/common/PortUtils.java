package com.splout.db.common;

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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class PortUtils {


	public static class PortLock {
		final int port;
		final FileLock lock;
		final File file;
		
		public PortLock(int port, FileLock lock, File file) {
			this.port = port;
			this.lock = lock;
			this.file = file;
		}

		public int getPort() {
			return port;
		}

		public void release() {
			try {
	      lock.release();
      } catch(IOException e) {
      }
      file.delete();
		}
	}

	public static PortLock getNextAvailablePort(int port) {
		// Look for next available port
		FileLock lock = null;
		File lockFile = null;
		boolean free = false;
		do {
			try {
				ServerSocket socket = new ServerSocket(port);
				socket.close();
				/*
				 * It's actually unsafe to assume the port will still be free when using it after calling this method. And
				 * "mysqld" can't handle well the situation where two daemons are started with the same port concurrently.
				 * Therefore we need to ensure that only ONE PROCESS locks the port at a time. We use NIO FileLock for that.
				 * Because this is fast, we lock on a temporary file and release it afterwards.
				 */
				lockFile = new File("/tmp", "portlock_" + port); // can't use java.io.tmpdir as it is overriden by Hadoop
				if(!lockFile.exists()) {
					lockFile.createNewFile();
					FileChannel channel = new RandomAccessFile(lockFile, "rw").getChannel();
					lock = channel.tryLock();
					if(lock != null) {
						free = true;
					}
				}
				if(!free) {
					port++;
				}
			} catch(Exception e) {
				port++;
			}
		} while(!free);
		return new PortLock(port, lock, lockFile);
	}
}
