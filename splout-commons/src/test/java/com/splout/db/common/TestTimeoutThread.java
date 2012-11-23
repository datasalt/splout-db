package com.splout.db.common;

/*
 * #%L
 * Splout SQL commons
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.splout.db.common.TimeoutThread.QueryAndTime;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SQLiteConnection.class })
public class TestTimeoutThread {

	@Test
	public void testNoInterrupt() throws InterruptedException {
	  final TimeoutThread timeoutThread = new TimeoutThread(10000);
		timeoutThread.start();
		
		final AtomicBoolean failed = new AtomicBoolean(false);

		Thread[] t = new Thread[10];
		for(int i = 0; i < 10; i++) {
			t[i] = new Thread() {
				public void run() {
					try {
	          Thread.sleep((long)(Math.random()*4000));
          } catch(InterruptedException e2) {
	          e2.printStackTrace();
          }
					SQLiteConnection conn = mock(SQLiteConnection.class);
					timeoutThread.startQuery(conn, "MyQuery" + this.getName());
					try {
	          Thread.sleep(2000);
          } catch(InterruptedException e1) {
	          e1.printStackTrace();
          }
					try {
	          verify(conn).interrupt();
          	failed.set(true);
          } catch(Throwable e) {
          	// expected
          }
          timeoutThread.endQuery(conn);
				};
			};
			t[i].start();
		}
		
		for(int i = 0; i < 10; i++) {
			t[i].join();
		}
		
		assertEquals(false, failed.get());
		
		assertEquals(10, timeoutThread.getConnections().size());
		assertEquals(10, timeoutThread.getCurrentQueries().size());
		for(Map.Entry<SQLiteConnection, QueryAndTime> entry: timeoutThread.getCurrentQueries().entrySet()) {
			assertEquals(new Long(-1l), entry.getValue().time);
			assertNull(entry.getValue().query);
		}
	}
	
	@Test
	public void test() throws SQLiteException, InterruptedException {
	  final TimeoutThread timeoutThread = new TimeoutThread(2000);
		timeoutThread.start();
		
		final AtomicBoolean failed = new AtomicBoolean(false);

		Thread[] t = new Thread[10];
		for(int i = 0; i < 10; i++) {
			t[i] = new Thread() {
				public void run() {
					try {
	          Thread.sleep((long)(Math.random()*4000));
          } catch(InterruptedException e2) {
	          e2.printStackTrace();
          }
					SQLiteConnection conn = mock(SQLiteConnection.class);
					timeoutThread.startQuery(conn, "MyQuery" + this.getName());
					try {
	          Thread.sleep(4000);
          } catch(InterruptedException e1) {
	          e1.printStackTrace();
          }
					try {
	          verify(conn).interrupt();
          } catch(Throwable e) {
          	failed.set(true);
	          e.printStackTrace();
          }
				};
			};
			t[i].start();
		}
		
		for(int i = 0; i < 10; i++) {
			t[i].join();
		}
		
		assertEquals(false, failed.get());
		
		assertEquals(10, timeoutThread.getConnections().size());
		assertEquals(10, timeoutThread.getCurrentQueries().size());
		for(Map.Entry<SQLiteConnection, QueryAndTime> entry: timeoutThread.getCurrentQueries().entrySet()) {
			assertEquals(new Long(-1l), entry.getValue().time);
			assertNull(entry.getValue().query);
		}
	}
}
