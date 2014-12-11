package com.splout.db.benchmark;

/*
 * #%L
 * Splout SQL Server
 * %%
 * Copyright (C) 2012 - 2014 Datasalt Systems S.L.
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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.splout.db.common.JSONSerDe.JSONSerDeException;

/**
 * Used by {@link StreamingPerfTest} 
 */
public class TCPTest {

  private final static Log log = LogFactory.getLog(TCPTest.class);

  public static void tcpTest(String file, String table) throws UnknownHostException, IOException, InterruptedException, JSONSerDeException {
    final TCPServer server = new TCPServer(8888, file, table);

    Thread t = new Thread() {
      @Override
      public void run() {
        server.serve();
      }
    };
    t.start();

    while (!server.isServing()) {
      Thread.sleep(100);
    }

    Socket clientSocket = new Socket("localhost", 8888);
    DataInputStream inFromServer = new DataInputStream(new BufferedInputStream(clientSocket.getInputStream()));

    try {
      do {
        // Read a record
        inFromServer.readUTF();
        inFromServer.readInt();
        inFromServer.readDouble();
        inFromServer.readUTF();
      } while (true);
    } catch (Throwable th) {
      th.printStackTrace();
    }

    clientSocket.close();
    server.stop();
    t.interrupt();
  }

  static class TCPServer {

    protected int serverPort;
    protected ServerSocket serverSocket = null;
    protected boolean isStopped = false;
    protected String file;
    protected String tableName;

    public TCPServer(int port, String file, String tableName) {
      this.file = file;
      this.tableName = tableName;
      this.serverPort = port;
    }

    private synchronized boolean isStopped() {
      return this.isStopped;
    }

    public synchronized void stop() {
      this.isStopped = true;
      try {
        this.serverSocket.close();
      } catch (IOException e) {
        throw new RuntimeException("Error closing server", e);
      }
    }

    public boolean isServing() {
      return !isStopped() && this.serverSocket != null && this.serverSocket.isBound();
    }

    public void serve() {
      try {
        this.serverSocket = new ServerSocket(this.serverPort);
      } catch (IOException e) {
        throw new RuntimeException("Cannot open port " + this.serverPort, e);
      }
      while (!isStopped()) {
        Socket clientSocket = null;
        try {
          clientSocket = this.serverSocket.accept();
        } catch (IOException e) {
          if (isStopped()) {
            System.out.println("Server Stopped.");
            return;
          }
          throw new RuntimeException("Error accepting client connection", e);
        }
        new Thread(new WorkerRunnable(clientSocket, file, tableName)).start();
      }
    }
  }

  static class WorkerRunnable implements Runnable {

    protected Socket clientSocket = null;
    protected String fileName = null;
    protected String tableName = null;

    public WorkerRunnable(Socket clientSocket, String file, String tableName) {
      this.clientSocket = clientSocket;
      this.fileName = file;
      this.tableName = tableName;
    }

    public void run() {
      try {
        InputStream input = clientSocket.getInputStream();
        OutputStream output = clientSocket.getOutputStream();

        File file = new File(fileName);
        log.info("Reading file: " + file + " using SQLite and sending it over the network using TCP...");
        SQLiteConnection conn = new SQLiteConnection(file);
        conn.open(true);
        SQLiteStatement st = conn.prepare("SELECT * FROM " + tableName, false);

        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(output));
        
        do {
          st.step();
          if (st.hasRow()) {
            dos.writeUTF(st.columnString(0));
            dos.writeInt(st.columnInt(1));
            dos.writeDouble(st.columnDouble(2));
            dos.writeUTF(st.columnString(3));
          } else {
            break;
          }
        } while (true);

        st.dispose();
        conn.dispose();

        output.close();
        input.close();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (SQLiteException e) {
        e.printStackTrace();
      } 
    }
  }
}