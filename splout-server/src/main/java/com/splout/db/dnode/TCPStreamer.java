package com.splout.db.dnode;

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
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.splout.db.common.SploutConfiguration;
import com.splout.db.engine.EngineManager;
import com.splout.db.engine.EngineManager.EngineException;
import com.splout.db.engine.ResultSerializer;
import com.splout.db.engine.ResultSerializer.SerializationException;
import com.splout.db.engine.StreamingIterator;
import com.splout.db.thrift.DNodeException;

/**
 * An interface for the DNode to send streaming data through TCP.
 */
public class TCPStreamer {

  private final static Log log = LogFactory.getLog(TCPStreamer.class);

  private DNodeHandler dNode;
  private int tcpPort;

  private TCPServer server;

  public void start(SploutConfiguration config, DNodeHandler dNode) throws InterruptedException, IOException {
    this.dNode = dNode;
    this.tcpPort = config.getInt(DNodeProperties.STREAMING_PORT);

    server = new TCPServer();
    server.bind();
    
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
  }

  public void stop() {
    server.stop();
  }

  public int getTcpPort() {
    return tcpPort;
  }

  class TCPServer {

    protected ServerSocket serverSocket = null;
    protected boolean isStopped = false;

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

    public void bind() throws IOException {
      try {
        this.serverSocket = new ServerSocket(tcpPort);
      } catch (IOException e) {
        throw e;
      }
    }

    public void serve() {
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

        new Thread(new WorkerRunnable(clientSocket, dNode)).start();
      }
    }
  }

  static class WorkerRunnable implements Runnable {

    protected final Socket clientSocket;
    protected final DNodeHandler handler;

    public WorkerRunnable(Socket clientSocket, DNodeHandler handler) {
      this.clientSocket = clientSocket;
      this.handler = handler;
    }

    public void run() {
      try {
        InputStream input = clientSocket.getInputStream();
        OutputStream output = clientSocket.getOutputStream();

        final DataInputStream dis = new DataInputStream(new BufferedInputStream(input));
        // Get tablespace
        String tablespace = dis.readUTF();
        // Get version number
        long version = dis.readLong();
        // Get partition number
        int partition = dis.readInt();
        // Get query
        final String query = dis.readUTF();

        log.info("Got streaming request: " + tablespace + ", " + version + ", " + partition + ", " + query);
        EngineManager manager = handler.getManager(tablespace, version, partition);

        final DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(output));

        manager.streamQuery(new StreamingIterator() {

          @Override
          public String getQuery() {
            return query;
          }

          @Override
          public void columns(String[] columns) {
          }

          @Override
          public void collect(Object[] result) {
            byte[] bytes;
            try {
              bytes = ResultSerializer.serializeToByteArray(result);
              dos.writeInt(bytes.length);
              dos.write(bytes);
            } catch (SerializationException e) {
              e.printStackTrace();
            } catch (IOException e) {
              e.printStackTrace();
            }
          }

          @Override
          public void endStreaming() {
            try {
              dos.writeInt(-1);
              dos.flush();
              log.info("Finished stream");
            } catch (IOException e) {
              e.printStackTrace();
            }
          }

        });

        output.close();
        input.close();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (EngineException e) {
        e.printStackTrace();
      } catch (DNodeException e1) {
        e1.printStackTrace();
      }
    }
  }

  /**
   * This main method can be used for testing the TCP interface directly to a
   * local DNode. Will ask for protocol input from Stdin and print output to
   * Stdout
   */
  public static void main(String[] args) throws UnknownHostException, IOException, SerializationException {
    SploutConfiguration config = SploutConfiguration.get();
    Socket clientSocket = new Socket("localhost", config.getInt(DNodeProperties.STREAMING_PORT));

    DataInputStream inFromServer = new DataInputStream(new BufferedInputStream(clientSocket.getInputStream()));
    DataOutputStream outToServer = new DataOutputStream(new BufferedOutputStream(clientSocket.getOutputStream()));

    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    System.out.println("Enter tablespace: ");
    String tablespace = reader.readLine();

    System.out.println("Enter version number: ");
    long versionNumber = Long.parseLong(reader.readLine());

    System.out.println("Enter partition: ");
    int partition = Integer.parseInt(reader.readLine());

    System.out.println("Enter query: ");
    String query = reader.readLine();

    outToServer.writeUTF(tablespace);
    outToServer.writeLong(versionNumber);
    outToServer.writeInt(partition);
    outToServer.writeUTF(query);

    outToServer.flush();

    byte[] buffer = new byte[0];
    boolean read;
    do {
      read = false;
      int nBytes = inFromServer.readInt();
      if (nBytes > 0) {
        buffer = new byte[nBytes];
        int inRead = inFromServer.read(buffer);
        if (inRead > 0) {
          Object[] res = ResultSerializer.deserialize(ByteBuffer.wrap(buffer), Object[].class);
          read = true;
          System.out.println(Arrays.toString(res));
        }
      }
    } while (read);

    clientSocket.close();
  }
}