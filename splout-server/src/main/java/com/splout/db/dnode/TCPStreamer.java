package com.splout.db.dnode;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
 * Work-in-progress
 */
public class TCPStreamer {

  private final static Log log = LogFactory.getLog(TCPStreamer.class);

  private DNodeHandler dNode;
  private int tcpPort;

  private TCPServer server;

  public void start(SploutConfiguration config, DNodeHandler dNode) throws InterruptedException {
    this.dNode = dNode;
    this.tcpPort = config.getInt(DNodeProperties.STREAMING_PORT);

    server = new TCPServer();

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

    public void serve() {
      try {
        this.serverSocket = new ServerSocket(tcpPort);
      } catch (IOException e) {
        throw new RuntimeException("Cannot open port " + tcpPort, e);
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

  public static void main(String[] args) throws UnknownHostException, IOException, SerializationException {
    SploutConfiguration config = SploutConfiguration.get();
    Socket clientSocket = new Socket("localhost", config.getInt(DNodeProperties.STREAMING_PORT));

    DataInputStream inFromServer = new DataInputStream(new BufferedInputStream(clientSocket.getInputStream()));
    DataOutputStream outToServer = new DataOutputStream(new BufferedOutputStream(clientSocket.getOutputStream()));

//    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
//    System.out.println("Enter tablespace: ");
//    String tablespace = reader.readLine();
//
//    System.out.println("Enter version number: ");
//    long versionNumber = Long.parseLong(reader.readLine());
//
//    System.out.println("Enter partition: ");
//    int partition = Integer.parseInt(reader.readLine());
//
//    System.out.println("Enter query: ");
//    String query = reader.readLine();

    String tablespace = "theadex";
    Long versionNumber = 1416485195l;
    Integer partition = 0;
    String query = "SELECT COUNT(*) FROM TopInterests;";
    
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