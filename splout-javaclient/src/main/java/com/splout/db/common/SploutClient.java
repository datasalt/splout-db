package com.splout.db.common;

/*
 * #%L
 * Splout SQL Java client
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

import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.splout.db.common.JSONSerDe.JSONSerDeException;
import com.splout.db.qnode.beans.*;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Java HTTP Interface to Splout that uses Google Http Client (https://code.google.com/p/google-http-java-client/). We
 * chose this client over Jersey or HttpClient to avoid conflicts with Hadoop dependencies.
 */
public class SploutClient {

  HttpRequestFactory requestFactory;
  String[] qNodes;
  String[] qNodesNoProtocol;

  public SploutClient(String... qnodes) {
    this(20 * 1000, qnodes);
  }

  public SploutClient(final int timeoutMillis, String... qnodes) {
    HttpTransport transport = new NetHttpTransport();
    requestFactory = transport.createRequestFactory(new HttpRequestInitializer() {
      @Override
      public void initialize(HttpRequest request) throws IOException {
        request.readTimeout = timeoutMillis;
      }
    });
    this.qNodes = qnodes;
    // strip last "/" if present
    for (int i = 0; i < qNodes.length; i++) {
      if (qNodes[i].endsWith("/")) {
        qNodes[i] = qNodes[i].substring(0, qNodes[i].length() - 1);
      }
    }
    this.qNodesNoProtocol = new String[qNodes.length];
    for (int i = 0; i < qNodes.length; i++) {
      qNodesNoProtocol[i] = qNodes[i].replaceAll("http://", "");
    }
  }

  public static String asString(InputStream inputStream) throws IOException {
    StringWriter writer = new StringWriter();
    try {
      IOUtils.copy(inputStream, writer);
      return writer.toString();
    } finally {
      inputStream.close();
      writer.close();
    }
  }

  /*
   *
   */
  public Tablespace tablespace(String tablespace) throws IOException {
    HttpRequest request = requestFactory.buildGetRequest(new GenericUrl(
        qNodes[(int) (Math.random() * qNodes.length)] + "/api/tablespace/" + tablespace));
    HttpResponse resp = request.execute();
    try {
      return JSONSerDe.deSer(asString(resp.getContent()), Tablespace.class);
    } catch (JSONSerDeException e) {
      throw new IOException(e);
    }
  }

  /*
   *
   */
  public QNodeStatus overview() throws IOException {
    HttpRequest request = requestFactory.buildGetRequest(new GenericUrl(
        qNodes[(int) (Math.random() * qNodes.length)] + "/api/overview"));
    HttpResponse resp = request.execute();
    try {
      return JSONSerDe.deSer(asString(resp.getContent()), QNodeStatus.class);
    } catch (JSONSerDeException e) {
      throw new IOException(e);
    }
  }

  /*
   *
   */
  @SuppressWarnings("unchecked")
  public List<String> dNodeList() throws IOException {
    HttpRequest request = requestFactory.buildGetRequest(new GenericUrl(
        qNodes[(int) (Math.random() * qNodes.length)] + "/api/dnodelist"));
    HttpResponse resp = request.execute();
    try {
      return JSONSerDe.deSer(asString(resp.getContent()), ArrayList.class);
    } catch (JSONSerDeException e) {
      throw new IOException(e);
    }
  }

  private static class StringHttpContent implements HttpContent {

    byte[] content;

    public StringHttpContent(String strCont) {
      try {
        content = strCont.getBytes("UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public String getEncoding() {
      return "UTF-8";
    }

    @Override
    public long getLength() throws IOException {
      return content.length;
    }

    @Override
    public String getType() {
      return "text/ascii";
    }

    @Override
    public boolean retrySupported() {
      return false;
    }

    @Override
    public void writeTo(OutputStream oS) throws IOException {
      oS.write(content);
    }
  }

  /*
   *
   */
  public QueryStatus query(String tablespace, String key, String query, String partition)
      throws IOException {
    URI uri;
    try {
      uri = new URI("http", qNodesNoProtocol[(int) (Math.random() * qNodes.length)], "/api/query/"
          + tablespace, "&sql=" + query + (key != null ? "&key=" + key : "")
          + (partition != null ? "&partition=" + partition : "")
         , null);

      HttpRequest request = requestFactory.buildGetRequest(new GenericUrl(uri.toASCIIString()));
      HttpResponse resp = request.execute();
      try {
        return JSONSerDe.deSer(asString(resp.getContent()), QueryStatus.class);
      } catch (JSONSerDeException e) {
        throw new IOException(e);
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public DeployInfo deploy(String tablespace, PartitionMap partitionMap, ReplicationMap replicationMap,
                           URI dataUri) throws IOException {

    DeployRequest deployRequest = new DeployRequest();
    deployRequest.setTablespace(tablespace);
    deployRequest.setData_uri(dataUri.toString());
    deployRequest.setPartitionMap(partitionMap.getPartitionEntries());
    deployRequest.setReplicationMap(replicationMap.getReplicationEntries());

    return deploy(deployRequest);
  }

  public DeployInfo deploy(final DeployRequest... requests) throws IOException {
    try {
      String strCont = JSONSerDe.ser(new ArrayList<DeployRequest>(Arrays.asList(requests)));
      HttpContent content = new StringHttpContent(strCont);
      HttpRequest request = requestFactory.buildPostRequest(new GenericUrl(
          qNodes[(int) (Math.random() * qNodes.length)] + "/api/deploy"), content);
      HttpResponse resp = request.execute();
      return JSONSerDe.deSer(asString(resp.getContent()), DeployInfo.class);
    } catch (JSONSerDeException e) {
      throw new IOException(e);
    }
  }

  /*
   * Same method as query(), but using POST instead of GET. Useful for very long SQL queries.
   */
  public QueryStatus queryPost(String tablespace, String key, String query, String partition)
      throws IOException {

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("sql", query);
    params.put("key", new String[]{key});
    params.put("partition", partition);

    try {
      HttpContent content = new StringHttpContent(JSONSerDe.ser(params));
      HttpRequest request = requestFactory.buildPostRequest(new GenericUrl(
          qNodes[(int) (Math.random() * qNodes.length)] + "/api/query/" + tablespace), content);
      HttpResponse resp = request.execute();
      return JSONSerDe.deSer(asString(resp.getContent()), QueryStatus.class);
    } catch (JSONSerDeException e) {
      throw new IOException(e);
    }
  }

  public StatusMessage cancelDeploy(long version) throws IOException {
    HttpRequest request = requestFactory.buildGetRequest(new GenericUrl(
        qNodes[(int) (Math.random() * qNodes.length)] + "/api/canceldeployment?version=" + version));
    HttpResponse resp = request.execute();
    try {
      return JSONSerDe.deSer(asString(resp.getContent()), StatusMessage.class);
    } catch (JSONSerDeException e) {
      throw new IOException(e);
    }
  }
}