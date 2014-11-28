package com.splout.db.examples;

/*
 * #%L
 * Splout SQL Hadoop library
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

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.splout.db.common.SploutClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Servlet that caches and returns the trending wikipedia articles from: http://www.trendingtopics.org/pages.xml?page=1
 */
@SuppressWarnings("serial")
public class PageCountsTrendingServlet extends HttpServlet {

  private final static Log log = LogFactory.getLog(PageCountsServlet.class);
  private HttpRequestFactory requestFactory;

  public PageCountsTrendingServlet() {
    HttpTransport transport = new NetHttpTransport();
    requestFactory = transport.createRequestFactory();
  }

  private long cachedFrom;
  private String cachedString = null;

  public final static long CACHE_FOR = 1000 * 60 * 5; // 5 minutes cache

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
      IOException {

    long now = System.currentTimeMillis();
    if (cachedString != null && (now - cachedFrom) < CACHE_FOR) {
      resp.getWriter().write(cachedString);
      log.info("Returning XML content from cache.");
      return;
    }

    HttpRequest trendReq = requestFactory.buildGetRequest(new GenericUrl("http://www.trendingtopics.org/pages.xml?page=1"));
    cachedString = SploutClient.asString(trendReq.execute().getContent());
    log.info("Got XML content from trendingtopics: " + cachedString);
    cachedFrom = System.currentTimeMillis();

    resp.getWriter().write(cachedString);
  }
}
