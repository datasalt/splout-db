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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.splout.db.common.SploutClient;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.resource.Resource;
import org.mortbay.resource.ResourceCollection;

/**
 * Front-end for the Wikipedia pagecounts example.
 */
public class PageCountsWebapp {

  @Parameter(names = {"-p", "--port"}, description = "The port the webapp will run on.")
  Integer port = 8080;

  @Parameter(names = {"-q", "--qnodes"}, description = "The QNodes this demo will use, comma-separated.")
  String qNodes = "http://localhost:4412";

  public void run() throws Exception {
    Server server = new Server(port);

    WebAppContext context = new WebAppContext();
    context.setContextPath("/");
    context.addServlet(new ServletHolder(new DefaultServlet()), "/pagecounts/*");
    context.addServlet(new ServletHolder(new PageCountsServlet(new SploutClient(qNodes.split(",")))), "/api");
    context.addServlet(new ServletHolder(new PageCountsTrendingServlet()), "/trends");

    ResourceCollection resources = new ResourceCollection(new String[]{Resource.newClassPathResource(
        "pagecounts").toString()});

    context.setBaseResource(resources);

    server.setHandler(context);
    server.start();

    try {
      while (true) {
        Thread.sleep(5000);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
      server.stop();
    }
  }

  public static void main(String[] args) {
    PageCountsWebapp webapp = new PageCountsWebapp();
    JCommander jComm = new JCommander(webapp);
    jComm.setProgramName("Page Counts Splout Example Webapp");
    try {
      jComm.parse(args);
      webapp.run();
    } catch (ParameterException e) {
      System.err.println(e.getMessage());
      jComm.usage();
      System.exit(-1);
    } catch (Throwable t) {
      t.printStackTrace();
      jComm.usage();
      System.exit(-1);
    }
  }
}
