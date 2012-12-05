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
import com.splout.db.benchmark.SploutBenchmark;
import com.splout.db.benchmark.SploutBenchmark.StressThreadImpl;
import com.splout.db.common.SploutClient;
import com.splout.db.qnode.beans.QueryStatus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * A benchmark designed ot stress-test Splout with the meteorogical dataset you can find
 * on examples/meteo. Each thread will launch an aggregation by day or month (depending on
 * configuration) selecting randomly the station between existing ones.
 */
public class MeteoBenchmark {

  @Parameter(required = true, names = {"-q", "--qnodes"}, description = "Comma-separated list QNode addresses.")
  private String qNodes;

  @Parameter(names = {"-n", "--niterations"}, description = "The number of iterations for running the benchmark more than once.")
  private Integer nIterations = 1;

  @Parameter(names = {"-nth", "--nthreads"}, description = "The number of threads to use for the test.")
  private Integer nThreads = 1;

  @Parameter(required = true, names = {"-nq", "--nqueries"}, description = "The number of queries to perform for the benchmark.")
  private Integer nQueries;

  @Parameter(required = false, names = {"-m", "--month"}, description = "Perform month queries instead of day ones.")
  private boolean monthQueries = false;

  public void start() throws InterruptedException {
    Map<String, String> context = new HashMap<String, String>();
    context.put("qnodes", qNodes);
    context.put("month", monthQueries + "");
    SploutBenchmark benchmark = new SploutBenchmark();
    for (int i = 0; i < nIterations; i++) {
      benchmark.stressTest(nThreads, nQueries, MeteoStressThreadImpl.class, context);
      benchmark.printStats(System.out);
    }
  }

  /**
   * Stress thread implementation following convention from {@link com.splout.db.benchmark.SploutBenchmark}.
   */
  public static class MeteoStressThreadImpl extends StressThreadImpl {

    SploutClient client;
    final String tablespace = "meteo-pby-stn-wban";

    ArrayList<Map<String, Integer>> stations;
    Random rand = new Random();

    Map<String, String> context;


    @Override
    public void init(Map<String, String> context) throws Exception {
      this.context = context;
      client = new SploutClient(context.get("qnodes").split(","));
      String query = "SELECT stn, wban FROM stations";
      System.out.println("Retrieving stations list");
      long statTime = System.currentTimeMillis();
      QueryStatus st = client.query(tablespace, "any", query);
      if (st.getResult() == null) {
        throw new RuntimeException("Impossible to retrieve stations list. " + st);
      }
      stations = st.getResult();
      System.out.println("Loaded " + stations.size() + " stations in " + (System.currentTimeMillis() - statTime) + " ms.");
    }

    @SuppressWarnings("unchecked")
    @Override
    public int nextQuery() throws Exception {
      try {

        int rndIdx = rand.nextInt(stations.size());
        int stn = stations.get(rndIdx).get("stn");
        int wban = stations.get(rndIdx).get("wban");
        String query;
        if (context.get("month").equals("false")) {
          query = "select year,month,day,min,max from meteo where stn=" + stn + " and wban = " + wban + " order by year,month,day";
        } else {
          query = "select year,month,min(min) as min,max(max) as max from meteo where stn=" + stn + " and wban=" + wban + " group by year,month order by year,month";
        }

        QueryStatus st = client
            .query(tablespace, stn + "" + wban, query);
        if (st.getResult() != null) {
          return st.getResult().size();
        } else {
          return 0;
        }
      } catch (java.net.SocketTimeoutException e) {
        System.err.println("More than 20 seconds... ");
        return 0;
      }
    }
  }

  public static void main(String[] args) throws InterruptedException {
    MeteoBenchmark benchmarkTool = new MeteoBenchmark();

    JCommander jComm = new JCommander(benchmarkTool);
    jComm.setProgramName("Meteo Benchmark Tool");
    try {
      jComm.parse(args);
    } catch (ParameterException e) {
      System.out.println(e.getMessage());
      System.out.println();
      jComm.usage();
      System.exit(-1);
    } catch (Throwable t) {
      t.printStackTrace();
      jComm.usage();
      System.exit(-1);
    }

    benchmarkTool.start();
    System.exit(0);
  }
}
