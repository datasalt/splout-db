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

import java.io.IOException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.splout.db.benchmark.SploutBenchmark;
import com.splout.db.benchmark.SploutBenchmark.StressThreadImpl;
import com.splout.db.common.SploutClient;
import com.splout.db.common.Tablespace;
import com.splout.db.qnode.beans.QueryStatus;

/**
 * A benchmark designed ot stress-test Splout with the Wikipedia Page Counts dataset.
 */
@SuppressWarnings("unchecked")
public class PageCountsBenchmark {

	@Parameter(required = true, names = { "-q", "--qnodes" }, description = "Comma-separated list QNode addresses.")
	private String qNodes;

	@Parameter(names = { "-n", "--niterations" }, description = "The number of iterations for running the benchmark more than once.")
	private Integer nIterations = 1;

	@Parameter(names = { "-nth", "--nthreads" }, description = "The number of threads to use for the test.")
	private Integer nThreads = 1;

	@Parameter(required = true, names = { "-nq", "--nqueries" }, description = "The number of queries to perform for the benchmark.")
	private Integer nQueries;

	public final static String TABLESPACE = "pagecounts";

	public void start() throws InterruptedException, IOException {
		Map<String, Object> context = new HashMap<String, Object>();
		context.put("qnodes", qNodes);
		Map<Integer, Long> rowIdsPerPartition = new HashMap<Integer, Long>();
		context.put("rowIdsPerPartition", rowIdsPerPartition);

		// First get Tablespace metadata - nPartitions
		SploutClient client = new SploutClient(((String) context.get("qnodes")).split(","));
		Tablespace tablespace = client.tablespace(TABLESPACE);
		int nPartitions = tablespace.getPartitionMap().getPartitionEntries().size();

		// Then gather number of registers for each partition
		for(int partition = 0; partition < nPartitions; partition++) {
			String query = "SELECT MAX(rowid) FROM pagecounts;";
			Map<String, Object> obj = (Map<String, Object>) client
			    .query(TABLESPACE, null, query, partition + "").getResult().get(0);
			rowIdsPerPartition.put(partition, Long.parseLong(obj.get("MAX(rowid)").toString()));
		}

		SploutBenchmark benchmark = new SploutBenchmark();
		for(int i = 0; i < nIterations; i++) {
			benchmark.stressTest(nThreads, nQueries, PageCountsStressThreadImpl.class, context);
			benchmark.printStats(System.out);
		}
	}

	/**
	 * Stress thread implementation following convention from {@link SploutBenchmark}.
	 */
	public static class PageCountsStressThreadImpl extends StressThreadImpl {

		SploutClient client;
		Map<Integer, Long> rowIdsPerPartition;

		String pageToQuery = null;
		int querySequence = 0;
		int partition;

		@Override
		public void init(Map<String, Object> context) throws Exception {
			client = new SploutClient(((String) context.get("qnodes")).split(","));
			rowIdsPerPartition = (Map<Integer, Long>) context.get("rowIdsPerPartition");
		}

		@Override
		public int nextQuery() throws Exception {
			try {
				if(pageToQuery == null) {
					// State Automata: If pageToQuery is null, get a random one...
					// ... pick a random partition
					partition = (int) (Math.random() * rowIdsPerPartition.keySet().size());
					// ... pick a random rowId from the partition
					long randomRowId = (long) (Math.random() * rowIdsPerPartition.get(partition));
					// ... query a random row
					String query = "SELECT * FROM pagecounts WHERE rowid = " + randomRowId;
					QueryStatus st = client.query(TABLESPACE, null, query, partition + "");
					if(st.getResult() != null && st.getResult().size() > 0) {
						Map<String, Object> obj = (Map<String, Object>) st.getResult().get(
						    (int) (Math.random() * st.getResult().size()));
						// get the pagename
						pageToQuery = (String) obj.get("pagename");
						// some pagenames may have & characters which are not automatically encoded by SploutClient...
						pageToQuery = URLEncoder.encode(pageToQuery.replaceAll(Pattern.quote("'"), "''"), "UTF-8");
					}
					return 1;
				} else {
					// State Automata: If pageToQuery is not null, perform a GROUP BY query and set the page again to null...
					String query;
					if(querySequence == 0) {
						query = "SELECT SUM(pageviews) AS totalpageviews, date FROM pagecounts WHERE pagename = '"
						    + pageToQuery + "' GROUP BY date;";
					} else {
						query = "SELECT SUM(pageviews) AS totalpageviews, hour FROM pagecounts WHERE pagename = '"
						    + pageToQuery + "' GROUP BY hour;";
					}
					QueryStatus st = client.query(TABLESPACE, null, query, partition + "");
					querySequence++;
					if(querySequence == 2) {
						pageToQuery = null;
						querySequence = 0;
					}

					if(st.getResult() != null) {
						return st.getResult().size();
					} else {
						System.err.println("Query with no results (" + query + ") - please fix this error.");
						return 0;
//						throw new RuntimeException("Query with no results - that's impossible!");
					}
				}
			} catch(java.net.SocketTimeoutException e) {
				//
				System.err.println("More than 20 seconds... ");
				return 0;
			}
		}
	}

	public static void main(String[] args) throws InterruptedException, IOException {
		PageCountsBenchmark benchmarkTool = new PageCountsBenchmark();

		JCommander jComm = new JCommander(benchmarkTool);
		jComm.setProgramName("PageCounts Benchmark Tool");
		try {
			jComm.parse(args);
		} catch(ParameterException e) {
			System.out.println(e.getMessage());
			System.out.println();
			jComm.usage();
			System.exit(-1);
		} catch(Throwable t) {
			t.printStackTrace();
			jComm.usage();
			System.exit(-1);
		}

		benchmarkTool.start();
		System.exit(0);
	}
}
