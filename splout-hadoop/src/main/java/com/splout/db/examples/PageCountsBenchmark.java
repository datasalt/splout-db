package com.splout.db.examples;

import java.util.HashMap;
import java.util.Map;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.splout.db.benchmark.SploutBenchmark;
import com.splout.db.benchmark.SploutBenchmark.StressThreadImpl;
import com.splout.db.common.SploutClient;
import com.splout.db.qnode.beans.QueryStatus;

/**
 * A benchmark designed ot stress-test Splout with the Wikipedia Page Counts dataset. Each thread will do an
 * auto-complete query (WHERE pagename LIKE 'Aa%'), select a random result from it and in the next query perform an
 * aggregation with GROUP BY just like a normal user of the demo.
 */
public class PageCountsBenchmark {

	@Parameter(required = true, names = { "-q", "--qnodes" }, description = "Comma-separated list QNode addresses.")
	private String qNodes;

	@Parameter(names = { "-n", "--niterations" }, description = "The number of iterations for running the benchmark more than once.")
	private Integer nIterations = 1;

	@Parameter(names = { "-nth", "--nthreads" }, description = "The number of threads to use for the test.")
	private Integer nThreads = 1;

	@Parameter(required = true, names = { "-nq", "--nqueries" }, description = "The number of queries to perform for the benchmark.")
	private Integer nQueries;

	public void start() throws InterruptedException {
		Map<String, String> context = new HashMap<String, String>();
		context.put("qnodes", qNodes);
		SploutBenchmark benchmark = new SploutBenchmark();
		for(int i = 0; i < nIterations; i++) {
			benchmark.stressTest(nThreads, nQueries, PageCountsStressThreadImpl.class, context);
			benchmark.printStats(System.out);
		}
	}

	/**
	 * Stress thread implementation following convention from {@link SploutBenchmark}.
	 * <p>
	 * Each thread will do an auto-complete query (WHERE pagename LIKE 'Aa%'), select a random result from it and in the
	 * next query perform an aggregation with GROUP BY just like a normal user of the demo.
	 */
	public static class PageCountsStressThreadImpl extends StressThreadImpl {

		SploutClient client;
		final String tablespace = "pagecounts";
		final int nCharsPrefix = 3;

		String pageToQuery = null;

		int querySequence = 0;
		
		@Override
		public void init(Map<String, String> context) throws Exception {
			client = new SploutClient(context.get("qnodes").split(","));
		}

		// Return a random three-character prefix for auto-suggest
		char[] randomPrefix() {
			char[] pref = new char[nCharsPrefix];
			for(int i = 0; i < nCharsPrefix; i++) {
				char c = (char) ('a' + (int) (Math.random() * 24));
				if(i == 0) {
					c = Character.toUpperCase(c);
				}
				pref[i] = c;
			}
			return pref;
		}

		@SuppressWarnings("unchecked")
    @Override
		public int nextQuery() throws Exception {
			if(pageToQuery == null) {
				// State Automata: If pageToQuery is null, get a random one from an auto-suggest query...
				String pref = new String(randomPrefix());
				String query = "SELECT * FROM pagecounts WHERE pagename LIKE '" + pref + "%' LIMIT 10";
				QueryStatus st = client.query(tablespace, pref.substring(0, Math.min(pref.length(), 2)), query);
				if(st.getResult() != null && st.getResult().size() > 0) {
					Map<String, Object> obj = (Map<String, Object>) st.getResult().get(
					    (int) (Math.random() * st.getResult().size()));
					pageToQuery = (String) obj.get("pagename");
				}
				if(st.getResult() != null) {
					return st.getResult().size();
				} else {
					return 0;
				}
			} else {
				// State Automata: If pageToQuery is not null, perform a GROUP BY query and set the page again to null...
				String query ;
				if(querySequence == 0) {
					query = "SELECT SUM(pageviews) AS totalpageviews, date FROM pagecounts WHERE pagename = '"
				    + pageToQuery + "' GROUP BY date;";
				} else {
					query = "SELECT SUM(pageviews) AS totalpageviews, hour FROM pagecounts WHERE pagename = '" 
						+ pageToQuery + "' GROUP BY hour;";					
				}
				QueryStatus st = client.query(tablespace, pageToQuery.substring(0, Math.min(pageToQuery.length(), 2)), query);
				querySequence++;
				if(querySequence == 2) {
					pageToQuery = null;
					querySequence = 0;
				}
				if(st.getResult() != null) {
					return st.getResult().size();
				} else {
					return 0;
				}
			}
		}
	}
	
	public static void main(String[] args) throws InterruptedException {
		PageCountsBenchmark benchmarkTool = new PageCountsBenchmark();
		
		JCommander jComm = new JCommander(benchmarkTool);
		jComm.setProgramName("PageCounts Benchmark Tool");
		try {
			jComm.parse(args);
		} catch (ParameterException e){
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
