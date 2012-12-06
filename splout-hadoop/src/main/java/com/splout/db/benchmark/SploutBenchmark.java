package com.splout.db.benchmark;

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

import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generic benchmark implementation for Splout. Subclasses of {@link StressThreadImpl} must implement specialized SQL business logic.
 */
public class SploutBenchmark {

	private long totalMillis;
	private long reqPerSecond;
	private long totalRows;
	private int averageRows;
	private double average;
	private double max, min;
	private double stdev, median, percen90;
	private double gt10, gt50, gt100, gt250, gt500, gt1000, gt2500;
	
	/**
	 * Subclasses of this class can be used for performing arbitrary benchmarks. They must perform queries when requested by {@link #nextQuery()} method.
	 */
	public static abstract class StressThreadImpl {

		public abstract void init(Map<String, String> context) throws Exception;

		public abstract int nextQuery() throws Exception; // Performs the query and returns the number of rows processed
	}

	public void stressTest(final int nThreads, final int nQueries, 
	    final Class<? extends StressThreadImpl> stressThreadClass, final Map<String, String> context)
	    throws InterruptedException {
		ExecutorService service = Executors.newFixedThreadPool(nThreads);

		final HistogramWithStats histo = new HistogramWithStats();
		final AtomicLong totalRows = new AtomicLong(0);
		long startTime = System.currentTimeMillis();
		/*
		 * Instantiate pool of threads
		 */
		for(int k = 0; k < nThreads; k++) {
			final int threadId = k;
			service.submit(new Runnable() {

				@Override
				public void run() {
					try {
						/*
						 * Run a StressThreadImpl inside each Thread
						 */
						StressThreadImpl thisThread = stressThreadClass.newInstance();
						// init
						thisThread.init(context);
						for(int i = 0; i < nQueries; i++) {
							if(i % nThreads == threadId) { // distribute work
								long start = System.currentTimeMillis();
								int rowCount = thisThread.nextQuery();
								long end = System.currentTimeMillis();
								histo.add((end - start)); // add stat to histogram
								totalRows.addAndGet(rowCount); // add total number of rows processed
							}
						}
					} catch(Throwable t) {
						t.printStackTrace();
						throw new RuntimeException(t);
					}
				}
			});
		}
		service.shutdown();
		while(!service.isTerminated()) {
			service.awaitTermination(500, TimeUnit.MILLISECONDS);
		}
		totalMillis = System.currentTimeMillis() - startTime;
		/*
		 * Calculate and save stats for this run
		 */
		this.totalRows = totalRows.get();
		averageRows = (int) (this.totalRows / nQueries);
		average = histo.getAverage();
		max = histo.getMax();
		min = histo.getMin();
		stdev = histo.getStdev();
		median = -1;
		percen90 = -1;
		for(int i = (int) histo.getMin(); i <= histo.getMax(); i++) {
			if(median == -1 && histo.getLeftAccumulatedProbability(i) > 0.5) {
				median = i;
			} 
			if(percen90 == -1 && histo.getLeftAccumulatedProbability(i) > 0.9) {
				percen90 = i;
			}
		}
		gt10 = histo.getRigthAccumulatedProbability(10);
		gt50 = histo.getRigthAccumulatedProbability(50);
		gt100 = histo.getRigthAccumulatedProbability(100);
		gt250 = histo.getRigthAccumulatedProbability(250);
		gt500 = histo.getRigthAccumulatedProbability(500);
		gt1000 = histo.getRigthAccumulatedProbability(1000);
		gt2500 = histo.getRigthAccumulatedProbability(2500);
		reqPerSecond = nQueries / (totalMillis / 1000);
	}
	
	public void printStats(PrintStream outStream) {
		outStream.println("Average query time\t" + average);
		outStream.println("Max\t" + max);
		outStream.println("Min\t" + min);
		outStream.println("Stdev\t" + stdev);
		outStream.println("Median\t" + median);
		outStream.println("90 percentil\t" + percen90);
		outStream.println("p > 10ms\t" + gt10);
		outStream.println("p > 50ms\t" + gt50);
		outStream.println("p > 100ms\t" + gt100);
		outStream.println("p > 250ms\t" + gt250);
		outStream.println("p > 500ms\t" + gt500);
		outStream.println("p > 1000ms\t" + gt1000);
		outStream.println("p > 2500ms\t" + gt2500);
		outStream.println("Requests per second\t" + reqPerSecond);
		outStream.println("Total rows\t" + totalRows);
		outStream.println("Average rows\t" + averageRows);
	}
	
	// ------- Getters -------- //
	
	/**
	 * After performing the benchmark, the total number of rows hit
	 */
	public long getTotalRows() {
  	return totalRows;
  }
	/**
	 * The average number of rows hit per query
	 */
	public int getAverageRows() {
  	return averageRows;
  }
	/**
	 * The average query response time
	 */
	public double getAverage() {
  	return average;
  }
	/**
	 * The maximum query response time
	 */
	public double getMax() {
  	return max;
  }
	/**
	 * The minimum query response time
	 */
	public double getMin() {
  	return min;
  }
	/**
	 * The standard deviation of the response time
	 */
	public double getStdev() {
  	return stdev;
  }
	/**
	 * The median query response time
	 */
	public double getMedian() {
  	return median;
  }
	/**
	 * The 90 percentil query response time
	 */
	public double getPercen90() {
  	return percen90;
  }
	/**
	 * The frequency of queries above 10 milliseconds
	 */
	public double getGt10() {
  	return gt10;
  }
	/**
	 * The frequency of queries above 50 milliseconds
	 */
	public double getGt50() {
  	return gt50;
  }
	/**
	 * The frequency of queries above 100 milliseconds
	 */
	public double getGt100() {
  	return gt100;
  }
	/**
	 * The frequency of queries above 250 milliseconds
	 */
	public double getGt250() {
  	return gt250;
  }
	/**
	 * The frequency of queries above 500 milliseconds
	 */
	public double getGt500() {
  	return gt500;
  }
	/**
	 * The frequency of queries above 1000 milliseconds
	 */
	public double getGt1000() {
  	return gt1000;
  }
	/**
	 * The frequency of queries above 2500 milliseconds
	 */
	public double getGt2500() {
  	return gt2500;
  }
}
