package com.splout.db.common;

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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.Progressable;

/**
 * A Heart Beater mostly inspired by the one in SolrOutputFormat PATCH
 */
public class HeartBeater extends Thread {
	public static Log LOG = LogFactory.getLog(HeartBeater.class);

	/**
	 * count of threads asking for heart beat, at 0 no heart beat done. This could be an atomic long but then missmatches
	 * in need/cancel could result in negative counts.
	 */
	volatile int threadsNeedingHeartBeat = 0;

	Progressable progress;

	public final static String WAIT_TIME_CONF = "com.splout.hearbeater.wait.time";
	/**
	 * The amount of time to wait between checks for the need to issue a heart beat. In milliseconds.
	 */
	long waitTimeMs;

	public Progressable getProgress() {
		return progress;
	}

	/**
	 * Create the heart beat object thread set it to daemon priority and start the thread. When the count in
	 * {@link #threadsNeedingHeartBeat} is positive, the heart beat will be issued on the progress object ever 5 seconds.
	 */
	public HeartBeater(Progressable progress, long waitTimeMs) {
		setDaemon(true);
		this.progress = progress;
		this.waitTimeMs = waitTimeMs;
		LOG.info("Heart beat reporting class is " + progress.getClass().getName());
		start();
	}

	public void setProgress(Progressable progress) {
		this.progress = progress;
	}

	@Override
	public void run() {
		LOG.info("HeartBeat thread running");
		while(true) {
			try {
				synchronized(this) {
					if(threadsNeedingHeartBeat > 0) {
						progress.progress();
						if(LOG.isInfoEnabled()) {
							LOG.info(String.format("Issuing heart beat for %d threads", threadsNeedingHeartBeat));
						}
					} else {
						if(LOG.isInfoEnabled()) {
							LOG.info(String.format("heartbeat skipped count %d", threadsNeedingHeartBeat));
						}
					}
					this.wait(waitTimeMs);
				}
			} catch(Throwable e) {
				LOG.error("HeartBeat throwable", e);
			}
		}
	}

	/**
	 * inform the background thread that heartbeats are to be issued. Issue a heart beat also
	 */
	public synchronized void needHeartBeat() {
		threadsNeedingHeartBeat++;
		// Issue a progress report right away,
		// just in case the the cancel comes before the background thread issues a
		// report.
		// If enough cases like this happen the 600 second timeout can occur
		progress.progress();
		if(threadsNeedingHeartBeat == 1) {
			// this.notify(); // wake up the heartbeater
		}
	}

	/**
	 * inform the background thread that this heartbeat request is not needed. This must be called at some point after
	 * each {@link #needHeartBeat()} request.
	 */
	public synchronized void cancelHeartBeat() {
		if(threadsNeedingHeartBeat > 0) {
			threadsNeedingHeartBeat--;
		} 
	}

	@SuppressWarnings("rawtypes")
	public void setStatus(String status) {
		if(progress instanceof TaskInputOutputContext) {
			((TaskInputOutputContext) progress).setStatus(status);
		}
	}
}
