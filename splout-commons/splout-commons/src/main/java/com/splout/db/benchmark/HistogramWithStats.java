package com.splout.db.benchmark;

/*
 * #%L
 * Splout SQL commons
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

/**
 * A {@link PositiveHistogram} but that also keeps average, stdev, min and max values.
 */
public class HistogramWithStats extends PositiveHistogram {
	
	private double squares = 0;
	private double accum = 0;
	private double min = Double.MAX_VALUE;
	private double max = Double.MIN_VALUE;

	public HistogramWithStats(int bits, double initialUpperLimit) {
	  super(bits, initialUpperLimit);
  }
	
	public HistogramWithStats(int bits) {
		// Histogram upper limit is enforced by the number of bits.
		// This way, ranges in the histogram will always be integers.
		this(bits, (int)Math.pow(2, bits - 1));
	}
	
	public HistogramWithStats() {
		super(8, 1);
	}
	
	/*
	 * Deep copy constructor
	 */
	public HistogramWithStats(HistogramWithStats copy) {
		super(1, copy.upperLimit);
		buckets = new int[copy.buckets.length];
		for(int i = 0; i < copy.buckets.length; i++) {
			this.buckets[i] = copy.buckets[i];
		}
		count = copy.count;
		squares = copy.squares;
		accum = copy.accum;
		min = copy.min;
		max = copy.max;
	}

	@Override
  public synchronized void add(double value) {
	  super.add(value);
	  
	  accum += value;
	  squares += (value * value);
	  
	  min = Math.min(min, value);
	  max = Math.max(max, value);
  }
	
	public synchronized double getAverage() {
		return accum / getCount();
	}
	
	public synchronized double getAccum() {
		return accum;
	}
	
	/**
	 * Returns the stdev. 
	 */
	public synchronized double getStdev() {
		return Math.sqrt((squares / getCount()) - Math.pow(getAverage(), 2));
	}

	/**
	 * Return the maximun value
	 */
	public synchronized double getMax() {
		return max;
	}
	
	/**
	 * Return the minimun value
	 */
	public synchronized double getMin() {
		return min;
	}
}