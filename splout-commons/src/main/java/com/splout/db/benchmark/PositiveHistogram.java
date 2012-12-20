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
 * A class for computing approximated histograms, keeping a constant resolution, constant amount of memory, and without
 * the needed of pre or post processing.
 * 
 * WARNING: It only works for positive values. And if they are distributed far from 0, then will not work properly as
 * well.
 */
public class PositiveHistogram {

	protected int buckets[];
	protected double upperLimit;
	protected long count = 0;

	/**
	 * 2^bits will be number of buckets. If known, it is useful to provide an initialUpperLimit. If it is not known, just
	 * set it as 0
	 */
	public PositiveHistogram(int bits, double initialUpperLimit) {
		// Bits is only used to enforce that buckets are 2^k 
		buckets = new int[1 << (bits - 1)];
		if(initialUpperLimit <= 0) {
			throw new IllegalArgumentException("Upper limit must be > 0");
		}
		this.upperLimit = initialUpperLimit;
	}

	/**
	 * Add a new value to the histogram
	 */
	public synchronized void add(double value) {
		if(value < 0.) {
			throw new RuntimeException("Negatives values not allowed. Provided " + value);
		}

		if(value > upperLimit) {
			resize(value);
		}

		buckets[bucketFor(value)] += 1.;
		count++;
	}

	protected synchronized void resize(double value) {
		int scale = (int) Math.ceil(value / upperLimit);
		int[] newBuckets = new int[buckets.length];

		for(int i = 0; i < buckets.length; i++) {
			int toBucket = (i / scale);
			newBuckets[toBucket] += buckets[i];
		}

		buckets = newBuckets;
		upperLimit = upperLimit * scale;
	}

	/**
	 * Return the bucket index for a given value.
	 */
	public synchronized int bucketFor(Double value) {
		return Math.min(buckets.length - 1, (int) ((value / upperLimit) * buckets.length));
	}

	/**
	 * Returns the maximum value that can be keep if the current histogram without needing to redistribute the buckets.
	 */
	public synchronized double getUpperLimit() {
		return upperLimit;
	}

	/**
	 * Return the list of buckets. They represent the values between 0 and {@link #getUpperLimit()}
	 */
	public synchronized int[] getBuckets() {
		return buckets;
	}

	/**
	 * Return the total sum of the values of the buckets. Normally, they will be the number of elements. If normalized,
	 * then it will be 1.
	 */
	private synchronized double getTotalSum() {
		double ret = 0;
		for(int i = 0; i < buckets.length; i++) {
			ret += buckets[i];
		}
		return ret;
	}

	/**
	 * Returns the number of elements that was added to the histogram.
	 */
	public synchronized long getCount() {
		return count;
	}

	/**
	 * Computes the accumulated probability for the values at the right of the provided value
	 */
	public synchronized double getRigthAccumulatedProbability(double value) {
		if(value >= upperLimit) {
			return 0;
		}

		int bucket = bucketFor(value);
		double accum = 0.;
		double bucketRedistribution;

		// We only get some probability from the first bucket.
		// As much as if data inside buckets would be evenly distributed.
		double doubleBucket = (value / upperLimit) * buckets.length;
		double ceilBucket = Math.ceil(doubleBucket);
    // Strange case where ceil(doubleBucket) == doubleBucket, because doubleBucket is a pure integer
    if (ceilBucket == doubleBucket) {
      ceilBucket += 1;
    }
		bucketRedistribution = ceilBucket - doubleBucket;

		accum += bucketRedistribution * buckets[bucket];

		for(int i = bucket + 1; i < buckets.length; i++) {
			accum += buckets[i];
		}

		return accum / getTotalSum();
	}

	/**
	 * Computes the accumulated probability for the values at the left of the provided value
	 */
	public synchronized double getLeftAccumulatedProbability(double value) {
		return 1. - getRigthAccumulatedProbability(value);
	}

	public synchronized double[] getNormalizedHistogram() {
		double[] histo = new double[buckets.length];

		double totalSum = getTotalSum();
		for(int i = 0; i < buckets.length; i++) {
			histo[i] = buckets[i] / totalSum;
		}

		return histo;
	}

	public synchronized double getBucketSize() {
		return upperLimit / (double)buckets.length;
	}
	
	public String toTSV() {
		StringBuilder sb = new StringBuilder();
		double bucketSize = getBucketSize();
		for(int i = 0; i < buckets.length; i++) {
			sb.append((bucketSize * i) + (bucketSize / 2));
			sb.append("\t");
			sb.append(buckets[i]);
			sb.append("\n");
		}
		return sb.toString();
	}
}