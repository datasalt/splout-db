package com.splout.db.benchmark;

/*
 * #%L
 * Splout SQL Server
 * %%
 * Copyright (C) 2012 Datasalt Systems S.L.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

/**
 * A simple tool that can measure the performance of queries using {@link HistogramWithStats}.
 */
public class PerformanceTool {

  private HistogramWithStats histogram;
  private ThreadLocal<Long> startTime = new ThreadLocal<Long>() {
    protected Long initialValue() {
      return System.currentTimeMillis();
    }

    ;
  };

  public PerformanceTool() {
    histogram = new HistogramWithStats();
  }

  public void startQuery() {
    startTime.set(System.currentTimeMillis());
  }

  public long endQuery() {
    long time = System.currentTimeMillis() - startTime.get();
    histogram.add(time);
    return time;
  }

  public int getNQueries() {
    return (int) histogram.getCount();
  }

  public double getAverage() {
    return histogram.getAverage();
  }

  public HistogramWithStats getHistogram() {
    return histogram;
  }
}
