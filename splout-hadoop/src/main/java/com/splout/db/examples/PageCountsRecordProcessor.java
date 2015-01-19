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

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.splout.db.hadoop.CounterInterface;
import com.splout.db.hadoop.RecordProcessor;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 * Custom record processor that filters out some records and URL-Decodes a field. Made for the Wikipedia pagecounts
 * dataset example.
 */
@SuppressWarnings("serial")
public class PageCountsRecordProcessor extends RecordProcessor {

  // The resulting Table Tuple
  private ITuple tuple;

  public PageCountsRecordProcessor(Schema pageCountsSchema, String date, String hour) {
    this.tuple = new Tuple(pageCountsSchema);
    this.tuple.set("date", date);
    this.tuple.set("hour", hour);
  }

  @Override
  public ITuple process(ITuple record, CounterInterface context) throws Throwable {
    // Filter out records that are not from English Wikipedia
    if (!record.get("projectcode").toString().equals("en")) {
      // count for stats
      context.getCounter("stats", "noEnglishWikipedia").increment(1);
      return null;
    }
    try {
      // URL-Denormalize the pagenames
      String pageName = record.get("pagename").toString();
      record.set("pagename", decode(pageName));
    } catch (Throwable t) {
      if (t.getMessage() != null && t.getMessage().contains("URLDecoder: ")) {
        // There are some illegal characters around, we can filter those entries.
        // The exception is not very friendly (IllegalArgumentException) so we have to catch
        // uncatched exceptions here...
        // count for stats
        context.getCounter("stats", "illegalCharacters").increment(1);
        return null;
      } else {
        throw t;
      }
    }
    // Set the fields we are interested in
    // Since we don't care about the bytes field (it's not in the Table schema), we skip it
    tuple.set("pagename", record.get("pagename"));
    tuple.set("pageviews", record.get("pageviews"));
    // return the Tuple
    return tuple;
  }

  public static String decode(String str) throws UnsupportedEncodingException {
    return URLDecoder.decode(str, "UTF-8");
  }
}
