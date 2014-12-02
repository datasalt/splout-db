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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class PageCountsUrlGenerator {

  public static void main(String[] args) throws URISyntaxException, IOException {

//		final String[] hosts = { "10.244.43.226:4412", "10.194.247.126:4412" };

    final String[] hosts = {"localhost:4412"};

    BufferedWriter writer = new BufferedWriter(new FileWriter("urls.txt"));

    char firstChar = 'A';
    while (firstChar < 'Z') {
      char secondChar = 'a';
      while (secondChar < 'z') {
        char thirdChar = 'a';
        while (thirdChar < 'z') {
          char fourthChar = 'a';
          while (fourthChar < 'd') {
            for (String host : hosts) {
              String key = "" + firstChar + secondChar + thirdChar + fourthChar;
              String query = "SELECT * FROM pagecounts WHERE pagename LIKE '" + key + "%' LIMIT 10";
              String url = new URI("http", host, "/api/query/pagecounts", "key=" + key + "&sql=" + query, null).toASCIIString();
              writer.write(url + "\n");
              query = "SELECT hour, SUM(pageviews), AVG(pageviews) FROM (SELECT * FROM pagecounts WHERE pagename LIKE '" + key + "%' LIMIT 100) GROUP BY hour;";
              url = new URI("http", host, "/api/query/pagecounts", "key=" + key + "&sql=" + query, null).toASCIIString();
              writer.write(url + "\n");
            }
            fourthChar++;
          }
          thirdChar++;
        }
        secondChar++;
      }
      firstChar++;
    }

    writer.close();
  }
}