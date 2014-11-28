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
import java.net.URISyntaxException;

public class PageCountsNameGenerator {

  public static void main(String[] args) throws URISyntaxException, IOException {
    long nNames = Long.parseLong(args[0]);

    BufferedWriter writer = new BufferedWriter(new FileWriter("names.txt"));

    long namesWritten = 0;

    char firstChar = 'A';
    while (firstChar < 'Z') {
      char secondChar = 'a';
      while (secondChar < 'z') {
        char thirdChar = 'a';
        while (thirdChar < 'z') {
          char fourthChar = 'a';
          while (fourthChar < 'z') {
//						char fifthChar = 'a';
//						while(fifthChar < 'z') {
            String name = "" + firstChar + secondChar + thirdChar + fourthChar;
            writer.write(name + "\n");
            namesWritten++;
            if (namesWritten == nNames) {
              writer.close();
              System.exit(0);
//							}
//							fifthChar++;
            }
            fourthChar++;
          }
          thirdChar++;
        }
        secondChar++;
      }
      firstChar++;
    }
  }

}
