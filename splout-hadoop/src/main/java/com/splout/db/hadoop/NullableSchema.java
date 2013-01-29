package com.splout.db.hadoop;

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

import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;

import java.util.ArrayList;
import java.util.List;

public class NullableSchema {

  /**
   * Returns a copy of the given schema, but with all
   * fields configured as nullable.
   * @param anSchema An schema
   * @return A copy of the schema, but all fields nullable.
   **/
  public static Schema nullableSchema(Schema anSchema) {
    ArrayList<Schema.Field> newFields = new ArrayList<Schema.Field>();
    List<Field> oldFields = anSchema.getFields();
    for (Field f: oldFields) {
      newFields.add(Field.cloneField(f, f.getName(), true));
    }
    return new Schema(anSchema.getName(), newFields);
  }
}
