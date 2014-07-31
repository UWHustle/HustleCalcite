/*
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.impl.tpcds;

import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaFactory;
import net.hydromatic.optiq.SchemaPlus;

import org.eigenbase.util.Util;

import java.util.Map;

/**
 * Factory that creates a {@link TpcdsSchema}.
 *
 * <p>Allows a custom schema to be included in a model.json file.</p>
 */
@SuppressWarnings("UnusedDeclaration")
public class TpcdsSchemaFactory implements SchemaFactory {
  // public constructor, per factory contract
  public TpcdsSchemaFactory() {
  }

  public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    Map map = (Map) operand;
    double scale = Util.first((Double) map.get("scale"), 1D);
    int part = Util.first((Integer) map.get("part"), 1);
    int partCount = Util.first((Integer) map.get("partCount"), 1);
    boolean columnPrefix = Util.first((Boolean) map.get("columnPrefix"), true);
    return new TpcdsSchema(scale, part, partCount);
  }
}

// End TpcdsSchemaFactory.java
