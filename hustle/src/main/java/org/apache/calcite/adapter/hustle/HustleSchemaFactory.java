/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.hustle;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.util.*;

@SuppressWarnings("UnusedDeclaration")
public class HustleSchemaFactory implements SchemaFactory {
    /** Public singleton, per factory contract. */
    public static final HustleSchemaFactory INSTANCE = new HustleSchemaFactory();

    private HustleSchemaFactory() {}

    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
       final String catalogFilePath = (String) operand.get("catalog_file");
       File catalogFile = new File(catalogFilePath);
       return new HustleSchema(catalogFile);
    }
}
