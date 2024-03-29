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

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.sql.type.*;
import org.json.simple.*;
import org.json.simple.parser.*;

import java.io.*;
import java.util.*;
import java.util.Map;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema
 * is a CSV file in that directory.
 */
public class HustleSchema extends AbstractSchema {
    private final File catalogFile;
    private Map<String, Table> tableMap;

    public HustleSchema(File catalogFile) {
        super();
        this.catalogFile = catalogFile;
    }

    @Override
    public Map<String, Table> getTableMap() {
        if (tableMap == null) {
            tableMap = createTableMap();
        }
        return tableMap;
    }

    private Map<String, Table> createTableMap() {
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        JSONParser parser = new JSONParser();

        try (Reader reader = new FileReader(catalogFile)) {
            JSONObject jsonCatalog = (JSONObject) parser.parse(reader);

            JSONArray jsonTables = (JSONArray) jsonCatalog.get("tables");
            Iterator<JSONObject> tablesIterator = jsonTables.iterator();
            while (tablesIterator.hasNext()) {
                JSONObject jsonTable = tablesIterator.next();
                String tableName = (String) jsonTable.get("name");
//                System.out.println(tableName);
                JSONArray jsonColumns = (JSONArray) jsonTable.get("columns");
                Iterator<JSONObject>  columnsIterator = jsonColumns.iterator();
                ArrayList<String> columnNames = new ArrayList<>();
                ArrayList<SqlTypeName> columnTypes = new ArrayList<>();
                while (columnsIterator.hasNext()) {
                    JSONObject jsonColumn = columnsIterator.next();
//                    System.out.print(jsonColumn.get("name")+" - ");
                    JSONObject jsonTypeVariant = (JSONObject) jsonColumn.get("type_variant");
                    String typeVariant = (String)jsonTypeVariant.keySet().iterator().next();
//                    System.out.println(typeVariant);
                    columnNames.add((String) jsonColumn.get("name"));
                    columnTypes.add(hustleTypeToSqlType(typeVariant));
                }
//                System.out.println();
                builder.put(tableName, new HustleTable(tableName, columnNames, columnTypes));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return builder.build();
    }
    private SqlTypeName hustleTypeToSqlType(String htype) {
        //TODO(chronis): match integer precision between hustle and calcite
        String htype_lower = htype.toLowerCase();
        switch(htype_lower){
            case "int8": return SqlTypeName.TINYINT;
            case "int16": return SqlTypeName.SMALLINT;
            case "int32": return SqlTypeName.BIGINT;
            case "int64": return SqlTypeName.INTEGER;
            case "bool": return SqlTypeName.BOOLEAN;
            case "char": return SqlTypeName.VARCHAR;
            case "bits": return SqlTypeName.BINARY;
            default: return SqlTypeName.VARCHAR;
        }
    }
}
// End CsvSchema.java
