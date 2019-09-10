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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.util.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for table that reads CSV files.
 */
public class HustleTable extends AbstractTable {
    protected final String name;
    protected final List<String> columnNames;
    protected final List<SqlTypeName> columnTypes;

    /** Creates a CsvTable. */
    HustleTable(String name, List<String> columnNames, List<SqlTypeName> columnTypes) {
        this.name = name;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        final List<RelDataType> types = new ArrayList<>();
        for (SqlTypeName type : columnTypes){
            types.add(typeFactory.createSqlType(type));
        }
        return typeFactory.createStructType(Pair.zip(columnNames, types));
    }
}
