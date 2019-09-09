package org.apache.calcite.test;


import org.apache.calcite.adapter.hustle.*;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.util.Util;

import java.util.*;

class Triple {
    public String s;
    public String p;
    public String o;

    public Triple(String s, String p, String o) {
        // super();
        this.s = s;
        this.p = p;
        this.o = o;
    }
}

public class TestOne {

    public static class TestSchema {
        public final Triple[] rdf = {new Triple("s", "p", "o")};
    }

    public static void main(String[] args) {
        SchemaPlus schemaPlus = Frameworks.createRootSchema(true);

        // add table to schema T
//        schemaPlus.add("T", new ReflectiveSchema(new TestSchema()));

        //figure out how to get this from a config file
        Map<String, Object> operandMap = new HashMap<>();
        operandMap.put("catalogFile", "/Users/chronis/code/hustle/catalog.json");
        HustleSchema hs = (HustleSchema) HustleSchemaFactory.INSTANCE.create(schemaPlus, "HT", operandMap);
        hs.getTableMap();
        schemaPlus.add("H", hs);


        FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .defaultSchema(schemaPlus)
                .build();

        SqlParser.ConfigBuilder paresrConfig = SqlParser.configBuilder(frameworkConfig.getParserConfig());

        //SQL case insensitive
        paresrConfig.setCaseSensitive(false).setConfig(paresrConfig.build());

        Planner planner = Frameworks.getPlanner(frameworkConfig);

        SqlNode sqlNode;
        RelRoot relRoot = null;

        try {
            // parser
            // sqlNode = planner.parse("select \"a\".\"s\", count(\"a\".\"s\") from \"T\".\"rdf\" \"a\" group by \"a\".\"s\"");
            // sqlNode = planner.parse("select A.\"s\", count(A.\"s\") from \"T\".\"rdf\" A, \"T\".\"rdf\" B group by A.\"s\"");
            sqlNode = planner.parse("select * from \"H\".\"customer\"");
            System.out.println(sqlNode.toString());

            // validate
            planner.validate(sqlNode);

            // get root node of the rel tree
            relRoot = planner.rel(sqlNode);
        } catch (Exception e) {
            e.printStackTrace();
        }

        RelNode relNode = relRoot.project();
        // System.out.print(RelOptUtil.toString(relNode));
        System.out.print(RelOptUtil.dumpPlan("", relNode, SqlExplainFormat.JSON, SqlExplainLevel.NO_ATTRIBUTES));
    }
}





