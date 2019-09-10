package org.apache.calcite.test;


import org.apache.calcite.adapter.hustle.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import java.util.*;

public class EndToEndTest {

    public static void main(String[] args) {
        SchemaPlus schemaPlus = Frameworks.createRootSchema(true);
        Map<String, Object> operandMap = new HashMap<>();
        String catalogPath = EndToEndTest.class.getClassLoader().getResource("ssb_catalog.json").getFile();
        operandMap.put("catalog_file", catalogPath);
        HustleSchema hc = (HustleSchema) HustleSchemaFactory.INSTANCE.create(schemaPlus, "hc", operandMap);
        schemaPlus.add("ssb", hc);

        FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .defaultSchema(schemaPlus)
                .build();

        SqlParser.ConfigBuilder paresrConfig = SqlParser.configBuilder(frameworkConfig.getParserConfig());

        Planner planner = Frameworks.getPlanner(frameworkConfig);

        SqlNode sqlNode = null;
        RelRoot relRoot = null;
        RelNode relOpt = null;

        try {
//            String sqlquery = "SELECT \"cf\".\"numberx\" FROM \"tatp\".\"Special_Facility\" AS \"sf\", \"tatp\".\"Call_Forwarding\" AS \"cf\" WHERE (\"sf\".\"s_id\" = 5 AND \"sf\".\"sf_type\" = 3 AND \"sf\".\"is_active\" = 1) AND (\"cf\".\"s_id\" = \"sf\".\"s_id\" AND \"cf\".\"sf_type\" = \"sf\".\"sf_type\") AND (\"cf\".\"start_time\" <= 1 AND 2 < \"cf\".\"end_time\")";
//            String sqlquery = "SELECT \"s_id\", \"sub_nbr\", \"bit_1\", \"bit_2\", \"bit_3\", \"bit_4\", \"bit_5\", \"bit_6\", \"bit_7\", \"bit_8\", \"bit_9\", \"bit_10\", \"hex_1\", \"hex_2\", \"hex_3\", \"hex_4\", \"hex_5\", \"hex_6\", \"hex_7\", \"hex_8\", \"hex_9\", \"hex_10\", \"byte2_1\", \"byte2_2\", \"byte2_3\", \"byte2_4\", \"byte2_5\", \"byte2_6\", \"byte2_7\", \"byte2_8\", \"byte2_9\", \"byte2_10\", \"msc_location\", \"vlr_location\" FROM \"tatp\".\"Subscriber\" WHERE \"s_id\" = 5";
            String sqlquery = "SELECT SUM(\"lineorder\".\"lo_extendedprice\"*\"lineorder\".\"lo_discount\") AS \"revenue\"\n" +
                    "FROM \"ssb\".\"lineorder\", \"ssb\".\"ddate\"\n" +
                    "WHERE \"lineorder\".\"lo_orderdate\" = \"ddate\".\"d_datekey\"\n" +
                    "  AND \"ddate\".\"d_year\" = 1993\n" +
                    "  AND \"lineorder\".\"lo_discount\" BETWEEN 1 AND 3\n" +
                    "  AND \"lineorder\".\"lo_quantity\" < 25\n";
//        String sqlquery = "SELECT \"cf\".\"numberx\" FROM \"tatp\".\
            System.out.println(sqlquery);
            sqlNode = planner.parse(sqlquery);//"select c_custkey, \"part\".\"p_partkey\" from \"ssb\".\"customer\", \"ssb\".\"part\" where \"part\".\"p_partkey\" = \"customer\".\"c_custkey\"");

            // validate
            planner.validate(sqlNode);

            // get root node of the rel tree
            relRoot = planner.rel(sqlNode);

            // relOpt = planner.transform(0, RelTraitSet.createEmpty(), relRoot.rel);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.print(RelOptUtil.toString(relRoot.rel));
        System.out.println("###___####____####___####___###");
        System.out.print(sqlNode.toSqlString(CalciteSqlDialect.DEFAULT));
        // System.out.print(RelOptUtil.dumpPlan("", relNode, SqlExplainFormat.JSON, SqlExplainLevel.NO_ATTRIBUTES));
    }

}





