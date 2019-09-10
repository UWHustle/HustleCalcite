package org.apache.calcite.test;

import org.apache.calcite.adapter.hustle.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.schema.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.tools.*;
import org.junit.*;

import java.util.*;

import static org.junit.Assert.*;

public class TATPTest {
    private Planner planner_;

    private void ComparePlans(String sqlQuery, String expectedPlan) {
        SqlNode sqlNode = null;
        RelRoot relRoot = null;
        String plan = "";
        try {
            // Parse
            sqlNode = planner_.parse(sqlQuery);
            // Validate
            planner_.validate(sqlNode);
            // Convert to Logical Plan
            relRoot = planner_.rel(sqlNode);
            plan = RelOptUtil.toString(relRoot.rel);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        Assert.assertEquals(plan, expectedPlan);
    }
    @Before
    public void SetUp() {
        SchemaPlus schemaPlus = Frameworks.createRootSchema(true);
        Map<String, Object> operandMap = new HashMap<>();
        String catalogPath = EndToEndTest.class.getClassLoader().getResource("tatp_catalog.json").getFile();
        operandMap.put("catalog_file", catalogPath);
        HustleSchema hc = (HustleSchema) HustleSchemaFactory.INSTANCE.create(schemaPlus, "hc", operandMap);
        schemaPlus.add("tatp", hc);

        FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .defaultSchema(schemaPlus)
                .build();

        planner_ = Frameworks.getPlanner(frameworkConfig);
    }

    @Test
    public void q1() {
        String sqlQuery =
                "SELECT \"s_id\", \"sub_nbr\", \"bit_1\", \"bit_2\", \"bit_3\", \"bit_4\", \"bit_5\"," +
                " \"bit_6\", \"bit_7\", \"bit_8\", \"bit_9\", \"bit_10\", \"hex_1\", \"hex_2\", \"hex_3\", \"hex_4\"," +
                " \"hex_5\", \"hex_6\", \"hex_7\", \"hex_8\", \"hex_9\", \"hex_10\", \"byte2_1\", \"byte2_2\"," +
                " \"byte2_3\", \"byte2_4\", \"byte2_5\", \"byte2_6\", \"byte2_7\", \"byte2_8\", \"byte2_9\"," +
                " \"byte2_10\", \"msc_location\", \"vlr_location\"" +
                " FROM \"tatp\".\"Subscriber\"" +
                " WHERE \"s_id\" = 5";


        String expectedPlan =
                "LogicalProject(s_id=[$0], sub_nbr=[$1], bit_1=[$2], bit_2=[$3], bit_3=[$4], bit_4=[$5], bit_5=[$6], bit_6=[$7], bit_7=[$8], bit_8=[$9], bit_9=[$10], bit_10=[$11], hex_1=[$12], hex_2=[$13], hex_3=[$14], hex_4=[$15], hex_5=[$16], hex_6=[$17], hex_7=[$18], hex_8=[$19], hex_9=[$20], hex_10=[$21], byte2_1=[$22], byte2_2=[$23], byte2_3=[$24], byte2_4=[$25], byte2_5=[$26], byte2_6=[$27], byte2_7=[$28], byte2_8=[$29], byte2_9=[$30], byte2_10=[$31], msc_location=[$32], vlr_location=[$33])\n" +
                        "  LogicalFilter(condition=[=(CAST($0):INTEGER NOT NULL, 5)])\n" +
                        "    EnumerableTableScan(table=[[tatp, Subscriber]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q2() {
        String sqlQuery =
                "SELECT \"cf\".\"numberx\"" +
                " FROM \"tatp\".\"Special_Facility\" AS \"sf\", \"tatp\".\"Call_Forwarding\" AS \"cf\"" +
                " WHERE (\"sf\".\"s_id\" = 5 AND \"sf\".\"sf_type\" = 3 AND \"sf\".\"is_active\" = 1) " +
                "AND (\"cf\".\"s_id\" = \"sf\".\"s_id\" AND \"cf\".\"sf_type\" = \"sf\".\"sf_type\") AND " +
                    "(\"cf\".\"start_time\" <= 1 AND 2 < \"cf\".\"end_time\")";

        String expectedPlan =
                "LogicalProject(numberx=[$10])\n" +
                        "  LogicalFilter(condition=[AND(=(CAST($0):INTEGER NOT NULL, 5), =(CAST($1):INTEGER NOT NULL, 3), =(CAST($2):INTEGER NOT NULL, 1), =($6, $0), =($7, $1), <=($8, 1), <(2, $9))])\n" +
                        "    LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "      EnumerableTableScan(table=[[tatp, Special_Facility]])\n" +
                        "      EnumerableTableScan(table=[[tatp, Call_Forwarding]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q3_1() {
        String sqlQuery =
                "INSERT INTO \"tatp\".\"Special_Facility\" VALUES(17, 4, 1, 5, 5, 'ABCDE')\n";

        String expectedPlan =
                "LogicalTableModify(table=[[tatp, Special_Facility]], operation=[INSERT], flattened=[true])\n" +
                        "  LogicalValues(tuples=[[{ 17, 4, 1, 5, 5, 'ABCDE' }]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q3_2() {
        String sqlQuery =
                "INSERT INTO \"tatp\".\"Call_Forwarding\" VALUES(17, 4, 7, 8, 'ABCDEFGHIJKLMNO')\n";

        String expectedPlan =
                "LogicalTableModify(table=[[tatp, Call_Forwarding]], operation=[INSERT], flattened=[true])\n" +
                        "  LogicalValues(tuples=[[{ 17, 4, 7, 8, 'ABCDEFGHIJKLMNO' }]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q3_3() {
        String sqlQuery =
                "SELECT \"cf\".\"numberx\" FROM \"tatp\".\"Special_Facility\" AS \"sf\", \"tatp\".\"Call_Forwarding\" AS \"cf\" WHERE (\"sf\".\"s_id\" = 17 AND \"sf\".\"sf_type\" = 4 AND \"sf\".\"is_active\" = 1) AND (\"cf\".\"s_id\" = \"sf\".\"s_id\" AND \"cf\".\"sf_type\" = \"sf\".\"sf_type\") AND (\"cf\".\"start_time\" <= 8 AND 1 < \"cf\".\"end_time\")\n";

        String expectedPlan =
                "LogicalProject(numberx=[$10])\n" +
                        "  LogicalFilter(condition=[AND(=(CAST($0):INTEGER NOT NULL, 17), =(CAST($1):INTEGER NOT NULL, 4), =(CAST($2):INTEGER NOT NULL, 1), =($6, $0), =($7, $1), <=($8, 8), <(1, $9))])\n" +
                        "    LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "      EnumerableTableScan(table=[[tatp, Special_Facility]])\n" +
                        "      EnumerableTableScan(table=[[tatp, Call_Forwarding]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q4() {
        String sqlQuery =
                "SELECT \"data1\", \"data2\", \"data3\", \"data4\" FROM \"tatp\".\"Access_Info\" WHERE \"s_id\" = 1 AND \"ai_type\" = 1\n";

        String expectedPlan =
                "LogicalProject(data1=[$2], data2=[$3], data3=[$4], data4=[$5])\n" +
                        "  LogicalFilter(condition=[AND(=(CAST($0):INTEGER NOT NULL, 1), =(CAST($1):INTEGER NOT NULL, 1))])\n" +
                        "    EnumerableTableScan(table=[[tatp, Access_Info]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q5_1() {
        String sqlQuery =
                "INSERT INTO \"tatp\".\"Access_Info\" VALUES(22, 4, 7, 8, 'AAA', 'AAAAA')\n";

        String expectedPlan =
                "LogicalTableModify(table=[[tatp, Access_Info]], operation=[INSERT], flattened=[true])\n" +
                        "  LogicalValues(tuples=[[{ 22, 4, 7, 8, 'AAA', 'AAAAA' }]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q5_2() {
        String sqlQuery =
                "SELECT \"data1\", \"data2\", \"data3\", \"data4\" FROM \"tatp\".\"Access_Info\" WHERE \"s_id\" = 22 AND \"ai_type\" = 4\n";

        String expectedPlan =
                "LogicalProject(data1=[$2], data2=[$3], data3=[$4], data4=[$5])\n" +
                        "  LogicalFilter(condition=[AND(=(CAST($0):INTEGER NOT NULL, 22), =(CAST($1):INTEGER NOT NULL, 4))])\n" +
                        "    EnumerableTableScan(table=[[tatp, Access_Info]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q6() {
        String sqlQuery =
                "UPDATE \"tatp\".\"Subscriber\" SET \"vlr_location\" = 1 WHERE \"sub_nbr\" = 1\n";

        String expectedPlan =
                "LogicalTableModify(table=[[tatp, Subscriber]], operation=[UPDATE], updateColumnList=[[vlr_location]], sourceExpressionList=[[1]], flattened=[true])\n" +
                        "  LogicalProject(s_id=[$0], sub_nbr=[$1], bit_1=[$2], bit_2=[$3], bit_3=[$4], bit_4=[$5], bit_5=[$6], bit_6=[$7], bit_7=[$8], bit_8=[$9], bit_9=[$10], bit_10=[$11], hex_1=[$12], hex_2=[$13], hex_3=[$14], hex_4=[$15], hex_5=[$16], hex_6=[$17], hex_7=[$18], hex_8=[$19], hex_9=[$20], hex_10=[$21], byte2_1=[$22], byte2_2=[$23], byte2_3=[$24], byte2_4=[$25], byte2_5=[$26], byte2_6=[$27], byte2_7=[$28], byte2_8=[$29], byte2_9=[$30], byte2_10=[$31], msc_location=[$32], vlr_location=[$33], EXPR$0=[1])\n" +
                        "    LogicalFilter(condition=[=(CAST($1):INTEGER NOT NULL, 1)])\n" +
                        "      EnumerableTableScan(table=[[tatp, Subscriber]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q7_1() {
        String sqlQuery =
                "SELECT 1 FROM \"tatp\".\"Subscriber\" WHERE \"sub_nbr\" = 1\n";

        String expectedPlan =
                "LogicalProject(EXPR$0=[1])\n" +
                        "  LogicalFilter(condition=[=(CAST($1):INTEGER NOT NULL, 1)])\n" +
                        "    EnumerableTableScan(table=[[tatp, Subscriber]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q7_2() {
        String sqlQuery =
                "SELECT 1 FROM \"tatp\".\"Special_Facility\" WHERE \"s_id\" = 1\n";

        String expectedPlan =
                "LogicalProject(EXPR$0=[1])\n" +
                        "  LogicalFilter(condition=[=(CAST($0):INTEGER NOT NULL, 1)])\n" +
                        "    EnumerableTableScan(table=[[tatp, Special_Facility]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q7_3() {
        String sqlQuery =
                "INSERT INTO \"tatp\".\"Call_Forwarding\" VALUES (1, 1, 1, 1, 'a')\n";

        String expectedPlan =
                "LogicalTableModify(table=[[tatp, Call_Forwarding]], operation=[INSERT], flattened=[true])\n" +
                        "  LogicalValues(tuples=[[{ 1, 1, 1, 1, 'a' }]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q8_1() {
        String sqlQuery =
                "SELECT 1 FROM \"tatp\".\"Subscriber\" WHERE \"sub_nbr\" = 1\n";

        String expectedPlan =
                "LogicalProject(EXPR$0=[1])\n" +
                        "  LogicalFilter(condition=[=(CAST($1):INTEGER NOT NULL, 1)])\n" +
                        "    EnumerableTableScan(table=[[tatp, Subscriber]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q8_2() {
        String sqlQuery =
                "DELETE FROM \"tatp\".\"Call_Forwarding\" WHERE \"s_id\" = 1 AND \"sf_type\" = 1 AND \"start_time\" = 1";

        String expectedPlan =
                "LogicalTableModify(table=[[tatp, Call_Forwarding]], operation=[DELETE], flattened=[true])\n" +
                        "  LogicalProject(s_id=[$0], sf_type=[$1], start_time=[$2], end_time=[$3], numberx=[$4])\n" +
                        "    LogicalFilter(condition=[AND(=(CAST($0):INTEGER NOT NULL, 1), =(CAST($1):INTEGER NOT NULL, 1), =(CAST($2):INTEGER NOT NULL, 1))])\n" +
                        "      EnumerableTableScan(table=[[tatp, Call_Forwarding]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

}
