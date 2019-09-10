package org.apache.calcite.test;

import org.apache.calcite.adapter.hustle.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.schema.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.tools.*;
import org.junit.*;
import static org.junit.Assert.fail;

import java.sql.*;
import java.util.*;

public class SSBTest {
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
        String catalogPath = EndToEndTest.class.getClassLoader().getResource("ssb_catalog.json").getFile();
        operandMap.put("catalog_file", catalogPath);
        HustleSchema hc = (HustleSchema) HustleSchemaFactory.INSTANCE.create(schemaPlus, "hc", operandMap);
        schemaPlus.add("ssb", hc);

        FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .defaultSchema(schemaPlus)
                .build();

        planner_ = Frameworks.getPlanner(frameworkConfig);
    }

    @Test
    public void q1() {
        String sqlQuery = "SELECT SUM(\"lineorder\".\"lo_extendedprice\"*\"lineorder\".\"lo_discount\") AS \"revenue\"\n" +
                "FROM \"ssb\".\"lineorder\", \"ssb\".\"ddate\"\n" +
                "WHERE \"lineorder\".\"lo_orderdate\" = \"ddate\".\"d_datekey\"\n" +
                "  AND \"ddate\".\"d_year\" = 1993\n" +
                "  AND \"lineorder\".\"lo_discount\" BETWEEN 1 AND 3\n" +
                "  AND \"lineorder\".\"lo_quantity\" < 25\n";

        String expectedPlan =
                "LogicalAggregate(group=[{}], revenue=[SUM($0)])\n" +
                        "  LogicalProject($f0=[*($9, $11)])\n" +
                        "    LogicalFilter(condition=[AND(=($5, $17), =(CAST($21):INTEGER NOT NULL, 1993), >=($11, 1), <=($11, 3), <($8, 25))])\n" +
                        "      LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "        EnumerableTableScan(table=[[ssb, lineorder]])\n" +
                        "        EnumerableTableScan(table=[[ssb, ddate]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q2() {
        String sqlQuery =
                "SELECT SUM(\"lineorder\".\"lo_extendedprice\"*\"lineorder\".\"lo_discount\") AS \"revenue\"\n" +
                        "FROM \"ssb\".\"lineorder\", \"ssb\".\"ddate\"\n" +
                        "WHERE \"lineorder\".\"lo_orderdate\" = \"ddate\".\"d_datekey\"\n" +
                        "  AND \"ddate\".\"d_yearmonthnum\" = 199401\n" +
                        "  AND \"lineorder\".\"lo_discount\" BETWEEN 4 AND 6\n" +
                        "  AND \"lineorder\".\"lo_quantity\" BETWEEN 26 AND 35";

        String expectedPlan =
                "LogicalAggregate(group=[{}], revenue=[SUM($0)])\n" +
                        "  LogicalProject($f0=[*($9, $11)])\n" +
                        "    LogicalFilter(condition=[AND(=($5, $17), =(CAST($22):INTEGER NOT NULL, 199401), >=($11, 4), <=($11, 6), >=($8, 26), <=($8, 35))])\n" +
                        "      LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "        EnumerableTableScan(table=[[ssb, lineorder]])\n" +
                        "        EnumerableTableScan(table=[[ssb, ddate]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q3() {
        String sqlQuery =
                "SELECT SUM(\"lineorder\".\"lo_extendedprice\"*\"lineorder\".\"lo_discount\") AS \"revenue\"\n" +
                        "FROM \"ssb\".\"lineorder\", \"ssb\".\"ddate\"\n" +
                        "WHERE \"lineorder\".\"lo_orderdate\" = \"ddate\".\"d_datekey\"\n" +
                        "  AND \"ddate\".\"d_weeknuminyear\" = 6\n" +
                        "  AND \"ddate\".\"d_year\" = 1994\n" +
                        "  AND \"lineorder\".\"lo_discount\" BETWEEN 5 AND 7\n" +
                        "  AND \"lineorder\".\"lo_quantity\" BETWEEN 36 AND 40\n";

        String expectedPlan =
                "LogicalAggregate(group=[{}], revenue=[SUM($0)])\n" +
                        "  LogicalProject($f0=[*($9, $11)])\n" +
                        "    LogicalFilter(condition=[AND(=($5, $17), =(CAST($28):INTEGER NOT NULL, 6), =(CAST($21):INTEGER NOT NULL, 1994), >=($11, 5), <=($11, 7), >=($8, 36), <=($8, 40))])\n" +
                        "      LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "        EnumerableTableScan(table=[[ssb, lineorder]])\n" +
                        "        EnumerableTableScan(table=[[ssb, ddate]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q4() {
        String sqlQuery =
                "SELECT SUM(\"lineorder\".\"lo_revenue\"), \"ddate\".\"d_year\", \"part\".\"p_brand1\"\n" +
                        "FROM  \"ssb\".\"lineorder\", \"ssb\".\"ddate\", \"ssb\".\"part\", \"ssb\".\"supplier\"\n" +
                        "WHERE \"lineorder\".\"lo_orderdate\" = \"ddate\".\"d_datekey\"\n" +
                        "  AND \"lineorder\".\"lo_partkey\" = \"part\".\"p_partkey\"\n" +
                        "  AND \"lineorder\".\"lo_suppkey\" = \"supplier\".\"s_suppkey\"\n" +
                        "  AND \"part\".\"p_category\" = 'MFGR#12'\n" +
                        "  AND \"supplier\".\"s_region\" = 'AMERICA'\n" +
                        "GROUP BY \"ddate\".\"d_year\", \"part\".\"p_brand1\"\n" +
                        "ORDER BY \"ddate\".\"d_year\", \"part\".\"p_brand1\"\n";

        String expectedPlan =
                "LogicalSort(sort0=[$1], sort1=[$2], dir0=[ASC], dir1=[ASC])\n" +
                        "  LogicalProject(EXPR$0=[$2], d_year=[$0], p_brand1=[$1])\n" +
                        "    LogicalAggregate(group=[{0, 1}], EXPR$0=[SUM($2)])\n" +
                        "      LogicalProject(d_year=[$21], p_brand1=[$38], lo_revenue=[$12])\n" +
                        "        LogicalFilter(condition=[AND(=($5, $17), =($3, $34), =($4, $43), =($37, 'MFGR#12'), =($48, 'AMERICA'))])\n" +
                        "          LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "            LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "              LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "                EnumerableTableScan(table=[[ssb, lineorder]])\n" +
                        "                EnumerableTableScan(table=[[ssb, ddate]])\n" +
                        "              EnumerableTableScan(table=[[ssb, part]])\n" +
                        "            EnumerableTableScan(table=[[ssb, supplier]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q5() {
        String sqlQuery =
                "SELECT SUM(\"lineorder\".\"lo_revenue\"), \"ddate\".\"d_year\", \"part\".\"p_brand1\"\n" +
                        "FROM  \"ssb\".\"lineorder\", \"ssb\".\"ddate\", \"ssb\".\"part\", \"ssb\".\"supplier\"\n" +
                        "WHERE \"lineorder\".\"lo_orderdate\" = \"ddate\".\"d_datekey\"\n" +
                        "  AND \"lineorder\".\"lo_partkey\" = \"part\".\"p_partkey\"\n" +
                        "  AND \"lineorder\".\"lo_suppkey\" = \"supplier\".\"s_suppkey\"\n" +
                        "  AND \"part\".\"p_brand1\" BETWEEN 'MFGR#2221' AND 'MFGR#2228'\n" +
                        "  AND \"supplier\".\"s_region\" = 'AMERICA'\n" +
                        "GROUP BY \"ddate\".\"d_year\", \"part\".\"p_brand1\"\n" +
                        "ORDER BY \"ddate\".\"d_year\", \"part\".\"p_brand1\"\n";

        String expectedPlan =
                "LogicalSort(sort0=[$1], sort1=[$2], dir0=[ASC], dir1=[ASC])\n" +
                        "  LogicalProject(EXPR$0=[$2], d_year=[$0], p_brand1=[$1])\n" +
                        "    LogicalAggregate(group=[{0, 1}], EXPR$0=[SUM($2)])\n" +
                        "      LogicalProject(d_year=[$21], p_brand1=[$38], lo_revenue=[$12])\n" +
                        "        LogicalFilter(condition=[AND(=($5, $17), =($3, $34), =($4, $43), >=($38, 'MFGR#2221'), <=($38, 'MFGR#2228'), =($48, 'AMERICA'))])\n" +
                        "          LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "            LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "              LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "                EnumerableTableScan(table=[[ssb, lineorder]])\n" +
                        "                EnumerableTableScan(table=[[ssb, ddate]])\n" +
                        "              EnumerableTableScan(table=[[ssb, part]])\n" +
                        "            EnumerableTableScan(table=[[ssb, supplier]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q6() {
        String sqlQuery =
                "SELECT SUM(\"lineorder\".\"lo_revenue\"), \"ddate\".\"d_year\", \"part\".\"p_brand1\"\n" +
                        "FROM  \"ssb\".\"lineorder\", \"ssb\".\"ddate\", \"ssb\".\"part\", \"ssb\".\"supplier\"\n" +
                        "WHERE \"lineorder\".\"lo_orderdate\" = \"ddate\".\"d_datekey\"\n" +
                        "  AND \"lineorder\".\"lo_partkey\" = \"part\".\"p_partkey\"\n" +
                        "  AND \"lineorder\".\"lo_suppkey\" = \"supplier\".\"s_suppkey\"\n" +
                        "  AND \"part\".\"p_brand1\" = 'MFGR#2221'\n" +
                        "  AND \"supplier\".\"s_region\" = 'EUROPE'\n" +
                        "GROUP BY \"ddate\".\"d_year\", \"part\".\"p_brand1\"\n" +
                        "ORDER BY \"ddate\".\"d_year\", \"part\".\"p_brand1\"";

        String expectedPlan =
                "LogicalSort(sort0=[$1], sort1=[$2], dir0=[ASC], dir1=[ASC])\n" +
                        "  LogicalProject(EXPR$0=[$2], d_year=[$0], p_brand1=[$1])\n" +
                        "    LogicalAggregate(group=[{0, 1}], EXPR$0=[SUM($2)])\n" +
                        "      LogicalProject(d_year=[$21], p_brand1=[$38], lo_revenue=[$12])\n" +
                        "        LogicalFilter(condition=[AND(=($5, $17), =($3, $34), =($4, $43), =($38, 'MFGR#2221'), =($48, 'EUROPE'))])\n" +
                        "          LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "            LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "              LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "                EnumerableTableScan(table=[[ssb, lineorder]])\n" +
                        "                EnumerableTableScan(table=[[ssb, ddate]])\n" +
                        "              EnumerableTableScan(table=[[ssb, part]])\n" +
                        "            EnumerableTableScan(table=[[ssb, supplier]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q7() {
        String sqlQuery =
                "SELECT \"customer\".\"c_nation\", \"supplier\".\"s_nation\", \"ddate\".\"d_year\", SUM(\"lineorder\".\"lo_revenue\") AS \"revenue\"\n" +
                        "FROM \"ssb\".\"customer\", \"ssb\".\"lineorder\", \"ssb\".\"supplier\", \"ssb\".\"ddate\"   \n" +
                        "WHERE \"lineorder\".\"lo_custkey\" = \"customer\".\"c_custkey\"\n" +
                        "  AND \"lineorder\".\"lo_suppkey\" = \"supplier\".\"s_suppkey\"\n" +
                        "  AND \"lineorder\".\"lo_orderdate\" = \"ddate\".\"d_datekey\"\n" +
                        "  AND \"customer\".\"c_region\" = 'ASIA'\n" +
                        "  AND \"supplier\".\"s_region\" = 'ASIA'\n" +
                        "  AND \"ddate\".\"d_year\" >= 1992 AND \"ddate\".\"d_year\" <= 1997\n" +
                        "GROUP BY \"customer\".\"c_nation\", \"supplier\".\"s_nation\", \"ddate\".\"d_year\"\n" +
                        "ORDER BY \"ddate\".\"d_year\" ASC, \"revenue\" DESC\n";

        String expectedPlan =
                "LogicalSort(sort0=[$2], sort1=[$3], dir0=[ASC], dir1=[DESC])\n" +
                        "  LogicalAggregate(group=[{0, 1, 2}], revenue=[SUM($3)])\n" +
                        "    LogicalProject(c_nation=[$4], s_nation=[$29], d_year=[$36], lo_revenue=[$20])\n" +
                        "      LogicalFilter(condition=[AND(=($10, $0), =($12, $25), =($13, $32), =($5, 'ASIA'), =($30, 'ASIA'), >=($36, 1992), <=($36, 1997))])\n" +
                        "        LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "          LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "            LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "              EnumerableTableScan(table=[[ssb, customer]])\n" +
                        "              EnumerableTableScan(table=[[ssb, lineorder]])\n" +
                        "            EnumerableTableScan(table=[[ssb, supplier]])\n" +
                        "          EnumerableTableScan(table=[[ssb, ddate]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q8() {
        String sqlQuery =
                "SELECT \"customer\".\"c_city\", \"supplier\".\"s_city\", \"ddate\".\"d_year\", SUM(\"lineorder\".\"lo_revenue\") AS \"revenue\"\n" +
                        "FROM \"ssb\".\"customer\", \"ssb\".\"lineorder\", \"ssb\".\"supplier\", \"ssb\".\"ddate\"   \n" +
                        "WHERE \"lineorder\".\"lo_custkey\" = \"customer\".\"c_custkey\"\n" +
                        "  AND \"lineorder\".\"lo_suppkey\" = \"supplier\".\"s_suppkey\"\n" +
                        "  AND \"lineorder\".\"lo_orderdate\" = \"ddate\".\"d_datekey\"\n" +
                        "  AND \"customer\".\"c_nation\" = 'UNITED STATES'\n" +
                        "  AND \"supplier\".\"s_nation\" = 'UNITED STATES'\n" +
                        "  AND \"ddate\".\"d_year\" >= 1992 AND \"ddate\".\"d_year\" <= 1997\n" +
                        "GROUP BY \"customer\".\"c_city\", \"s_city\", \"ddate\".\"d_year\"\n" +
                        "ORDER BY \"ddate\".\"d_year\" ASC, \"revenue\" DESC";

        String expectedPlan =
                "LogicalSort(sort0=[$2], sort1=[$3], dir0=[ASC], dir1=[DESC])\n" +
                        "  LogicalAggregate(group=[{0, 1, 2}], revenue=[SUM($3)])\n" +
                        "    LogicalProject(c_city=[$3], s_city=[$28], d_year=[$36], lo_revenue=[$20])\n" +
                        "      LogicalFilter(condition=[AND(=($10, $0), =($12, $25), =($13, $32), =($4, 'UNITED STATES'), =($29, 'UNITED STATES'), >=($36, 1992), <=($36, 1997))])\n" +
                        "        LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "          LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "            LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "              EnumerableTableScan(table=[[ssb, customer]])\n" +
                        "              EnumerableTableScan(table=[[ssb, lineorder]])\n" +
                        "            EnumerableTableScan(table=[[ssb, supplier]])\n" +
                        "          EnumerableTableScan(table=[[ssb, ddate]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q9() {
        String sqlQuery =
                "SELECT \"customer\".\"c_city\",\"supplier\".\"s_city\", \"ddate\".\"d_year\", SUM(\"lineorder\".\"lo_revenue\") AS \"revenue\"\n" +
                        "FROM \"ssb\".\"customer\", \"ssb\".\"lineorder\", \"ssb\".\"supplier\", \"ssb\".\"ddate\"\n" +
                        "WHERE \"lineorder\".\"lo_custkey\" = \"customer\".\"c_custkey\"\n" +
                        "  AND \"lineorder\".\"lo_suppkey\" = \"supplier\".\"s_suppkey\"\n" +
                        "  AND \"lineorder\".\"lo_orderdate\" = \"ddate\".\"d_datekey\"\n" +
                        "  AND (\"customer\".\"c_city\" = 'UNITED KI1' OR \"customer\".\"c_city\" = 'UNITED KI5')\n" +
                        "  AND (\"supplier\".\"s_city\" = 'UNITED KI1' OR \"supplier\".\"s_city\" = 'UNITED KI5')\n" +
                        "  AND \"ddate\".\"d_year\" >= 1992 AND \"ddate\".\"d_year\" <= 1997\n" +
                        "GROUP BY \"customer\".\"c_city\", \"supplier\".\"s_city\", \"ddate\".\"d_year\"\n" +
                        "ORDER BY \"ddate\".\"d_year\" ASC, \"revenue\" DESC\n";

        String expectedPlan =
                "LogicalSort(sort0=[$2], sort1=[$3], dir0=[ASC], dir1=[DESC])\n" +
                        "  LogicalAggregate(group=[{0, 1, 2}], revenue=[SUM($3)])\n" +
                        "    LogicalProject(c_city=[$3], s_city=[$28], d_year=[$36], lo_revenue=[$20])\n" +
                        "      LogicalFilter(condition=[AND(=($10, $0), =($12, $25), =($13, $32), OR(=($3, 'UNITED KI1'), =($3, 'UNITED KI5')), OR(=($28, 'UNITED KI1'), =($28, 'UNITED KI5')), >=($36, 1992), <=($36, 1997))])\n" +
                        "        LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "          LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "            LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "              EnumerableTableScan(table=[[ssb, customer]])\n" +
                        "              EnumerableTableScan(table=[[ssb, lineorder]])\n" +
                        "            EnumerableTableScan(table=[[ssb, supplier]])\n" +
                        "          EnumerableTableScan(table=[[ssb, ddate]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q10() {
        String sqlQuery =
                "SELECT \"customer\".\"c_city\", \"supplier\".\"s_city\", \"ddate\".\"d_year\", SUM(\"lineorder\".\"lo_revenue\") AS \"revenue\"\n" +
                        "FROM \"ssb\".\"customer\", \"ssb\".\"lineorder\", \"ssb\".\"supplier\", \"ssb\".\"ddate\"   \n" +
                        "WHERE \"lineorder\".\"lo_custkey\" = \"customer\".\"c_custkey\"\n" +
                        "  AND \"lineorder\".\"lo_suppkey\" = \"supplier\".\"s_suppkey\"\n" +
                        "  AND \"lineorder\".\"lo_orderdate\" = \"ddate\".\"d_datekey\"\n" +
                        "  AND (\"customer\".\"c_city\" = 'UNITED KI1' OR \"customer\".\"c_city\" = 'UNITED KI5')\n" +
                        "  AND (\"supplier\".\"s_city\" = 'UNITED KI1' OR \"supplier\".\"s_city\" = 'UNITED KI5')\n" +
                        "  AND \"ddate\".\"d_yearmonth\" = 'Dec1997'\n" +
                        "GROUP BY \"customer\".\"c_city\", \"supplier\".\"s_city\", \"ddate\".\"d_year\"\n" +
                        "ORDER BY \"ddate\".\"d_year\" ASC, \"revenue\" DESC\n";

        String expectedPlan =
                "LogicalSort(sort0=[$2], sort1=[$3], dir0=[ASC], dir1=[DESC])\n" +
                        "  LogicalAggregate(group=[{0, 1, 2}], revenue=[SUM($3)])\n" +
                        "    LogicalProject(c_city=[$3], s_city=[$28], d_year=[$36], lo_revenue=[$20])\n" +
                        "      LogicalFilter(condition=[AND(=($10, $0), =($12, $25), =($13, $32), OR(=($3, 'UNITED KI1'), =($3, 'UNITED KI5')), OR(=($28, 'UNITED KI1'), =($28, 'UNITED KI5')), =($38, 'Dec1997'))])\n" +
                        "        LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "          LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "            LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "              EnumerableTableScan(table=[[ssb, customer]])\n" +
                        "              EnumerableTableScan(table=[[ssb, lineorder]])\n" +
                        "            EnumerableTableScan(table=[[ssb, supplier]])\n" +
                        "          EnumerableTableScan(table=[[ssb, ddate]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q11() {
        String sqlQuery =
                "SELECT \"ddate\".\"d_year\", \"customer\".\"c_nation\", sum(\"lineorder\".\"lo_revenue\"-\"lineorder\".\"lo_supplycost\") AS \"profit\"\n" +
                        "FROM \"ssb\".\"ddate\", \"ssb\".\"customer\", \"ssb\".\"supplier\", \"ssb\".\"part\", \"ssb\".\"lineorder\"\n" +
                        "WHERE \"lineorder\".\"lo_custkey\" = \"customer\".\"c_custkey\"\n" +
                        "  AND \"lineorder\".\"lo_suppkey\" = \"supplier\".\"s_suppkey\"\n" +
                        "  AND \"lineorder\".\"lo_partkey\" = \"part\".\"p_partkey\"\n" +
                        "  AND \"lineorder\".\"lo_orderdate\" = \"ddate\".\"d_datekey\"\n" +
                        "  AND \"customer\".\"c_region\" = 'AMERICA'\n" +
                        "  AND \"supplier\".\"s_region\" = 'AMERICA'\n" +
                        "  AND (\"part\".\"p_mfgr\" = 'MFGR#1' OR \"part\".\"p_mfgr\" = 'MFGR#2')\n" +
                        "GROUP BY \"ddate\".\"d_year\", \"customer\".\"c_nation\"\n" +
                        "ORDER BY \"ddate\".\"d_year\", \"customer\".\"c_nation\"";

        String expectedPlan =
                "LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])\n" +
                        "  LogicalAggregate(group=[{0, 1}], profit=[SUM($2)])\n" +
                        "    LogicalProject(d_year=[$4], c_nation=[$21], $f2=[-($53, $54)])\n" +
                        "      LogicalFilter(condition=[AND(=($43, $17), =($45, $25), =($44, $32), =($46, $0), =($22, 'AMERICA'), =($30, 'AMERICA'), OR(=($34, 'MFGR#1'), =($34, 'MFGR#2')))])\n" +
                        "        LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "          LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "            LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "              LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "                EnumerableTableScan(table=[[ssb, ddate]])\n" +
                        "                EnumerableTableScan(table=[[ssb, customer]])\n" +
                        "              EnumerableTableScan(table=[[ssb, supplier]])\n" +
                        "            EnumerableTableScan(table=[[ssb, part]])\n" +
                        "          EnumerableTableScan(table=[[ssb, lineorder]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }

    @Test
    public void q12() {
        String sqlQuery =
                "SELECT \"ddate\".\"d_year\", \"supplier\".\"s_nation\", \"part\".\"p_category\", sum(\"lineorder\".\"lo_revenue\"-\"lineorder\".\"lo_supplycost\") AS \"profit\"\n" +
                        "FROM \"ssb\".\"ddate\", \"ssb\".\"customer\", \"ssb\".\"supplier\", \"ssb\".\"part\", \"ssb\".\"lineorder\"\n" +
                        "WHERE \"lineorder\".\"lo_custkey\" = \"customer\".\"c_custkey\"\n" +
                        "  AND \"lineorder\".\"lo_suppkey\" = \"supplier\".\"s_suppkey\"\n" +
                        "  AND \"lineorder\".\"lo_partkey\" = \"part\".\"p_partkey\"\n" +
                        "  AND \"lineorder\".\"lo_orderdate\" = \"ddate\".\"d_datekey\"\n" +
                        "  AND \"customer\".\"c_region\" = 'AMERICA'\n" +
                        "  AND \"supplier\".\"s_region\" = 'AMERICA'\n" +
                        "  AND (\"ddate\".\"d_year\" = 1997 OR \"ddate\".\"d_year\" = 1998)\n" +
                        "  AND (\"part\".\"p_mfgr\" = 'MFGR#1' OR \"part\".\"p_mfgr\" = 'MFGR#2')\n" +
                        "GROUP BY \"ddate\".\"d_year\", \"supplier\".\"s_nation\", \"part\".\"p_category\"\n" +
                        "ORDER BY \"ddate\".\"d_year\", \"supplier\".\"s_nation\", \"part\".\"p_category\"";

        String expectedPlan =
                "LogicalSort(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[ASC], dir1=[ASC], dir2=[ASC])\n" +
                        "  LogicalAggregate(group=[{0, 1, 2}], profit=[SUM($3)])\n" +
                        "    LogicalProject(d_year=[$4], s_nation=[$29], p_category=[$35], $f3=[-($53, $54)])\n" +
                        "      LogicalFilter(condition=[AND(=($43, $17), =($45, $25), =($44, $32), =($46, $0), =($22, 'AMERICA'), =($30, 'AMERICA'), OR(=(CAST($4):INTEGER NOT NULL, 1997), =(CAST($4):INTEGER NOT NULL, 1998)), OR(=($34, 'MFGR#1'), =($34, 'MFGR#2')))])\n" +
                        "        LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "          LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "            LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "              LogicalJoin(condition=[true], joinType=[inner])\n" +
                        "                EnumerableTableScan(table=[[ssb, ddate]])\n" +
                        "                EnumerableTableScan(table=[[ssb, customer]])\n" +
                        "              EnumerableTableScan(table=[[ssb, supplier]])\n" +
                        "            EnumerableTableScan(table=[[ssb, part]])\n" +
                        "          EnumerableTableScan(table=[[ssb, lineorder]])\n";

        ComparePlans(sqlQuery, expectedPlan);
    }





}


