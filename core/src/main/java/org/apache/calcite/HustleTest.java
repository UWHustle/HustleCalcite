package org.apache.calcite;


import org.apache.calcite.adapter.java.*;
import org.apache.calcite.plan.*;
import java.sql.DriverManager;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Frameworks.ConfigBuilder;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.util.*;

import java.sql.*;

public class HustleTest {
    public static class RelR {
        public final int a;
        public final int b;
        public final int c;

        public RelR(int a, int b, int c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }
    }
    public static class RelS {
        public final int a;
        public final int b;
        public final int c;

        public RelS(int a, int b, int c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }
    }
    public static class RelT {
        public final int a;
        public final int b;
        public final int c;

        public RelT(int a, int b, int c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }

    }
    public static class HustleSchema {
        @Override public String toString() {
            return "TestSchema";
        }
        public final RelR[] relR = {new RelR(1,2,3), new RelR(3,4,5)};
        public final RelS[] relS = {new RelS(1,2,3), new RelS(3,4,5)};
        public final RelT[] relT = {new RelT(1,2,3), new RelT(3,4,5)};
    }

    public static void main(String[] args) {
        System.out.println("Hello World!"); // Display the string.
        try {
            Class.forName("org.apache.calcite.jdbc.Driver");
            Connection connection =
                    DriverManager.getConnection("jdbc:calcite:");
            CalciteConnection calciteConnection =
                    connection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            rootSchema.add("hschema", new ReflectiveSchema(new HustleSchema()));
            String sql = "select R.\"a\", R.\"b\", S.\"a\"  \n"
                            + "from \"hschema\".\"relR\" as R, \"hschema\".\"relS\" as S\n"
                            + "where R.\"a\" = S.\"a\" \n"
                            + "ORDER BY R.\"b\"";
//            Statement statement = connection.createStatement();
//            ResultSet resultSet =
//                    statement.executeQuery("select *\n"
//                            + "from \"hschema\".\"relR\" as R\n"
//                            + "join \"hschema\".\"relS\" as S\n"
//                            + "on R.\"a\" = S.\"a\"");
//
//            final StringBuilder buf = new StringBuilder();
//            while (resultSet.next()) {
//                int n = resultSet.getMetaData().getColumnCount();
//                for (int i = 1; i <= n; i++) {
//                    buf.append(i > 1 ? "; " : "")
//                            .append(resultSet.getMetaData().getColumnLabel(i))
//                            .append("=")
//                            .append(resultSet.getObject(i));
//                }
//                System.out.println(buf.toString());
//                buf.setLength(0);
//            }
//            resultSet.close();
//            statement.close();
//            connection.close();
//
//


//            Properties info = new Properties();
//            info.setProperty("lex", "JAVA");
//            String jsonFile = SolrSqlTest.class.getClassLoader().getResource("solr.json").toString().replaceAll("file:/", "");
//            try {
//                Class.forName("org.apache.calcite.jdbc.Driver");
//            } catch (ClassNotFoundException e1) {
//                e1.printStackTrace();
//            }
//
//            CalciteConnection connection = (CalciteConnection) DriverManager.getConnection("jdbc:calcite:model="+jsonFile, info);
//            final SchemaPlus schema = connection.getSchema();
            connection.close();

            ConfigBuilder builder = Frameworks.newConfigBuilder().defaultSchema(rootSchema).
                                    parserConfig(SqlParser.configBuilder().setCaseSensitive(false).build());

            FrameworkConfig config = builder.build();
            Planner planner = Frameworks.getPlanner(config);

            SqlNode sqlNode = planner.parse(sql);
            SqlNode node = planner.validate(sqlNode);
            RelRoot relRoot = planner.rel(node);

            System.out.println("SQL:      " + sql);
            System.out.println("\n\n\n");
            System.out.println("Parsed:     "+sqlNode.toString());
            System.out.println("\n\n\n");
            System.out.println("Validated:     "+node.toString());
            System.out.println("\n\n\n");
            System.out.println("ToLplan:     "+relRoot);
            System.out.println("\n\n\n");
            System.out.println(Util.toLinux(
                    RelOptUtil.dumpPlan("", relRoot.rel, SqlExplainFormat.JSON,
                            SqlExplainLevel.DIGEST_ATTRIBUTES)));

        } catch (Exception e) {
            e.printStackTrace();
        }

        }
}
