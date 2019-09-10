package org.apache.calcite.test;

import org.apache.calcite.jdbc.*;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.*;
import org.apache.calcite.util.Util;

import org.junit.*;

import java.sql.*;
import java.util.*;

public class SSBTest_old {
    private CalciteAssert.AssertQuery sql(String sql) {
        return CalciteAssert.that()
                .query(sql);
    }

    @Before
    public void before() {
        System.out.println("Invoked before each test method");
    }

    private CalciteAssert.ConnectionFactory newConnectionFactory() {
        return new CalciteAssert.ConnectionFactory() {
            @Override public Connection createConnection() throws SQLException {
                final Connection connection = DriverManager.getConnection("jdbc:calcite:model=target/test-classes/model.json");
                return connection;
            }
        };
    }
    @Test
    public void q1() {
        String sqlquery = "SELECT \"s_id\", \"sub_nbr\", \"bit_1\", \"bit_2\", \"bit_3\", \"bit_4\", \"bit_5\", \"bit_6\", \"bit_7\", \"bit_8\", \"bit_9\", \"bit_10\", \"hex_1\", \"hex_2\", \"hex_3\", \"hex_4\", \"hex_5\", \"hex_6\", \"hex_7\", \"hex_8\", \"hex_9\", \"hex_10\", \"byte2_1\", \"byte2_2\", \"byte2_3\", \"byte2_4\", \"byte2_5\", \"byte2_6\", \"byte2_7\", \"byte2_8\", \"byte2_9\", \"byte2_10\", \"msc_location\", \"vlr_location\" FROM \"tatp\".\"Subscriber\" WHERE \"s_id\" = 5";
        CalciteAssert.that()
                .with(newConnectionFactory())
                .query(sqlquery)
                .convertContains("a");

    }
    @Test
    public void q2() {
        String sqlquery = "SELECT SUM(\"lineorder\".\"lo_extendedprice\"*\"lineorder\".\"lo_discount\") AS \"revenue\"\n" +
                "FROM \"ssb\".\"lineorder\", \"ssb\".\"ddate\"\n" +
                "WHERE \"lineorder\".\"lo_orderdate\" = \"ddate\".\"d_datekey\"\n" +
                "  AND \"ddate\".\"d_year\" = 1993\n" +
                "  AND \"lineorder\".\"lo_discount\" BETWEEN 1 AND 3\n" +
                "  AND \"lineorder\".\"lo_quantity\" < 25\n";
//        String sqlquery = "SELECT \"cf\".\"numberx\" FROM \"tatp\".\"Special_Facility\" AS \"sf\", \"tatp\".\"Call_Forwarding\" AS \"cf\" WHERE (\"sf\".\"s_id\" = 5 AND \"sf\".\"sf_type\" = 3 AND \"sf\".\"is_active\" = 1) AND (\"cf\".\"s_id\" = \"sf\".\"s_id\" AND \"cf\".\"sf_type\" = \"sf\".\"sf_type\") AND (\"cf\".\"start_time\" <= 1 AND 2 < \"cf\".\"end_time\")";
        CalciteAssert.that()
                .with(newConnectionFactory())
                .query(sqlquery).returns("");
//                .convertContains("a");

    }
}

