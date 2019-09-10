package org.apache.calcite.test;

import org.apache.calcite.util.Util;

import org.junit.Ignore;
import org.junit.Test;

public class HustleAdapterTest {

    private static final String VALUES0 = "(values (1, 'a'), (2, 'b'))";

    private static final String VALUES1 =
            "(values (1, 'a'), (2, 'b')) as t(x, y)";

    private static final String VALUES2 =
            "(values (1, 'a'), (2, 'b'), (1, 'b'), (2, 'c'), (2, 'c')) as t(x, y)";

    private static final String VALUES3 =
            "(values (1, 'a'), (2, 'b')) as v(w, z)";

    private static final String VALUES4 =
            "(values (1, 'a'), (2, 'b'), (3, 'b'), (4, 'c'), (2, 'c')) as t(x, y)";

    private CalciteAssert.AssertQuery sql(String sql) {
        return CalciteAssert.that()
                .query(sql);
    }

    /**
     * Tests a VALUES query evaluated using Spark.
     * There are no data sources.
     */
    @Test public void testValues() {

//        Util.discard(SparkRel.class);

        final String sql = "select *\n"
                + "from " + VALUES0;

        final String plan = "PLAN="
                + "EnumerableValues(tuples=[[{ 1, 'a' }, { 2, 'b' }]])";

        sql(sql).explainContains(plan);
    }

}
