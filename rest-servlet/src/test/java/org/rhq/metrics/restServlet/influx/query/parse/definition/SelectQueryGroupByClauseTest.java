package org.rhq.metrics.restServlet.influx.query.parse.definition;

import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;

import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.rhq.metrics.restServlet.influx.query.parse.InfluxQueryParser;
import org.rhq.metrics.restServlet.influx.query.parse.InfluxQueryParserFactory;

/**
 * @author Thomas Segismont
 */
@RunWith(Parameterized.class)
public class SelectQueryGroupByClauseTest {

    @Parameters(name = "{0}")
    public static Iterable<Object[]> testValidQueries() throws Exception {
        return FluentIterable //
            .from(Arrays.asList(InfluxTimeUnit.values())) //
            .transform(new Function<InfluxTimeUnit, Object[]>() {
                @Override
                public Object[] apply(InfluxTimeUnit influxTimeUnit) {
                    int bucketSize = influxTimeUnit.ordinal() + 15;
                    return new Object[] {
                        "select * from a group by time ( " + bucketSize + influxTimeUnit.getId() + " )", bucketSize,
                        influxTimeUnit };
                }
            });
    }

    private final SelectQueryDefinitionsParser definitionsParser = new SelectQueryDefinitionsParser();
    private final ParseTreeWalker parseTreeWalker = ParseTreeWalker.DEFAULT;
    private final InfluxQueryParserFactory parserFactory = new InfluxQueryParserFactory();

    private final String queryText;
    private final int bucketSize;
    private final InfluxTimeUnit bucketSizeUnit;

    public SelectQueryGroupByClauseTest(String queryText, int bucketSize, InfluxTimeUnit bucketSizeUnit) {
        this.queryText = queryText;
        this.bucketSize = bucketSize;
        this.bucketSizeUnit = bucketSizeUnit;
    }

    @Test
    public void shouldDetectTimespan() {
        InfluxQueryParser parser = parserFactory.newInstanceForQuery(queryText);
        parseTreeWalker.walk(definitionsParser, parser.selectQuery());
        SelectQueryDefinitions definitions = definitionsParser.getSelectQueryDefinitions();
        GroupByClause groupByClause = definitions.getGroupByClause();

        assertEquals("time", groupByClause.getBucketType());
        assertEquals(bucketSize, groupByClause.getBucketSize());
        assertEquals(bucketSizeUnit, groupByClause.getBucketSizeUnit());
    }
}
