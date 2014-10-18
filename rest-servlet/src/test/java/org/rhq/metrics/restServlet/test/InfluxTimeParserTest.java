package org.rhq.metrics.restServlet.test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;

import org.junit.Test;

import org.rhq.metrics.restServlet.influx.InfluxTimeParser;

/**
 * Test the time parsing logic
 * @author Heiko W. Rupp
 */
public class InfluxTimeParserTest {

    @Test
    public void testParse1H() throws Exception {


        String oneHour = "1h";
        Matcher m = InfluxTimeParser.simpleTimePattern.matcher(oneHour);
        assert m.matches() : "No match";
        long val = InfluxTimeParser.getTimeFromExpr(m);
        assert val == 3600L * 1000L : "Returned value was " + val;

    }

    @Test
    public void testParseAbsTime() throws Exception {


        String absTime = "1388534400s";
        Matcher m = InfluxTimeParser.simpleTimePattern.matcher(absTime);
        assert m.matches() : "No match";
        long val = InfluxTimeParser.getTimeFromExpr(m);
        assert val == 1388534400L * 1000L : "Returned value was " + val;

    }

    @Test
    public void testAbsoluteTimeLaterThanFull() throws Exception {
        String expr = "time > '2013-08-12 23:32:01.232'";
        String ref = "2013-08-12 23:32:01.232";

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        Date date = df.parse(ref);

        long val = InfluxTimeParser.parseTime(expr);

        assert val == date.getTime();

    }

    @Test
    public void testAbsoluteTimeLaterThanNoMillis() throws Exception {
        String expr = "time > '2013-08-12 23:32:01'";
        String ref = "2013-08-12 23:32:01.000";

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        Date date = df.parse(ref);


        long val = InfluxTimeParser.parseTime(expr);
        assert val == date.getTime();

    }

    @Test
    public void testAbsoluteTimeLaterThanNoTime() throws Exception {
        String expr = "time > '2013-08-12'";
        String ref = "2013-08-12 00:00:00.000";

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        Date date = df.parse(ref);


        long val = InfluxTimeParser.parseTime(expr);
        assert val == date.getTime();

    }

    @Test
    public void testAbsoluteTimeEarlierThan() throws Exception {
        String expr = "time < '2013-08-12 23:32:01.232'";
        String ref = "2013-08-12 23:32:01.232";

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
        Date date = df.parse(ref);


        long val = InfluxTimeParser.parseTime(expr);
        assert val == date.getTime();

    }

    @Test
    public void testParse1dAgo() throws Exception {
        String expr = "time > now() - 1d";

        parseRelativeTime(expr,24*3600);
    }

    @Test
    public void testParse6hAgo() throws Exception {
        String expr = "time > now() - 6h";
        parseRelativeTime(expr,6*3600);

    }

    @Test
    public void testParse2wAgo() throws Exception {
        String expr = "time > now() - 2w";
        parseRelativeTime(expr,2*7*24*3600);

    }

    @Test
    public void testParse2wAgo2() throws Exception {
        String expr = "time > now()-2w";
        parseRelativeTime(expr,2*7*24*3600);

    }

    @Test
    public void testParse2wAgo3() throws Exception {
        String expr = "time>now()-2w";
        parseRelativeTime(expr,2*7*24*3600);

    }

    @Test
    public void testParseAbsTime2() throws Exception {
        String expr = "time > 1388534400s";
        long val = InfluxTimeParser.parseTime(expr);

        assert val == 1388534400L*1000L : "Val is " + val;

    }

    private void parseRelativeTime(String expr, long expectedDiffSec) {
        long now = System.currentTimeMillis();
        long val = InfluxTimeParser.parseTime(expr);

        long diff = now - val;
        diff = diff / 1000;
        diff = diff - expectedDiffSec;

        assert diff <5 : "Diff was " + diff + ", val=[" + val + "], now=[" + now + "]";
    }

}
