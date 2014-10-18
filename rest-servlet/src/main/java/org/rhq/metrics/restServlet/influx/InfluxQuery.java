package org.rhq.metrics.restServlet.influx;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
* Represent a query string send from a client to the simulated influx db server
* @author Heiko W. Rupp
*/
public class InfluxQuery {
    private String alias;
    private String metric;

    private String mapping;
    private String mappingArgs = null;
    private long start;
    private long end;
    private int bucketLengthSec;
    private String inputQueryString;

    // TODO Group by is optional and can also have a fill(val) part
    static Pattern metricSelectPattern = Pattern.compile("select +(\\S+) +(as +\\S+ +)?from +(\\S+) +where +(.*?) +group by time\\((\\S+)\\).*");

    public InfluxQuery(String query) {
        inputQueryString = query;

        Matcher m = metricSelectPattern.matcher(query.toLowerCase());
        if (m.matches()) {
            String expr = m.group(1);
            alias = m.group(2);

            if (alias!=null && !alias.isEmpty()) {
                alias = alias.substring(3, alias.length()-1);
                alias = deQuote(alias);
            } else {
                alias ="value"; // TODO correct?
            }

            metric = deQuote(m.group(3));
            String timeExpr = m.group(4);
            String groupExpr = m.group(5);

            if (timeExpr.contains("and")) {
                int i = timeExpr.indexOf(" and ");
                start = InfluxTimeParser.parseTime(timeExpr.substring(0, i));
                end = InfluxTimeParser.parseTime(timeExpr.substring(i + 5, timeExpr.length()));
            } else {
                end = System.currentTimeMillis();
                start = InfluxTimeParser.parseTime(timeExpr);
            }

            bucketLengthSec = (int) InfluxTimeParser.parseTime(groupExpr) / 1000; // TODO bucket length can be less than 1s

            if (expr.contains("(")) {
                int parPos = expr.indexOf("(");
                mapping = expr.substring(0, parPos);
                if (expr.contains(",")) {
                    String tmp = expr.substring(parPos + 1);
                    tmp = tmp.substring(tmp.indexOf(",")+1);
                    tmp = tmp.substring(0,tmp.length()-1);
                    mappingArgs = tmp.trim();
                }
            } else {
                mapping = expr;
            }
        }
        else if (query.toLowerCase().startsWith("select * from")) {
            // TODO
            System.out.println("Not yet supported: " + query);
        }
        else {
            throw new IllegalArgumentException("Can not parse " + query);
        }
    }

    /**
     * The passed string may be surrounded by quotes, so we
     * need to remove them.
     * @param in String to de-quote
     * @return De-Quoted String
     */
    private String deQuote(String in) {

        if (in==null) {
            return null;
        }
        String out ;
        int start = 0;
        int end = in.length();
        if (in.startsWith("\"")) {
            start++;
        }
        if (in.endsWith("\"")) {
            end--;
        }
        out=in.substring(start,end);

        return out;
    }

    @Override
    public String toString() {
        return "InfluxQuery{" +
            "inputQueryString='" + inputQueryString + '\'' +
            ", mapping='" + mapping + '\'' +
            ", mappingArgs='" + mappingArgs + '\'' +
            ", start=" + start +
            ", end=" + end +
            ", bucketLengthSec=" + bucketLengthSec +
            '}';
    }

    public String getAlias() {
        return alias;
    }

    public String getMetric() {
        return metric;
    }

    public String getMapping() {
        return mapping;
    }

    public String getMappingArgs() {
        return mappingArgs;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public int getBucketLengthSec() {
        return bucketLengthSec;
    }
}
