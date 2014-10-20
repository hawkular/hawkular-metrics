package org.rhq.metrics.restServlet.influx;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for the timing expressions of InfluxDb
 * @author Heiko W. Rupp
 */
public class InfluxTimeParser {

    public static final Pattern simpleTimePattern = Pattern.compile("([0-9]+)([a-z])");

    public static long getTimeFromExpr(Matcher m) {
        String val = m.group(1);
        String unit = m.group(2);
        long factor ;
        switch (unit) {
        case "w":
            factor=7L*86400L*1000L;
            break;
        case "d":
            factor =86400L*1000L;
            break;
        case "h":
            factor = 3600L*1000L;
            break;
        case "m":
            factor = 60L*1000L;
            break;
        case "s":
            factor = 1000L;
            break;
        case "u":
            factor =1;
            break;
        default:
            throw new IllegalArgumentException("Unknown unit " + unit);
        }
        long offset = Long.parseLong(val);
        return offset * factor ;
    }

    /**
     * Parse the time input which looks like "time > now() - 6h"
     * or "time > '2013-08-12 23:32:01.232'".
     *
     * @param timeExpr Expression to parse
     * @return Time in Milliseconds
     */
    public static long parseTime(String timeExpr) {
        String tmp; // Skip over "time <"
        if (timeExpr.startsWith("time")) {
            tmp = timeExpr.substring(4);
            if (tmp.indexOf('>')>-1) {
                tmp = tmp.substring(tmp.indexOf('>') + 1);
            }
            else if (tmp.indexOf('<')>-1) {
                tmp = tmp.substring(tmp.indexOf('<') + 1);
            }
            tmp = tmp.trim();
        } else {
            tmp = timeExpr;
        }

        if (tmp.startsWith("now()")) {              // "now() - "
            tmp = tmp.substring(5);                 // skip over "now()"
            tmp = tmp.substring(tmp.indexOf("-")+1);// skip over " -"
            tmp = tmp.trim(); // remove potentially exiting " "
            Matcher m = simpleTimePattern.matcher(tmp);
            if (m.matches()) {
                long convertedOffset = getTimeFromExpr(m);
                return System.currentTimeMillis() - convertedOffset;
            }

        } else {
            // Match absolute time in e.g. seconds since epoch
            // or explicit time
            Matcher m = simpleTimePattern.matcher(tmp);
            if (m.matches()) {
                // seconds since epoch
                return getTimeFromExpr(m);
            }
            else {
                // This is now an absolute time in a format like
                // 2013-08-12 23:32:01.232
                // Milliseconds are optional
                // If time is omitted, 00:00:00 is assumed
                try {
                    // Strip surrounding ' that may be present
                    if (tmp.startsWith("'")) {
                        tmp = tmp.substring(1);
                    }
                    if (tmp.endsWith("'")) {
                        tmp = tmp.substring(0,tmp.length()-1);
                    }
                    tmp = tmp.trim();
                    Date date;
                    if (tmp.contains(".")) {
                        DateFormat dateFormatLong = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
                        date = dateFormatLong.parse(tmp);
                    } else if (tmp.contains(":")) {
                        DateFormat dateFormatMedium = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                        date = dateFormatMedium.parse(tmp);
                    } else {
                        DateFormat dateFormatShort = new SimpleDateFormat("yyyy-MM-dd");
                        date = dateFormatShort.parse(tmp);
                    }
                    return date.getTime();
                } catch (ParseException e) {
                    e.printStackTrace();  // TODO: Customise this generated block
                }
            }
        }
        return 0;  // TODO: Customise this generated block
    }
}
