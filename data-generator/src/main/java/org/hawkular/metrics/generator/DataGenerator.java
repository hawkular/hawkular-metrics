/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.metrics.generator;

import static org.hawkular.metrics.model.MetricType.GAUGE;

import java.io.File;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.math3.random.ValueServer;
import org.hawkular.metrics.core.service.TimeUUIDUtils;

import com.google.common.base.Stopwatch;

import ch.qos.logback.classic.Level;

/**
 * @author jsanda
 */
public class DataGenerator {

    public static final int STATUS_SHOW_USAGE = 1;

    private Options options;

    private String keyspace;

    private File dataDir;

    private int tenants;

    private int metricsPerTenant;

    private long interval;

    private long startTime;

    private long endTime;

    private int bufferSize;

    private Pattern startEndRegexp;

    private Pattern intervalRegexp;

    public DataGenerator() {
        startEndRegexp = Pattern.compile("(\\d+)(m|h|d)");
        intervalRegexp = Pattern.compile("(\\d+)(s|m|h|d)");

        Option keyspace = new Option(null, "keyspace", true,
                "The keyspace in which data will be stored. Defaults to hawkular_metrics");
        Option dataDir = new Option(null, "data-dir", true,
                "The directory in which to store data files. Defaults to ./data.");
        Option tenants = new Option(null, "tenants", true, "The number of tenants. Defaults to 100.");
        Option metricsPerTenant = new Option(null, "metrics-per-tenant", true,
                "The number of metrics per tenant. Defaults to 100.");
        Option start = new Option(null, "start", true,
                "Specified using the regex pattern (d+)(m|h|d) where m is for minutes, h is for hours, and d is for " +
                "days. The value is subtracted from \"now\" to determine a start time. A value of 4h would be " +
                "interpreted as four hours ago. Defaults to one hour ago. Must be less than the end time.");
        Option end = new Option(null, "end", true,
                "Specified using the regex pattern (d+)(m|h|d) where m is for minutes, h is for hours, and d is for " +
                "days. The value is substracted from \"now\" to determine the end time. A value of 4h would be " +
                "interpreted as four hours ago. Defaults to \"now\". Must be greater than the start time.");
        Option interval = new Option(null, "interval", true, "Specified using the regex pattern (d+)(s|m|h|d) where s" +
                " is for seconds, m for minutes, h for hours, and d for days. Used to determine the number of data " +
                "points written. Defaults to one minute.");
        Option bufferSize = new Option(null, "buffer-size", true,
                "Defines how much data will be buffered before being written out as a new SSTable. This corresponds " +
                "roughly to the data size of the SSTable. Interpreted as mega bytes and defaults to 128 MB.");

        options = new Options().addOption(new Option("h", "help", false, "Show this message."))
                .addOption(keyspace)
                .addOption(dataDir)
                .addOption(tenants)
                .addOption(metricsPerTenant)
                .addOption(start)
                .addOption(end)
                .addOption(interval)
                .addOption(bufferSize);
    }

    public void run(CommandLine cmdLine) throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();

        if (cmdLine.hasOption("h")) {
            printUsage();
            return;
        }

        keyspace = cmdLine.getOptionValue("keyspace", "hawkular_metrics");
        dataDir = new File(cmdLine.getOptionValue("data-dir", "./data"));
        dataDir.mkdirs();
        tenants = Integer.parseInt(cmdLine.getOptionValue("tenants", "100"));
        metricsPerTenant = Integer.parseInt(cmdLine.getOptionValue("metrics-per-tenant", "100"));

        ValueServer valueServer = new ValueServer();
        valueServer.setMu(100);
        valueServer.setMode(ValueServer.UNIFORM_MODE);

        String endValue = cmdLine.getOptionValue("end");
        if (endValue == null) {
            endTime = System.currentTimeMillis();
        } else {
            endTime = getDuration("end", endValue, startEndRegexp);
        }

        String startValue = cmdLine.getOptionValue("start");
        if (startValue == null) {
            startTime = endTime - TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
        } else {
            startTime = endTime - getDuration("start", startValue, startEndRegexp);
        }

        String intervalValue = cmdLine.getOptionValue("interval");
        if (intervalValue == null) {
            interval = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);
        } else {
            interval = getDuration("interval", intervalValue, intervalRegexp);
        }

        bufferSize = Integer.parseInt(cmdLine.getOptionValue("buffer-size", "128"));

        CQLSSTableWriter writer = createWriter();
        long totalDataPoints = 0;

        long currentTime = startTime;
        while (currentTime <= endTime) {
            for (int i = 0; i < tenants; ++i) {
                for (int j = 0; j < metricsPerTenant; ++j) {
                    UUID timeUUID = TimeUUIDUtils.getTimeUUID(currentTime);
                    writer.addRow("TENANT-" + i, GAUGE.getCode(), "GAUGE-" + j, 0L, timeUUID, valueServer.getNext());
                    ++totalDataPoints;
                }
            }
            currentTime += interval;
        }

        writer.close();
        stopwatch.stop();

        System.out.println("\n\nStart time: " + startTime);
        System.out.println("End time: " + endTime);
        System.out.println("Total duration: " + (endTime - startTime) + " ms");
        System.out.println("Interval: " + interval);
        System.out.println("Tenants: " + tenants);
        System.out.println("Metrics per tenant: " + metricsPerTenant);
        System.out.println("Total data points: " + totalDataPoints);
        System.out.println("Execution time: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
    }

    private long getDuration(String option, String optionValue, Pattern regexp) {
        Matcher matcher = regexp.matcher(optionValue);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(optionValue + " is an invalid value for --" + option);
        }
        long value = Long.valueOf(matcher.group(1));
        String units = matcher.group(2);
        return convertDurationToMillis(value, units);
    }

    private long convertDurationToMillis(long value, String units) {
        switch (units) {
            case "s": return TimeUnit.MILLISECONDS.convert(value, TimeUnit.SECONDS);
            case "m": return TimeUnit.MILLISECONDS.convert(value, TimeUnit.MINUTES);
            case "h": return TimeUnit.MILLISECONDS.convert(value, TimeUnit.HOURS);
            case "d": return TimeUnit.MILLISECONDS.convert(value, TimeUnit.DAYS);
            default: throw new IllegalArgumentException(units + " is an invalid time unit");
        }
    }

    private CQLSSTableWriter createWriter() {
        String schema = "CREATE TABLE " + keyspace +  ".data ( " +
                "tenant_id text, " +
                "type tinyint, " +
                "metric text, " +
                "dpart bigint, " +
                "time timeuuid, " +
                "data_retention int static, " +
                "n_value double, " +
                "availability blob, " +
                "l_value bigint, " +
                // Commenting out the aggregates column because there appears to be a bug in the C*
                // code that breaks CQLSSTableWriter when the schema includes a collection. Fortunately,
                // we are not using the column so it can safely be ignored.
//                "aggregates set<frozen <aggregate_data>>, " +
                "PRIMARY KEY ((tenant_id, type, metric, dpart), time) " +
                ") WITH CLUSTERING ORDER BY (time DESC)";

        String insertGauge = "INSERT INTO " + keyspace + ".data (tenant_id, type, metric, dpart, time, n_value) " +
                "VALUES (?, ?, ?, ?, ?, ?)";


        return CQLSSTableWriter.builder()
                .inDirectory(dataDir)
                .forTable(schema)
                .using(insertGauge)
                .withBufferSizeInMB(bufferSize)
                .build();
    }

    private void printUsage() {
        HelpFormatter helpFormatter = new HelpFormatter();
        String syntax = "java -jar hawkular-metrics-data-generator.jar [options]";
        String header = "";

        helpFormatter.printHelp(syntax, header, getHelpOptions(), null);
    }

    public Options getOptions() {
        return options;
    }

    @SuppressWarnings("unchecked")
    public Options getHelpOptions() {
        Options helpOptions = new Options();
        for (Option option : (Collection<Option>) options.getOptions()) {
            helpOptions.addOption(option);
        }
        return helpOptions;
    }

    public static void main(String[] args) throws Exception {
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger)
                org.slf4j.LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.WARN);

        DataGenerator dataGenerator = new DataGenerator();
        try {
            CommandLineParser parser = new PosixParser();
            CommandLine cmdLine = parser.parse(dataGenerator.getOptions(), args);
            dataGenerator.run(cmdLine);
        } catch (ParseException parseException) {
            dataGenerator.printUsage();
            System.exit(STATUS_SHOW_USAGE);
        }
    }

}
