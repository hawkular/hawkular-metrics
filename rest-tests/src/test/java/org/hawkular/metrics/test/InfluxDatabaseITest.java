/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.influxdb.dto.Serie;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Jeeva Kandasamy
 */
public class InfluxDatabaseITest extends InfluxDBTest {
    private String dbName = nextTenantId();

    /**
     * Test that writing of a simple Serie works.
     */
    @Test
    public void testWrite() {
        String timeSeries = "writeTest";
        long startTime = System.currentTimeMillis();
        Serie generatedSerie = getSerie(timeSeries, startTime);
        influxDB.write(dbName, TimeUnit.MILLISECONDS, generatedSerie);
        List<Serie> series = influxDB.query(dbName, "select * from " + timeSeries, TimeUnit.MILLISECONDS);
        Assert.assertNotNull(series);
    }

    @Test
    public void testQueryOrderedAsc() {
        String timeSeries = "queryTestOrderedAsc";
        long startTime = System.currentTimeMillis();
        Serie generatedSerie = getSerie(timeSeries, startTime);
        influxDB.write(dbName, TimeUnit.MILLISECONDS, generatedSerie);
        List<Serie> series = influxDB.query(dbName,
                "select value from " + timeSeries + " order asc",
                TimeUnit.MILLISECONDS);
        Assert.assertNotNull(series);
        this.assertSeriesEquals(generatedSerie, series.get(0));
    }

    @Test
    public void testQueryOrderedDesc() {
        String timeSeries = "queryTestOrderedDesc";
        long startTime = System.currentTimeMillis();
        Serie generatedSerie = getSerie(timeSeries, startTime);
        influxDB.write(dbName, TimeUnit.MILLISECONDS, generatedSerie);
        startTime = startTime - 9000L;
        Serie serieSource = new Serie.Builder(timeSeries)
                .columns("time", "value")
                .values(startTime + 8000L, 18.9)
                .values(startTime + 7000L, 17.9)
                .values(startTime + 6000L, 16.9)
                .values(startTime + 5000L, 15.9)
                .values(startTime + 4000L, 14.9)
                .values(startTime + 3000L, 13.9)
                .values(startTime + 2000L, 12.9)
                .values(startTime + 1000L, 11.9)
                .values(startTime, 10.9)
                .build();
        List<Serie> series = influxDB.query(dbName,
                "select value from " + timeSeries + " order desc",
                TimeUnit.MILLISECONDS);
        Assert.assertNotNull(series);
        this.assertSeriesEquals(serieSource, series.get(0));
    }

    @Test
    public void testQueryLimitClause() {
        String timeSeries = "queryTestLimitClause";
        long startTime = System.currentTimeMillis();
        Serie generatedSerie = getSerie(timeSeries, startTime);
        influxDB.write(dbName, TimeUnit.MILLISECONDS, generatedSerie);
        startTime = startTime - 9000L;
        Serie serieSource = new Serie.Builder(timeSeries)
                .columns("time", "value")
                .values(startTime, 10.9)
                .values(startTime + 1000L, 11.9)
                .values(startTime + 2000L, 12.9)
                .build();
        List<Serie> series = influxDB.query(dbName,
                "select value from " + timeSeries + " limit 3 order asc",
                TimeUnit.MILLISECONDS);
        Assert.assertNotNull(series);
        this.assertSeriesEquals(serieSource, series.get(0));
    }

    @Test
    public void testQueryTop() {
        String timeSeries = "queryTestTop";
        long startTime = System.currentTimeMillis();
        Serie generatedSerie = getSerie(timeSeries, startTime);
        influxDB.write(dbName, TimeUnit.MILLISECONDS, generatedSerie);
        startTime = startTime - 9000L;
        Serie serieSource = new Serie.Builder(timeSeries)
                .columns("time", "top")
                .values(startTime + 8000L, 18.9)
                .values(startTime + 7000L, 17.9)
                .values(startTime + 6000L, 16.9)
                .build();
        List<Serie> series = influxDB.query(dbName,
                "select top(value, 3) from " + timeSeries
                        + " where time > now() - 30s group by time(30s)",
                TimeUnit.MILLISECONDS);
        Assert.assertNotNull(series);
        this.assertSeriesEquals(serieSource, series.get(0));
    }

    @Test
    public void testQueryBottom() {
        String timeSeries = "queryTestBottom";
        long startTime = System.currentTimeMillis();
        Serie generatedSerie = getSerie(timeSeries, startTime);
        influxDB.write(dbName, TimeUnit.MILLISECONDS, generatedSerie);
        startTime = startTime - 9000L;
        Serie serieSource = new Serie.Builder(timeSeries)
                .columns("time", "bottom")
                .values(startTime, 10.9)
                .values(startTime + 1000L, 11.9)
                .values(startTime + 2000L, 12.9)
                .build();
        List<Serie> series = influxDB.query(dbName,
                "select bottom(value, 3) from " + timeSeries
                        + " where time > now() - 30s group by time(30s)",
                TimeUnit.MILLISECONDS);
        Assert.assertNotNull(series);
        this.assertSeriesEquals(serieSource, series.get(0));
    }

    @Test
    public void testQueryStddev() {
        String timeSeries = "queryTestStddev";
        long startTime = System.currentTimeMillis();
        Serie generatedSerie = getSerie(timeSeries, startTime);
        influxDB.write(dbName, TimeUnit.MILLISECONDS, generatedSerie);
        startTime = startTime - 9000L;
        Serie serieSource = new Serie.Builder(timeSeries)
                .columns("time", "stddev")
                .values(startTime + 8000L, 2.7386127875258)
                .build();
        List<Serie> series = influxDB.query(dbName,
                "select stddev(value) from " + timeSeries
                        + " where time > now() - 30s group by time(30s)",
                TimeUnit.MILLISECONDS);
        Assert.assertNotNull(series);
        this.assertSeriesEquals(serieSource, series.get(0));
    }

    @Test
    public void testQueryMean() {
        String timeSeries = "queryTestMean";
        long startTime = System.currentTimeMillis();
        Serie generatedSerie = getSerie(timeSeries, startTime);
        influxDB.write(dbName, TimeUnit.MILLISECONDS, generatedSerie);
        startTime = startTime - 9000L;
        Serie serieSource = new Serie.Builder(timeSeries)
                .columns("time", "mean")
                .values(startTime + 8000L, 14.9)
                .build();
        List<Serie> series = influxDB.query(dbName,
                "select mean(value) from " + timeSeries
                        + " where time > now() - 30s group by time(30s)",
                TimeUnit.MILLISECONDS);
        Assert.assertNotNull(series);
        this.assertSeriesEquals(serieSource, series.get(0));
    }

    @Test
    public void testQuerySum() {
        String timeSeries = "queryTestSum";
        long startTime = System.currentTimeMillis();
        Serie generatedSerie = getSerie(timeSeries, startTime);
        influxDB.write(dbName, TimeUnit.MILLISECONDS, generatedSerie);
        startTime = startTime - 9000L;
        Serie serieSource = new Serie.Builder(timeSeries)
                .columns("time", "sum")
                .values(startTime + 8000L, 134.1)
                .build();
        List<Serie> series = influxDB.query(dbName,
                "select sum(value) from " + timeSeries
                        + " where time > now() - 30s group by time(30s)",
                TimeUnit.MILLISECONDS);
        Assert.assertNotNull(series);
        this.assertSeriesEquals(serieSource, series.get(0));
    }

    private void assertSeriesEquals(Serie serieOne, Serie serieTwo) {
        Assert.assertNotNull(serieOne);
        Assert.assertNotNull(serieTwo);
        Assert.assertEquals(serieOne.getName(), serieTwo.getName());
        Assert.assertEquals(serieOne.getRows().size(), serieTwo.getRows().size());
        Assert.assertArrayEquals(serieOne.getColumns(), serieTwo.getColumns());
        for (int rowNo = 0; rowNo < serieOne.getRows().size(); rowNo++) {
            for (String column : serieOne.getColumns()) {
                if (column.equals("time")) {
                    Assert.assertEquals(Float.valueOf(serieOne.getRows().get(rowNo).get(column).toString()),
                            Float.valueOf(serieTwo.getRows().get(rowNo).get(column).toString()),
                            1e-7);
                } else {
                    Assert.assertEquals(Double.valueOf(serieOne.getRows().get(rowNo).get(column).toString()),
                            Double.valueOf(serieTwo.getRows().get(rowNo).get(column).toString()),
                            1e-13);
                }
            }
        }
    }

    private static Serie getSerie(String timeSeries, long startTime) {
        startTime = startTime - 9000L;//Minus N milliseconds from 'start time' based on number of rows going to write.
        return new Serie.Builder(timeSeries)
                .columns("time", "value")
                .values(startTime, 10.9)
                .values(startTime + 1000L, 11.9)
                .values(startTime + 2000L, 12.9)
                .values(startTime + 3000L, 13.9)
                .values(startTime + 4000L, 14.9)
                .values(startTime + 5000L, 15.9)
                .values(startTime + 6000L, 16.9)
                .values(startTime + 7000L, 17.9)
                .values(startTime + 8000L, 18.9)
                .build();
    }
}
