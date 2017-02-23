# Hawkular Dropwizard Reporter

## Usage

To use the Hawkular Dropwizard Reporter in any Java application:

* Pre-requisite: you must have a running instance of Hawkular Services or Hawkular Metrics. For more information see http://www.hawkular.org/

* Add a dependency to artifact _org.hawkular.metrics:hawkular-dropwizard-reporter_. Example with maven:
````
    <dependency>
        <groupId>org.hawkular.metrics</groupId>
        <artifactId>hawkular-dropwizard-reporter</artifactId>
        <version>0.25.0</version>
    </dependency>
````

* At some point in your application (generally at startup), create a _MetricRegistry_ and a _HawkularReporter_

Example:
````
        MetricRegistry registry = new MetricRegistry();
        HawkularReporter reporter = HawkularReporter.builder(registry, "my-tenant")
                .uri("http://myserver:8081")
                .build();
        reporter.start(1, TimeUnit.SECONDS);

        Meter meter = registry.meter("my.meter");

        // Later on, as appropriate:
        meter.mark();
````

For more information about Dropwizard Metrics usage, please refer to the official documentation: http://metrics.dropwizard.io/

### Using another HTTP client

The embedded HTTP client is designed to be as light as possible in terms of JAR dependencies. So, no Apache, no
Jetty... just a basic JDK URLConnection.

If you want to use a different HTTP client, you would just have to implement the interface `HawkularHttpClient` and pass an instance to the builder:
````
        HawkularReporter reporter = HawkularReporter.builder(registry, "my-tenant")
                // ...
                .useHttpClient(uri -> new MyHttpClient(uri))
                .build();
````

### Other builder options

* `basicAuth`: use when the Hawkular server requires an HTTP basic authentication.
* `bearerToken`: use when the Hawkular server requires a token-based authentication (like with OpenShift).
* `addHeader`: add any custom header you would like along with Hawkular requests.
* `prefixedWith`: add a prefix to all metrics when they are reported to Hawkular.
* `filter`: implement `MetricFilter` to exclude some metrics from reporting.
* `globalTags` / `addGlobalTag`: add one or more tags that will be applied to all metrics reported in Hawkular.
* `perMetricTags` / `addMetricTag`: add one or more tags that will be applied to the specified metric(s) in Hawkular.
* `addRegexTag`: same as above, but using a regexp pattern for metrics selection.
* `disableTagComposition`: turn off automatic tagging of metric composition when applicable (for exemple, a Meter metric will generate a tag "meter:5minrt" on its 5-minutes-rate component).
* `metricComposition` / `setMetricComposition`: set composing parts for a given metric name (see section "How it works" below, for more information). By default, all available parts are used.
* `setRegexMetricComposition`: same as above, using regexp.
* `failoverCacheDuration` (+InXX): set the failover cache duration. This cache is used to store post attempts in memory when the Hawkular server cannot be reached. Default is 10 minutes.
* `failoverCacheMaxSize`: set the failover cache maximum size, in number of requests. Unset by default (ie. there's no maximum).

## Usage in a Dropwizard application

You can use the Hawkular Dropwizard Reporter in a Dropwizard application by simply editing your app .yml file and
configure a reporter of type _hawkular_. For example:
````
metrics:
  reporters:
    - type: hawkular
      uri: http://myserver:8081
      tenant: my-tenant
````

You must also add a module dependency to hawkular-dropwizard-reporter-factory (it's a different module from the
 above one):
````
    <dependency>
        <groupId>org.hawkular.metrics</groupId>
        <artifactId>hawkular-dropwizard-reporter-factory</artifactId>
        <version>0.25.0</version>
    </dependency>
````

## Usage as an "addthis" plugin in Cassandra

* Copy and edit `sample/hawkular-cassandra-example.yml` to `[cassandra]/conf`
* Copy the shaded JAR artifact `hawkular-dropwizard-reporter-xxx-shaded.jar` to `[cassandra]/lib`
* Start Cassandra with `-Dcassandra.metricsReporterConfigFile=hawkular-cassandra-example.yml`

Note: this usage requires a version of the `reporter-config3` JAR, in Cassandra, that is not merged upstream yet. See
 fork here: https://github.com/jotak/metrics-reporter-config

## How it works

Dropwizard sends to this Hawkular reporter a list of datapoints for various metrics. Each Dropwizard datapoint can be
a **Gauge**, a **Counter**, an **Histogram**, a **Meter** or a **Timer**. However, Hawkular has a simpler data model
consisting in just **Gauges** and **Counters** (there's also Availability and Strings, but they aren't used here). So
there's a bit of transformation that happens here, described as follow:

Dropwizard type | Hawkular conversion
--------------- | -------------------
Gauge           | Gauge (unchanged)
Counter         | Counter (unchanged)
Histogram       | 10 gauges and 1 counter
Meter           | 4 gauges and 1 counter
Timer           | 14 gauges and 1 counter

So a single metric in Dropwizard may be "exploded" into several metrics in Hawkular. These exploded metrics share
the same base name (ie. the dropwizard metric name), with an appended suffix that explains what kind of data it represents.
For example, a Meter metric named _my.metered.metric_ in dropwizard will be converted in Hawkular in several metrics
such as _my.metered.metric.1minrt_, _my.metered.metric.meanrt_, etc. All suffixes are explained as follow:

Suffix | Description | Applies to
------ | ----------- | ----------
.count | Counts number of ticks / occurrences ([javadoc](http://metrics.dropwizard.io/3.1.0/apidocs/com/codahale/metrics/Metered.html#getCount--)) | Histograms, Meters and Timers
.1minrt | One minute rate ([javadoc](http://metrics.dropwizard.io/3.1.0/apidocs/com/codahale/metrics/Metered.html#getOneMinuteRate--)) | Meters and Timers
.5minrt | Five minutes rate ([javadoc](http://metrics.dropwizard.io/3.1.0/apidocs/com/codahale/metrics/Metered.html#getFiveMinuteRate--)) | Meters and Timers
.15minrt | Fifteen minutes rate ([javadoc](http://metrics.dropwizard.io/3.1.0/apidocs/com/codahale/metrics/Metered.html#getFifteenMinuteRate--)) | Meters and Timers
.meanrt | Mean rate ([javadoc](http://metrics.dropwizard.io/3.1.0/apidocs/com/codahale/metrics/Metered.html#getMeanRate--)) | Meters and Timers
.min | Minimum ([javadoc](http://metrics.dropwizard.io/3.1.0/apidocs/com/codahale/metrics/Snapshot.html#getMin--)) | Histograms and Timers
.max | Maximum ([javadoc](http://metrics.dropwizard.io/3.1.0/apidocs/com/codahale/metrics/Snapshot.html#getMax--)) | Histograms and Timers
.median | Median ([javadoc](http://metrics.dropwizard.io/3.1.0/apidocs/com/codahale/metrics/Snapshot.html#getMedian--)) | Histograms and Timers
.mean | Mean ([javadoc](http://metrics.dropwizard.io/3.1.0/apidocs/com/codahale/metrics/Snapshot.html#getMean--)) | Histograms and Timers
.stddev | Standard deviation ([javadoc](http://metrics.dropwizard.io/3.1.0/apidocs/com/codahale/metrics/Snapshot.html#getStdDev--)) | Histograms and Timers
.75perc | 75th percentile ([javadoc](http://metrics.dropwizard.io/3.1.0/apidocs/com/codahale/metrics/Snapshot.html#get75thPercentile--)) | Histograms and Timers
.95perc | 95th percentile ([javadoc](http://metrics.dropwizard.io/3.1.0/apidocs/com/codahale/metrics/Snapshot.html#get95thPercentile--)) | Histograms and Timers
.98perc | 98th percentile ([javadoc](http://metrics.dropwizard.io/3.1.0/apidocs/com/codahale/metrics/Snapshot.html#get98thPercentile--)) | Histograms and Timers
.99perc | 99th percentile ([javadoc](http://metrics.dropwizard.io/3.1.0/apidocs/com/codahale/metrics/Snapshot.html#get99thPercentile--)) | Histograms and Timers
.999perc | 999th percentile ([javadoc](http://metrics.dropwizard.io/3.1.0/apidocs/com/codahale/metrics/Snapshot.html#get999thPercentile--)) | Histograms and Timers

Note that, because these suffixes are only added on the Hawkular metric names and not on Dropwizard metric names, you
cannot use dropwizard metric filters to blacklist metrics based on their suffixes (for instance, all 75th percentile metrics).

However you can use the `HawkularReporterBuilder.metricComposition` (or other variations `setMetricComposition` and `setRegexMetricComposition`) to keep only the suffixes you want for a given metric or metrics pattern.
For instance:
````
        HawkularReporter reporter = HawkularReporter.builder(registry, "my-tenant")
            .setMetricComposition("my-metric", Lists.newArrayList("1minrt", "meanrt", "count"))
            .build();
````
