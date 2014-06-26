package org.rhq.metrics.restServlet;

import static java.lang.Double.NaN;
import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jboss.resteasy.annotations.GZIP;

import org.rhq.metrics.core.Counter;
import org.rhq.metrics.core.MetricsService;
import org.rhq.metrics.core.NumericMetric;
import org.rhq.metrics.core.RawNumericMetric;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;

/**
 * Interface to deal with metrics
 * @author Heiko W. Rupp
 */
@Api(value = "Related to metrics")
@Path("/")
public class MetricHandler {

    private static final Logger logger = LoggerFactory.getLogger(MetricHandler.class);
    private static final long EIGHT_HOURS = 8L*60L*60L*1000L; // 8 Hours in milliseconds

    @Inject
    private MetricsService metricsService;

    public MetricHandler() {
        if (logger.isDebugEnabled()) {
            logger.debug("MetricHandler instantiated");
        }
    }

	@GET
    @POST
	@Path("/ping")
	@Consumes({ "application/json", "application/xml" })
	@Produces({ "application/json", "application/xml","application/vnd.rhq.wrapped+json" })
    @ApiOperation(value = "Returns the current time and serves to check for the availability of the api.", responseClass = "Map<String,String>")
	public Response ping() {

        StringValue reply = new StringValue(new Date().toString());

		Response.ResponseBuilder builder = Response.ok(reply);
		return builder.build();
	}

    @POST
    @Path("/metrics/{id}")
    @Consumes({"application/json","application/xml"})
    @ApiOperation("Adds a single data point for the given id.")
    public void addMetric(@Suspended AsyncResponse asyncResponse, @PathParam("id") String id, IdDataPoint dataPoint) {
        addData(asyncResponse, ImmutableSet.of(new RawNumericMetric(id, dataPoint.getValue(),
            dataPoint.getTimestamp())));
    }

    @POST
    @Path("/metrics")
    @Consumes({"application/json","application/xml"})
    @ApiOperation("Add a collection of data. Values can be for different metric ids.")
    public void addMetrics(@Suspended AsyncResponse asyncResponse, Collection<IdDataPoint> dataPoints) {

        Set<RawNumericMetric> rawSet = new HashSet<>(dataPoints.size());
        for (IdDataPoint dataPoint : dataPoints) {
            RawNumericMetric rawMetric = new RawNumericMetric(dataPoint.getId(), dataPoint.getValue(),
                dataPoint.getTimestamp());
            rawSet.add(rawMetric);
        }

        addData(asyncResponse, rawSet);
    }

    private void addData(final AsyncResponse asyncResponse, Set<RawNumericMetric> rawData) {
        ListenableFuture<Map<RawNumericMetric,Throwable>> future = metricsService.addData(rawData);
        Futures.addCallback(future, new FutureCallback<Map<RawNumericMetric, Throwable>>() {
            @Override
            public void onSuccess(Map<RawNumericMetric, Throwable> errors) {
                Response jaxrs = Response.ok().type(MediaType.APPLICATION_JSON_TYPE).build();
                asyncResponse.resume(jaxrs);
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @POST
    @Path("/counters")
    @Produces({"application/json"})
    public void updateCountersForGroups(@Suspended final AsyncResponse asyncResponse, Collection<Counter> counters) {
        updateCounters(asyncResponse, counters);
    }

    @POST
    @Path("/counters/{group}")
    @Produces("application/json")
    public void updateCounterForGroup(@Suspended final AsyncResponse asyncResponse, @PathParam("group") String group,
        Collection<Counter> counters) {
        for (Counter counter : counters) {
            counter.setGroup(group);
        }
        updateCounters(asyncResponse, counters);
    }

    private void updateCounters(final AsyncResponse asyncResponse, Collection<Counter> counters) {
        ListenableFuture<Void> future = metricsService.updateCounters(counters);
        Futures.addCallback(future, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void result) {
                Response jaxrs = Response.ok().type(MediaType.APPLICATION_JSON_TYPE).build();
                asyncResponse.resume(jaxrs);
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @POST
    @Path("/counters/{group}/{counter}")
    public void updateCounter(@Suspended final AsyncResponse asyncResponse, @PathParam("group") String group,
        @PathParam("counter") String counter) {
        updateCounterValue(asyncResponse, group, counter, 1L);
    }

    @POST
    @Path("/counters/{group}/{counter}/{value}")
    public void updateCounter(@Suspended final AsyncResponse asyncResponse, @PathParam("group") String group,
        @PathParam("counter") String counter, @PathParam("value") Long value) {
        updateCounterValue(asyncResponse, group, counter, value);
    }

    private void updateCounterValue(final AsyncResponse asyncResponse, String group, String counter, Long value) {
        ListenableFuture<Void> future = metricsService.updateCounter(new Counter(group, counter, value));
        Futures.addCallback(future, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void result) {
                Response jaxrs = Response.ok().type(MediaType.APPLICATION_JSON_TYPE).build();
                asyncResponse.resume(jaxrs);
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @GET
    @Path("/counters/{group}")
    @Produces({"application/json","application/vnd.rhq.wrapped+json"})
    public void getCountersForGroup(@Suspended final AsyncResponse asyncResponse, @PathParam("group") String group) {
        ListenableFuture<List<Counter>> future = metricsService.findCounters(group);
        Futures.addCallback(future, new FutureCallback<List<Counter>>() {
            @Override
            public void onSuccess(List<Counter> counters) {
                Response jaxrs = Response.ok(counters).type(MediaType.APPLICATION_JSON_TYPE).build();
                asyncResponse.resume(jaxrs);
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @GET
    @Path("/counters/{group}/{counter}")
    @Produces({"application/json","application/vnd.rhq.wrapped+json"})
    public void getCounter(@Suspended final AsyncResponse asyncResponse, @PathParam("group") final String group,
        @PathParam("counter") final String counter) {
        ListenableFuture<List<Counter>> future = metricsService.findCounters(group, asList(counter));
        Futures.addCallback(future, new FutureCallback<List<Counter>>() {
            @Override
            public void onSuccess(List<Counter> counters) {
                if (counters.isEmpty()) {
                    asyncResponse.resume(Response.status(404).entity("Counter[group: " + group + ", name: " +
                        counter + "] not found").build());
                } else {
                    Response jaxrs = Response.ok(counters.get(0)).type(MediaType.APPLICATION_JSON_TYPE).build();
                    asyncResponse.resume(jaxrs);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @GZIP
    @GET
    @Path("/metrics/{id}")
    @ApiOperation("Return metrical values for a given metric id. If no parameters are given, the raw data " +
        "for a time period of [now-8h,now] is returned.")
    @Produces({"application/json","application/xml","application/vnd.rhq.wrapped+json"})
    public void getDataForId(@Suspended final AsyncResponse asyncResponse,
        @ApiParam("Id of the metric to return data for") @PathParam("id") final String id,
        @ApiParam(value = "Start time in millis since epoch", defaultValue = "Now - 8h") @QueryParam("start") Long start,
        @ApiParam(value = "End time in millis since epoch",defaultValue = "Now") @QueryParam("end") Long end,
        @ApiParam(value = "If non-zero: number of buckets to partition the data into. Raw data otherwise", defaultValue = "0")
            @QueryParam("buckets") final int numberOfBuckets,
        @QueryParam("bucketWidthSeconds") final int bucketWidthSeconds,
        @ApiParam("If true, empty buckets are not returned.") @QueryParam("skipEmpty") @DefaultValue("false") final boolean skipEmpty,
        @QueryParam("bucketCluster") @DefaultValue("true") final boolean bucketCluster,
        @Context HttpHeaders headers) {

        long now = System.currentTimeMillis();
        if (start == null) {
            start = now - EIGHT_HOURS;
        }
        if (end == null) {
            end = now;
        }
        final Long finalStart = start;
        final Long finalEnd = end;


        ListenableFuture<Boolean> idExistsFuture = metricsService.idExists(id);
        Futures.addCallback(idExistsFuture, new FutureCallback<Boolean>() {
            @Override
            public void onSuccess(Boolean result) {
                if (!result) {
                    StringValue val = new StringValue("Metric with id [" + id + "] not found. ");
                    asyncResponse.resume(Response.status(404).entity(val).build());
                }

                final ListenableFuture<List<RawNumericMetric>> future = metricsService.findData(id, finalStart, finalEnd);

                Futures.addCallback(future, new FutureCallback<List<RawNumericMetric>>() {
                    @Override
                    public void onSuccess(List<RawNumericMetric> metrics) {
                        if (numberOfBuckets == 0) {
                            // Normal case of raw metrics
                            List<DataPoint> points = new ArrayList<>(metrics.size());
                            for (NumericMetric item : metrics) {
                                DataPoint point = new DataPoint(item.getTimestamp(), item.getAvg());
                                points.add(point);
                            }
                            GenericEntity<List<DataPoint>> list = new GenericEntity<List<DataPoint>>(points) {};
                            Response jaxrs = Response.ok(list).build();
                            asyncResponse.resume(jaxrs);

                        } else {
                            // User wants data in buckets
                            List<BucketDataPoint> points = new ArrayList<>(numberOfBuckets);
                            if (bucketWidthSeconds == 0) {
                                // we will have numberOfBuckets buckets over the whole time span

                                long bucketsize = (finalEnd - finalStart) / numberOfBuckets;
                                for (int i = 0; i < numberOfBuckets; i++) {
                                    long startTime = finalStart + i*bucketsize;

                                    BucketDataPoint point = createPointInSimpleBucket(id, startTime, bucketsize, metrics);
                                    if (!skipEmpty || !point.isEmpty()) {
                                        points.add(point);
                                    }
                                }
                            } else {
                                // we will have numberOfBuckets buckets, but with a fixed with. Buckets will thus
                                // be reused over time after (numberOfBuckets*bucketWidthSeconds seconds)
                                long totalLength = numberOfBuckets * bucketWidthSeconds * 1000L ;

                                // find the minimum ts
                                long minTs = Long.MAX_VALUE;
                                for (RawNumericMetric metric : metrics) {
                                    if (metric.getTimestamp() < minTs) {
                                        minTs = metric.getTimestamp();
                                    }
                                }

                                TLongObjectMap<List<RawNumericMetric>> buckets = new TLongObjectHashMap<>(numberOfBuckets);

                                for (RawNumericMetric metric : metrics) {
                                    long bucket = metric.getTimestamp() - minTs;
                                    bucket = bucket % totalLength;
                                    bucket = bucket / (bucketWidthSeconds * 1000L);
                                    List<RawNumericMetric> tmpList = buckets.get(bucket);
                                    if (tmpList == null) {
                                        tmpList = new ArrayList<>();
                                        buckets.put(bucket, tmpList);
                                    }
                                    tmpList.add(metric);
                                }
                                if (bucketCluster) {
                                    // Now that stuff is in buckets - we need to "flatten" them out.
                                    // As we collapse stuff from a lot of input timestamps into some
                                    // buckets, we only use a relative time for the bucket timestamps.
                                    for (int i = 0; i < numberOfBuckets; i++) {
                                        List<RawNumericMetric> tmpList = buckets.get(i);
                                        BucketDataPoint point;
                                        if (tmpList == null) {
                                            if (!skipEmpty) {
                                                point = new BucketDataPoint(id, i * bucketWidthSeconds * 1000L, NaN, NaN, NaN);
                                                points.add(point);
                                            }
                                        } else {
                                            point = getBucketDataPoint(tmpList.get(0).getId(),
                                                i * bucketWidthSeconds * 1000L, tmpList);
                                            points.add(point);
                                        }

                                    }
                                } else {
                                    // We want to keep the raw values, but put them into clusters anyway
                                    // without collapsing them into a single min/avg/max tuple
                                    for (int i = 0; i < numberOfBuckets; i++) {
                                        List<RawNumericMetric> tmpList = buckets.get(i);
                                        BucketDataPoint point;
                                        if (tmpList!=null) {
                                            for (RawNumericMetric metric : tmpList) {
                                                point = new BucketDataPoint(id, // TODO could be simple data points
                                                    i * bucketWidthSeconds * 1000L, NaN,metric.getValue(),NaN);
                                                point.setValue(metric.getValue());
                                                points.add(point);
                                            }
                                        }

                                    }

                                }
                            }

                            GenericEntity<List<BucketDataPoint>> list = new GenericEntity<List<BucketDataPoint>>(points) {};
                            Response jaxrs = Response.ok(list).build();
                            asyncResponse.resume(jaxrs);
                        }

                    }

                    @Override
                    public void onFailure(Throwable t) {
                        asyncResponse.resume(t);
                    }
                });
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @GZIP
    @GET
    @Path("/metrics")
    @Produces({"application/json","application/xml","application/vnd.rhq.wrapped+json"})
    public void listMetrics(@Suspended final AsyncResponse asyncResponse, @QueryParam("q") final String filter) {

        ListenableFuture<List<String>> future = ServiceKeeper.getInstance().service.listMetrics();
        Futures.addCallback(future, new FutureCallback<List<String>>() {
            @Override
            public void onSuccess(List<String> result) {
                final List<SimpleLink> listWithLinks = new ArrayList<>(result.size());
                for (String name : result) {
                    if ((filter == null || filter.isEmpty()) || (name.contains(filter))) {
                        SimpleLink link = new SimpleLink("metrics", "/rhq-metrics/metrics/" + name + "/", name);
                        listWithLinks.add(link);
                    }
                }

                GenericEntity<List<SimpleLink>> list = new GenericEntity<List<SimpleLink>>(listWithLinks) {} ;
                Response.ResponseBuilder builder = Response.ok(list);

                asyncResponse.resume(builder.build());

            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @DELETE
    @Path("/metrics/{id}")
    @Produces({"application/json","application/xml"})
    public void deleteMetric(@Suspended final AsyncResponse asyncResponse, @PathParam("id") final String id) {

        ListenableFuture<Boolean> idExistsFuture = metricsService.idExists(id);
        Futures.addCallback(idExistsFuture, new FutureCallback<Boolean>() {
            @Override
            public void onSuccess(Boolean result) {
                ListenableFuture<Boolean> future = metricsService.deleteMetric(id);
                Futures.addCallback(future, new FutureCallback<Boolean>() {
                    @Override
                    public void onSuccess(Boolean result) {
                        Response.ResponseBuilder builder;
                        if (result) {
                            StringValue deleted = new StringValue("Metric with id " + id + " deleted");
                            builder = Response.ok(deleted);
                        } else {
                            StringValue res = new StringValue("Deletion failed");
                            builder = Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(res);
                        }
                        asyncResponse.resume(builder.build());
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        asyncResponse.resume(t);
                    }
                });
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }

        });

    }

    private BucketDataPoint createPointInSimpleBucket(String id, long startTime, long bucketsize,
                                                      List<RawNumericMetric> metrics) {
        List<RawNumericMetric> bucketMetrics = new ArrayList<>(metrics.size());
        // Find matching metrics
        for (NumericMetric raw : metrics) {
            if (raw.getTimestamp() >= startTime && raw.getTimestamp() < startTime + bucketsize) {
                bucketMetrics.add((RawNumericMetric) raw);
            }
        }

        return getBucketDataPoint(id, startTime, bucketMetrics);
    }

    private BucketDataPoint getBucketDataPoint(String id, long startTime, List<RawNumericMetric> bucketMetrics) {
        Double min = null;
        Double max = null;
        double sum = 0;
        for (RawNumericMetric raw : bucketMetrics) {
            if (max==null || raw.getValue() > max) {
                max = raw.getValue();
            }
            if (min==null || raw.getValue() < min) {
                min = raw.getValue();
            }
            sum += raw.getValue();
        }
        double avg = bucketMetrics.size()>0 ? sum / bucketMetrics.size() : NaN;
        if (min == null) {
            min = NaN;
        }
        if (max == null) {
            max = NaN;
        }
        BucketDataPoint result = new BucketDataPoint(id,startTime,min, avg,max);

        return result;
    }

}
