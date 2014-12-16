package org.rhq.metrics.restServlet;

import static java.lang.Double.NaN;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_XML;
import static javax.ws.rs.core.Response.ResponseBuilder;
import static javax.ws.rs.core.Response.Status;
import static org.rhq.metrics.core.MetricsService.DEFAULT_TENANT_ID;
import static org.rhq.metrics.restServlet.CustomMediaTypes.APPLICATION_VND_RHQ_WRAPPED_JSON;
import static org.rhq.metrics.restServlet.MetricHandler.createPointInSimpleBucket;
import static org.rhq.metrics.restServlet.MetricHandler.getBucketDataPoint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
import javax.ws.rs.core.Response;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;

import org.jboss.resteasy.annotations.GZIP;

import org.rhq.metrics.core.MetricId;
import org.rhq.metrics.core.MetricsService;
import org.rhq.metrics.core.NumericData;
import org.rhq.metrics.core.NumericMetric2;

/**
 * @author Thomas Segismont
 */
@Path("/metrics")
public class LegacyMetricsHandler {
    private static final long EIGHT_HOURS = MILLISECONDS.convert(8, HOURS);

    @Inject
    private MetricsService metricsService;

    @GZIP
    @GET
    @Path("/metrics")
    @Produces({APPLICATION_JSON,APPLICATION_XML,APPLICATION_VND_RHQ_WRAPPED_JSON})
    public void listMetrics(@Suspended AsyncResponse asyncResponse, @QueryParam("q") String filter) {

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
                ResponseBuilder builder = Response.ok(list);

                asyncResponse.resume(builder.build());

            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    // NOTE: This endpoint will be replaced or changed once the schema-changes branch is
    // merged into master. The request body needs to map to either a single metric or a
    // collection of metrics.
    @POST
    @Consumes({ APPLICATION_JSON, APPLICATION_XML })
    @ApiOperation("Add a collection of data. Values can be for different metric ids.")
    public void addMetrics(@Suspended AsyncResponse asyncResponse, Collection<IdDataPoint> dataPoints) {
        if (dataPoints.isEmpty()) {
            asyncResponse.resume(Response.ok().type(APPLICATION_JSON_TYPE).build());
        }

        List<NumericMetric2> metrics = new ArrayList<>();
        NumericMetric2 metric = null;
        for (IdDataPoint p : dataPoints) {
            if (metric == null) {
                metric = new NumericMetric2(DEFAULT_TENANT_ID, new MetricId(p.getId()));
            } else {
                if (!p.getId().equals(metric.getId().getName())) {
                    metrics.add(metric);
                    metric = new NumericMetric2(DEFAULT_TENANT_ID, new MetricId(p.getId()));
                }
            }
            metric.addData(p.getTimestamp(), (Double) p.getValue());
        }
        metrics.add(metric);

        ListenableFuture<Void> future = metricsService.addNumericData(metrics);
        Futures.addCallback(future, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void errors) {
                Response jaxrs = Response.ok().type(APPLICATION_JSON_TYPE).build();
                asyncResponse.resume(jaxrs);
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @GZIP
    @GET
    @Path("/{id}")
    @ApiOperation("Return metrical values for a given metric id. If no parameters are given, the raw data "
        + "for a time period of [now-8h,now] is returned.")
    @Produces({ APPLICATION_JSON, APPLICATION_XML, APPLICATION_VND_RHQ_WRAPPED_JSON })
    public void getDataForId(@Suspended AsyncResponse asyncResponse,
        @ApiParam("Id of the metric to return data for") @PathParam("id") String id,
        @ApiParam(value = "Start time in millis since epoch", defaultValue = "Now - 8h")
            @QueryParam("start") Long start,
        @ApiParam(value = "End time in millis since epoch", defaultValue = "Now") @QueryParam("end") Long end,
        @ApiParam(value = "If non-zero: number of buckets to partition the data into. Raw data otherwise",
            defaultValue = "0")
        @QueryParam("buckets") int numberOfBuckets,
        @QueryParam("bucketWidthSeconds") int bucketWidthSeconds,
        @ApiParam("If true, empty buckets are not returned.") @QueryParam("skipEmpty") @DefaultValue("false")
            boolean skipEmpty,
        @QueryParam("bucketCluster") @DefaultValue("true") boolean bucketCluster,
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

                NumericMetric2 metric = new NumericMetric2(DEFAULT_TENANT_ID, new MetricId(id));
                final ListenableFuture<List<NumericData>> future = metricsService
                    .findData(metric, finalStart, finalEnd);

                Futures.addCallback(future, new FutureCallback<List<NumericData>>() {
                    @Override
                    public void onSuccess(List<NumericData> metrics) {
                        if (numberOfBuckets == 0) {
                            // Normal case of raw metrics
                            List<DataPoint> points = new ArrayList<>(metrics.size());
                            for (NumericData item : metrics) {
                                DataPoint point = new DataPoint(item.getTimestamp(), item.getValue());
                                points.add(point);
                            }
                            GenericEntity<List<DataPoint>> list = new GenericEntity<List<DataPoint>>(points) {
                            };
                            Response jaxrs = Response.ok(list).build();
                            asyncResponse.resume(jaxrs);

                        } else {
                            // User wants data in buckets
                            List<BucketDataPoint> points = new ArrayList<>(numberOfBuckets);
                            if (bucketWidthSeconds == 0) {
                                // we will have numberOfBuckets buckets over the whole time span

                                long bucketsize = (finalEnd - finalStart) / numberOfBuckets;
                                for (int i = 0; i < numberOfBuckets; i++) {
                                    long startTime = finalStart + i * bucketsize;

                                    BucketDataPoint point = createPointInSimpleBucket(id, startTime, bucketsize,
                                        metrics);
                                    if (!skipEmpty || !point.isEmpty()) {
                                        points.add(point);
                                    }
                                }
                            } else {
                                // we will have numberOfBuckets buckets, but with a fixed with. Buckets will thus
                                // be reused over time after (numberOfBuckets*bucketWidthSeconds seconds)
                                long totalLength = (long) numberOfBuckets * bucketWidthSeconds * 1000L;

                                // find the minimum ts
                                long minTs = Long.MAX_VALUE;
                                for (NumericData metric : metrics) {
                                    if (metric.getTimestamp() < minTs) {
                                        minTs = metric.getTimestamp();
                                    }
                                }

                                TLongObjectMap<List<NumericData>> buckets = new TLongObjectHashMap<>(numberOfBuckets);

                                for (NumericData metric : metrics) {
                                    long bucket = metric.getTimestamp() - minTs;
                                    bucket = bucket % totalLength;
                                    bucket = bucket / (bucketWidthSeconds * 1000L);
                                    List<NumericData> tmpList = buckets.get(bucket);
                                    if (tmpList == null) {
                                        tmpList = new ArrayList<>();
                                        buckets.put(bucket, tmpList);
                                    }
                                    tmpList.add(metric);
                                }
                                // Now that stuff is in buckets - we need to "flatten" them out.
                                // As we collapse stuff from a lot of input timestamps into some
                                // buckets, we only use a relative time for the bucket timestamps.
                                if (bucketCluster)
                                    for (int i = 0; i < numberOfBuckets; i++) {
                                        List<NumericData> tmpList = buckets.get(i);
                                        BucketDataPoint point;
                                        if (tmpList == null) {
                                            if (!skipEmpty) {
                                                point = new BucketDataPoint(id, 1000L * i * bucketWidthSeconds, NaN,
                                                    NaN, NaN);
                                                points.add(point);
                                            }
                                        } else {
                                            point = getBucketDataPoint(tmpList.get(0).getMetric().getId().getName(),
                                                1000L * i * bucketWidthSeconds, tmpList);
                                            points.add(point);
                                        }

                                    }
                                else {
                                    // We want to keep the raw values, but put them into clusters anyway
                                    // without collapsing them into a single min/avg/max tuple
                                    for (int i = 0; i < numberOfBuckets; i++) {
                                        List<NumericData> tmpList = buckets.get(i);
                                        BucketDataPoint point;
                                        if (tmpList != null) {
                                            for (NumericData metric : tmpList) {
                                                point = new BucketDataPoint(id, // TODO could be simple data points
                                                    1000L * i * bucketWidthSeconds, NaN, metric.getValue(), NaN);
                                                point.setValue(metric.getValue());
                                                points.add(point);
                                            }
                                        }

                                    }
                                }
                            }

                            GenericEntity<List<BucketDataPoint>> list
                                = new GenericEntity<List<BucketDataPoint>>(points) {};
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

    @POST
    @Path("/{id}")
    @Consumes({ APPLICATION_JSON, APPLICATION_XML })
    @ApiOperation("Adds a single data point for the given id.")
    public void addMetric(@Suspended AsyncResponse asyncResponse, @PathParam("id") String id, IdDataPoint dataPoint) {
        dataPoint.setId(id);
        addMetrics(asyncResponse, asList(dataPoint));
    }

    @DELETE
    @Path("/{id}")
    @Produces({ APPLICATION_JSON, APPLICATION_XML })
    public void deleteMetric(@Suspended AsyncResponse asyncResponse, @PathParam("id") String id) {

        ListenableFuture<Boolean> idExistsFuture = metricsService.idExists(id);
        Futures.addCallback(idExistsFuture, new FutureCallback<Boolean>() {
            @Override
            public void onSuccess(Boolean result) {
                ListenableFuture<Boolean> future = metricsService.deleteMetric(id);
                Futures.addCallback(future, new FutureCallback<Boolean>() {
                    @Override
                    public void onSuccess(Boolean result) {
                        ResponseBuilder builder;
                        if (result) {
                            StringValue deleted = new StringValue("Metric with id " + id + " deleted");
                            builder = Response.ok(deleted);
                        } else {
                            StringValue res = new StringValue("Deletion failed");
                            builder = Response.status(Status.INTERNAL_SERVER_ERROR).entity(res);
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
}
