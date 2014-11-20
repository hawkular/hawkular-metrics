package org.rhq.metrics.restServlet;

import static java.lang.Double.NaN;
import static java.util.Arrays.asList;
import static org.rhq.metrics.core.MetricsService.DEFAULT_TENANT_ID;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;

import org.jboss.resteasy.annotations.GZIP;

import org.rhq.metrics.core.Availability;
import org.rhq.metrics.core.AvailabilityMetric;
import org.rhq.metrics.core.Counter;
import org.rhq.metrics.core.MetricData;
import org.rhq.metrics.core.MetricId;
import org.rhq.metrics.core.MetricsService;
import org.rhq.metrics.core.NumericData;
import org.rhq.metrics.core.NumericMetric2;
import org.rhq.metrics.core.Tag;
import org.rhq.metrics.core.Tenant;

/**
 * Interface to deal with metrics
 * @author Heiko W. Rupp
 */
@Api(value = "Related to metrics")
@Path("/")
public class MetricHandler {
    private static final long EIGHT_HOURS = MILLISECONDS.convert(8, HOURS);

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
    @Consumes({"application/json", "application/xml"})
    @Produces({"application/json", "application/xml", "application/vnd.rhq.wrapped+json"})
    @ApiOperation(value = "Returns the current time and serves to check for the availability of the api.",
        responseClass = "Map<String,String>")
    public Response ping() {

        StringValue reply = new StringValue(new Date().toString());

        Response.ResponseBuilder builder = Response.ok(reply);
        return builder.build();
    }

    @POST
    @Path("/tenants")
    @Consumes("application/json")
    public void createTenant(@Suspended final AsyncResponse asyncResponse, TenantParams params) {
        Tenant tenant = new Tenant().setId(params.getId());
        ListenableFuture<Void> insertFuture = metricsService.createTenant(tenant);
        Futures.addCallback(insertFuture, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void result) {
                asyncResponse.resume(Response.ok().type(MediaType.APPLICATION_JSON_TYPE).build());
            }

            @Override
            public void onFailure(Throwable t) {
                Map<String, String> errors = ImmutableMap.of("errorMsg", t.getMessage());
                asyncResponse.resume(Response.status(Response.Status.FORBIDDEN).entity(errors).type(
                    MediaType.APPLICATION_JSON_TYPE).build());
            }
        });
    }

    @POST
    @Path("/metrics/{id}")
    @Consumes({"application/json","application/xml"})
    @ApiOperation("Adds a single data point for the given id.")
    public void addMetric(@Suspended AsyncResponse asyncResponse, @PathParam("id") String id, IdDataPoint dataPoint) {
        addMetrics(asyncResponse, asList(dataPoint));
    }

    // NOTE: This endpoint will be replaced or changed once the schema-changes branch is
    // merged into master. The request body needs to map to either a single metric or a
    // collection of metrics.
    @POST
    @Path("/metrics")
    @Consumes({"application/json","application/xml"})
    @ApiOperation("Add a collection of data. Values can be for different metric ids.")
    public void addMetrics(@Suspended final AsyncResponse asyncResponse, Collection<IdDataPoint> dataPoints) {
        if (dataPoints.isEmpty()) {
            asyncResponse.resume(Response.ok().type(MediaType.APPLICATION_JSON_TYPE).build());
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
            metric.addData(p.getTimestamp(), p.getValue());
        }
        metrics.add(metric);

        ListenableFuture<Void> future = metricsService.addNumericData(metrics);
        Futures.addCallback(future, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void errors) {
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
    @Path("/numeric/{id}")
    @Consumes("application/json")
    public void addDataForMetric(@Suspended final AsyncResponse asyncResponse, @PathParam("id") String id,
        NumericDataParams params) {

        NumericMetric2 metric = new NumericMetric2(DEFAULT_TENANT_ID, new MetricId(id));

        for (NumericDataPoint p : params.getData()) {
            metric.addData(p.getTimestamp(), p.getValue());
        }

        ListenableFuture<Void> future = metricsService.addNumericData(asList(metric));
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
    @Path("/availability/{id}")
    @Consumes("application/json")
    public void addAvailabilityForMetric(@Suspended final AsyncResponse asyncResponse, @PathParam("id") String id,
        AvailabilityDataParams params) {
        AvailabilityMetric metric = new AvailabilityMetric(DEFAULT_TENANT_ID, new MetricId(id), params.getAttributes());

        for (AvailabilityDataPoint p : params.getData()) {
            metric.addData(new Availability(metric, p.getTimestamp(), p.getValue()));
        }

        ListenableFuture<Void> future = metricsService.addAvailabilityData(asList(metric));
        Futures.addCallback(future, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void result) {
                asyncResponse.resume(Response.ok().type(MediaType.APPLICATION_JSON_TYPE).build());
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @POST
    @Path("/numeric")
    @Consumes("application/json")
    public void addNumericData(@Suspended final AsyncResponse asyncResponse, List<NumericDataParams> paramsList) {
        if (paramsList.isEmpty()) {
            asyncResponse.resume(Response.ok().type(MediaType.APPLICATION_JSON_TYPE).build());
        }

        List<NumericMetric2> metrics = new ArrayList<>(paramsList.size());

        for (NumericDataParams params : paramsList) {
            NumericMetric2 metric = new NumericMetric2(DEFAULT_TENANT_ID, new MetricId(params.getName()),
                params.getAttributes());
            for (NumericDataPoint p : params.getData()) {
                metric.addData(p.getTimestamp(), p.getValue());
            }
            metrics.add(metric);
        }
        ListenableFuture<Void> insertFuture = metricsService.addNumericData(metrics);
        Futures.addCallback(insertFuture, new FutureCallback<Void>() {
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
    @Path("/availability")
    @Consumes("application/json")
    public void addAvailabilityData(@Suspended final AsyncResponse asyncResponse,
        List<AvailabilityDataParams> paramsList) {
        if (paramsList.isEmpty()) {
            asyncResponse.resume(Response.ok().type(MediaType.APPLICATION_JSON_TYPE).build());
        }

        List<AvailabilityMetric> metrics = new ArrayList<>(paramsList.size());

        for (AvailabilityDataParams params : paramsList) {
            AvailabilityMetric metric = new AvailabilityMetric(DEFAULT_TENANT_ID, new MetricId(params.getName()),
                params.getAttributes());
            for (AvailabilityDataPoint p : params.getData()) {
                metric.addData(new Availability(metric, p.getTimestamp(), p.getValue()));
            }
            metrics.add(metric);
        }
        ListenableFuture<Void> insertFuture = metricsService.addAvailabilityData(metrics);
        Futures.addCallback(insertFuture, new FutureCallback<Void>() {
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
    @Path("/numeric")
    public void findNumericDataByTags(@Suspended final AsyncResponse asyncResponse, @QueryParam("tags") String tags) {
        Set<String> tagSet = ImmutableSet.copyOf(tags.split(","));
        ListenableFuture<Map<MetricId, Set<NumericData>>> queryFuture = metricsService.findNumericDataByTags(
            DEFAULT_TENANT_ID, tagSet);
        Futures.addCallback(queryFuture, new FutureCallback<Map<MetricId, Set<NumericData>>>() {
            @Override
            public void onSuccess(Map<MetricId, Set<NumericData>> taggedDataMap) {
                Map<String, NumericDataOut> results = new HashMap<>();
                NumericDataOut dataOut = null;
                for (MetricId id : taggedDataMap.keySet()) {
                    List<NumericDataPoint> dataPoints = new ArrayList<>();
                    for (NumericData d : taggedDataMap.get(id)) {
                        if (dataOut == null) {
                            dataOut = new NumericDataOut();
                            dataOut.setTenantId(d.getMetric().getTenantId());
                            dataOut.setName(d.getMetric().getId().getName());
                        }
                        NumericDataPoint p = new NumericDataPoint();
                        p.setTimestamp(d.getTimestamp());
                        p.setValue(d.getValue());
                        dataPoints.add(p);
                    }
                    dataOut.setData(dataPoints);
                    results.put(id.getName(), dataOut);
                    dataOut = null;
                }
                asyncResponse.resume(Response.ok(results).type(MediaType.APPLICATION_JSON_TYPE).build());
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @GET
    @Path("/availability")
    public void findAvailabilityDataByTags(@Suspended final AsyncResponse asyncResponse,
        @QueryParam("tags") String tags) {
        Set<String> tagSet = ImmutableSet.copyOf(tags.split(","));
        ListenableFuture<Map<MetricId, Set<Availability>>> queryFuture = metricsService.findAvailabilityByTags(
            DEFAULT_TENANT_ID, tagSet);
        Futures.addCallback(queryFuture, new FutureCallback<Map<MetricId, Set<Availability>>>() {
            @Override
            public void onSuccess(Map<MetricId, Set<Availability>> taggedDataMap) {
                if (taggedDataMap.isEmpty()) {
                    asyncResponse.resume(Response.ok().status(Response.Status.NO_CONTENT).build());
                } else {
                    Map<String, AvailabilityDataOut> results = new HashMap<>();
                    AvailabilityDataOut dataOut = null;
                    for (MetricId id : taggedDataMap.keySet()) {
                        List<AvailabilityDataPoint> dataPoints = new ArrayList<>();
                        for (Availability a : taggedDataMap.get(id)) {
                            if (dataOut == null) {
                                dataOut = new AvailabilityDataOut();
                                dataOut.setTenantId(a.getMetric().getTenantId());
                                dataOut.setName(a.getMetric().getId().getName());
                            }
                            AvailabilityDataPoint p = new AvailabilityDataPoint();
                            p.setTimestamp(a.getTimestamp());
                            p.setValue(a.getType().getText());
                            dataPoints.add(p);
                        }
                        dataOut.setData(dataPoints);
                        results.put(id.getName(), dataOut);
                        dataOut = null;
                    }
                    asyncResponse.resume(Response.ok(results).type(MediaType.APPLICATION_JSON_TYPE).build());
                }
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    private Set<String> getTagNames(MetricData d) {
        if (d.getTags().isEmpty()) {
            return null;
        }
        Set<String> set = new HashSet<>();
        for (Tag tag : d.getTags()) {
            set.add(tag.getValue());
        }
        return set;
    }

    @GET
    @Path("/numeric/{id}")
    public void findNumericData(@Suspended final AsyncResponse asyncResponse, @PathParam("id") final String id,
        @QueryParam("start") Long start, @QueryParam("end") Long end) {

        long now = System.currentTimeMillis();
        if (start == null) {
            start = now - EIGHT_HOURS;
        }
        if (end == null) {
            end = now;
        }

        NumericMetric2 metric = new NumericMetric2(DEFAULT_TENANT_ID, new MetricId(id));
        ListenableFuture<NumericMetric2> future = metricsService.findNumericData(metric, start, end);
        Futures.addCallback(future, new FutureCallback<NumericMetric2>() {
            @Override
            public void onSuccess(NumericMetric2 metric) {
                if (metric == null) {
                    asyncResponse.resume(Response.ok().status(Response.Status.NO_CONTENT).build());
                } else {
                    NumericDataOut output = new NumericDataOut();
                    output.setTenantId(metric.getTenantId());
                    output.setName(metric.getId().getName());
                    output.setAttributes(metric.getAttributes());

                    List<NumericDataPoint> dataPoints = new ArrayList<>(metric.getData().size());
                    for (NumericData d : metric.getData()) {
                        NumericDataPoint dataPoint = new NumericDataPoint(d.getTimestamp(), d.getValue());
                        dataPoint.setTags(getTagNames(d));
                       dataPoints.add(dataPoint);
                    }
                    output.setData(dataPoints);
                    asyncResponse.resume(Response.ok(output).type(MediaType.APPLICATION_JSON_TYPE).build());
                }
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @GET
    @Path("/availability/{id}")
    public void findAvailabilityData(@Suspended final AsyncResponse asyncResponse, @PathParam("id") final String id,
        @QueryParam("start") Long start, @QueryParam("end") Long end) {

        long now = System.currentTimeMillis();
        if (start == null) {
            start = now - EIGHT_HOURS;
        }
        if (end == null) {
            end = now;
        }

        AvailabilityMetric metric = new AvailabilityMetric(DEFAULT_TENANT_ID, new MetricId(id));
        ListenableFuture<AvailabilityMetric> future = metricsService.findAvailabilityData(metric, start, end);
        Futures.addCallback(future, new FutureCallback<AvailabilityMetric>() {
            @Override
            public void onSuccess(AvailabilityMetric metric) {
                if (metric == null) {
                    asyncResponse.resume(Response.ok().status(Response.Status.NO_CONTENT).build());
                } else {
                    AvailabilityDataOut output = new AvailabilityDataOut();
                    output.setTenantId(metric.getTenantId());
                    output.setName(metric.getId().getName());
                    output.setAttributes(metric.getAttributes());

                    List<AvailabilityDataPoint> dataPoints = new ArrayList<>(metric.getData().size());
                    for (Availability a : metric.getData()) {
                        AvailabilityDataPoint p = new AvailabilityDataPoint();
                        p.setTimestamp(a.getTimestamp());
                        p.setValue(a.getType().getText());
                        p.setTags(getTagNames(a));
                        dataPoints.add(p);
                    }
                    output.setData(dataPoints);

                    asyncResponse.resume(Response.ok(output).type(MediaType.APPLICATION_JSON_TYPE).build());
                }
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @POST
    @Path("/tags/numeric")
    public void tagNumericData(@Suspended final AsyncResponse asyncResponse, TagParams params) {
        ListenableFuture<List<NumericData>> future;
        NumericMetric2 metric = new NumericMetric2(DEFAULT_TENANT_ID, new MetricId(params.getMetric()));
        if (params.getTimestamp() != null) {
            future = metricsService.tagNumericData(metric, params.getTags(), params.getTimestamp());
        } else {
            future = metricsService.tagNumericData(metric, params.getTags(), params.getStart(), params.getEnd());
        }
        Futures.addCallback(future, new FutureCallback<List<NumericData>>() {
            @Override
            public void onSuccess(List<NumericData> data) {
                asyncResponse.resume(Response.ok().type(MediaType.APPLICATION_JSON_TYPE).build());
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @POST
    @Path("/tags/availability")
    public void tagAvailabilityData(@Suspended final AsyncResponse asyncResponse, TagParams params) {
        ListenableFuture<List<Availability>> future;
        AvailabilityMetric metric = new AvailabilityMetric(DEFAULT_TENANT_ID, new MetricId(params.getMetric()));
        if (params.getTimestamp() != null) {
            future = metricsService.tagAvailabilityData(metric, params.getTags(), params.getTimestamp());
        } else {
            future = metricsService.tagAvailabilityData(metric, params.getTags(), params.getStart(), params.getEnd());
        }
        Futures.addCallback(future, new FutureCallback<List<Availability>>() {
            @Override
            public void onSuccess(List<Availability> data) {
                asyncResponse.resume(Response.ok().type(MediaType.APPLICATION_JSON_TYPE).build());
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @GET
    @Path("/tags/numeric/{tag}")
    public void findTaggedNumericData(@Suspended final AsyncResponse asyncResponse, @PathParam("tag") String tag) {
        ListenableFuture<Map<MetricId, Set<NumericData>>> future = metricsService.findNumericDataByTags(
            DEFAULT_TENANT_ID, ImmutableSet.of(tag));
        Futures.addCallback(future, new FutureCallback<Map<MetricId, Set<NumericData>>>() {
            @Override
            public void onSuccess(Map<MetricId, Set<NumericData>> taggedDataMap) {
                if (taggedDataMap.isEmpty()) {
                    asyncResponse.resume(Response.ok().status(Response.Status.NO_CONTENT).build());
                } else {
                    // TODO Should we return something other than NumericDataOutput?
                    // Currently we only query the tags table which does not include attributes.
                    // There is an attributes property in NumericDataOutput so the resulting json
                    // will always have a null attributes field, which might misleading. We may
                    // want to use a different return type that does not have an attributes property.

                    Map<String, NumericDataOut> results = new HashMap<>();
                    NumericDataOut dataOut = null;
                    for (MetricId id : taggedDataMap.keySet()) {
                        List<NumericDataPoint> dataPoints = new ArrayList<>();
                        for (NumericData d : taggedDataMap.get(id)) {
                            if (dataOut == null) {
                                dataOut = new NumericDataOut();
                                dataOut.setTenantId(d.getMetric().getTenantId());
                                dataOut.setName(d.getMetric().getId().getName());
                            }
                            NumericDataPoint p = new NumericDataPoint();
                            p.setTimestamp(d.getTimestamp());
                            p.setValue(d.getValue());
                            dataPoints.add(p);
                        }
                        dataOut.setData(dataPoints);
                        results.put(id.getName(), dataOut);
                        dataOut = null;
                    }
                    asyncResponse.resume(Response.ok(results).type(MediaType.APPLICATION_JSON_TYPE).build());
                }
            }

            @Override
            public void onFailure(Throwable t) {
                asyncResponse.resume(t);
            }
        });
    }

    @GET
    @Path("/tags/availability/{tag}")
    public void findTaggedAvailabilityData(@Suspended final AsyncResponse asyncResponse, @PathParam("tag") String tag) {
        ListenableFuture<Map<MetricId, Set<Availability>>> future = metricsService.findAvailabilityByTags(
            DEFAULT_TENANT_ID, ImmutableSet.of(tag));
        Futures.addCallback(future, new FutureCallback<Map<MetricId, Set<Availability>>>() {
            @Override
            public void onSuccess(Map<MetricId, Set<Availability>> taggedDataMap) {
                if (taggedDataMap.isEmpty()) {
                    asyncResponse.resume(Response.ok().status(Response.Status.NO_CONTENT).build());
                } else {
                    Map<String, AvailabilityDataOut> results = new HashMap<>();
                    AvailabilityDataOut dataOut = null;
                    for (MetricId id : taggedDataMap.keySet()) {
                        List<AvailabilityDataPoint> dataPoints = new ArrayList<>();
                        for (Availability a : taggedDataMap.get(id)) {
                            if (dataOut == null) {
                                dataOut = new AvailabilityDataOut();
                                dataOut.setTenantId(a.getMetric().getTenantId());
                                dataOut.setName(a.getMetric().getId().getName());
                            }
                            AvailabilityDataPoint p = new AvailabilityDataPoint();
                            p.setTimestamp(a.getTimestamp());
                            p.setValue(a.getType().getText());
                            dataPoints.add(p);
                        }
                        dataOut.setData(dataPoints);
                        results.put(id.getName(), dataOut);
                        dataOut = null;
                    }
                    asyncResponse.resume(Response.ok(results).type(MediaType.APPLICATION_JSON_TYPE).build());
                }
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
        ListenableFuture<Void> future = metricsService.updateCounter(new Counter(DEFAULT_TENANT_ID, group, counter,
            value));
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

                NumericMetric2 metric = new NumericMetric2(DEFAULT_TENANT_ID, new MetricId(id));
                final ListenableFuture<List<NumericData>> future = metricsService.findData(metric, finalStart, finalEnd);

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
                                long totalLength = (long)numberOfBuckets * bucketWidthSeconds * 1000L ;

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
                                if (bucketCluster) {
                                    // Now that stuff is in buckets - we need to "flatten" them out.
                                    // As we collapse stuff from a lot of input timestamps into some
                                    // buckets, we only use a relative time for the bucket timestamps.
                                    for (int i = 0; i < numberOfBuckets; i++) {
                                        List<NumericData> tmpList = buckets.get(i);
                                        BucketDataPoint point;
                                        if (tmpList == null) {
                                            if (!skipEmpty) {
                                                point = new BucketDataPoint(id, 1000L * i * bucketWidthSeconds , NaN, NaN, NaN);
                                                points.add(point);
                                            }
                                        } else {
                                            point = getBucketDataPoint(tmpList.get(0).getMetric().getId().getName(),
                                                1000L * i * bucketWidthSeconds, tmpList);
                                            points.add(point);
                                        }

                                    }
                                } else {
                                    // We want to keep the raw values, but put them into clusters anyway
                                    // without collapsing them into a single min/avg/max tuple
                                    for (int i = 0; i < numberOfBuckets; i++) {
                                        List<NumericData> tmpList = buckets.get(i);
                                        BucketDataPoint point;
                                        if (tmpList!=null) {
                                            for (NumericData metric : tmpList) {
                                                point = new BucketDataPoint(id, // TODO could be simple data points
                                                    1000L * i * bucketWidthSeconds, NaN,metric.getValue(),NaN);
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
        List<NumericData> metrics) {
        List<NumericData> bucketMetrics = new ArrayList<>(metrics.size());
        // Find matching metrics
        for (NumericData raw : metrics) {
            if (raw.getTimestamp() >= startTime && raw.getTimestamp() < startTime + bucketsize) {
                bucketMetrics.add((NumericData) raw);
            }
        }

        return getBucketDataPoint(id, startTime, bucketMetrics);
    }

    private BucketDataPoint getBucketDataPoint(String id, long startTime, List<NumericData> bucketMetrics) {
        Double min = null;
        Double max = null;
        double sum = 0;
        for (NumericData raw : bucketMetrics) {
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
