/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.dropwizard;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.hawkular.metrics.client.common.http.HawkularHttpClient;
import org.hawkular.metrics.client.common.http.JdkHawkularHttpClient;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;

@SuppressWarnings("WeakerAccess") public class HawkularReporterBuilder {

    private static final String KEY_HEADER_TENANT = "Hawkular-Tenant";
    private static final String KEY_HEADER_AUTHORIZATION = "Authorization";

    private final MetricRegistry registry;
    private String uri = "http://localhost:8080";
    private Map<String, String> headers = new HashMap<>();
    private Optional<String> prefix = Optional.empty();
    private MetricFilter filter = MetricFilter.ALL;
    private TimeUnit rateUnit = TimeUnit.SECONDS;
    private TimeUnit durationUnit = TimeUnit.MILLISECONDS;
    private Optional<Function<String, HawkularHttpClient>> httpClientProvider = Optional.empty();
    private final Map<String, String> globalTags = new HashMap<>();
    private final Map<String, Map<String, String>> perMetricTags = new HashMap<>();
    private final Collection<RegexContainer<Map<String, String>>> regexTags = new ArrayList<>();
    private boolean tagComposition = true;
    private Optional<Long> failoverCacheDuration = Optional.of(1000L * 60L * 10L); // In milliseconds; default: 10min
    private Optional<Integer> failoverCacheMaxSize = Optional.empty();
    private final Map<String, Set<String>> namedMetricsComposition = new HashMap<>();
    private final Collection<RegexContainer<Set<String>>> regexComposition = new ArrayList<>();

    /**
     * Create a new builder for an {@link HawkularReporter}
     * @param registry the Dropwizard Metrics registry
     * @param tenant the Hawkular tenant ID
     */
    public HawkularReporterBuilder(MetricRegistry registry, String tenant) {
        this.registry = registry;
        headers.put(KEY_HEADER_TENANT, tenant);
    }

    /**
     * This is a shortcut function to use with automatically populated pojos such as coming from yaml config
     */
    public HawkularReporterBuilder withNullableConfig(HawkularReporterConfig config) {
        if (config.getUri() != null) {
            this.uri(config.getUri());
        }
        if (config.getPrefix() != null) {
            this.prefixedWith(config.getPrefix());
        }
        if (config.getBearerToken() != null) {
            this.bearerToken(config.getBearerToken());
        }
        if (config.getHeaders() != null) {
            config.getHeaders().forEach(this::addHeader);
        }
        if (config.getGlobalTags() != null) {
            this.globalTags(config.getGlobalTags());
        }
        if (config.getPerMetricTags() != null) {
            this.perMetricTags(config.getPerMetricTags());
        }
        if (config.getTagComposition() != null && !config.getTagComposition()) {
            this.disableTagComposition();
        }
        if (config.getUsername() != null && config.getPassword() != null) {
            this.basicAuth(config.getUsername(), config.getPassword());
        }
        if (config.getMetricComposition() != null) {
            this.metricComposition(config.getMetricComposition());
        }
        failoverCacheDuration = Optional.ofNullable(config.getFailoverCacheDuration());
        failoverCacheMaxSize = Optional.ofNullable(config.getFailoverCacheMaxSize());
        return this;
    }

    /**
     * Set the URI for the Hawkular connection. Default URI is http://localhost:8080
     * @param uri base uri - do not include Hawkular Metrics path (/hawkular/metrics)
     */
    public HawkularReporterBuilder uri(String uri) {
        this.uri = uri;
        return this;
    }

    /**
     * Set username and password for basic HTTP authentication
     * @param username basic auth. username
     * @param password basic auth. password
     */
    public HawkularReporterBuilder basicAuth(String username, String password) {
        String encoded = Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
        headers.put(KEY_HEADER_AUTHORIZATION, "Basic " + encoded);
        return this;
    }

    /**
     * Set the bearer token for the Authorization header in Hawkular HTTP connections. Can be used, for instance, for
     * OpenShift connections
     * @param token the bearer token
     */
    public HawkularReporterBuilder bearerToken(String token) {
        headers.put(KEY_HEADER_AUTHORIZATION, "Bearer " + token);
        return this;
    }

    /**
     * Add a custom header to Hawkular HTTP connections
     * @param key header name
     * @param value header value
     */
    public HawkularReporterBuilder addHeader(String key, String value) {
        headers.put(key, value);
        return this;
    }

    /**
     * Configure a prefix for each metric name. Optional, but useful to identify single hosts
     */
    public HawkularReporterBuilder prefixedWith(String prefix) {
        this.prefix = Optional.of(prefix);
        return this;
    }

    /**
     * Configure a special MetricFilter, which defines what metrics are reported
     */
    public HawkularReporterBuilder filter(MetricFilter filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Set dropwizard rates conversion
     */
    public HawkularReporterBuilder convertRatesTo(TimeUnit rateUnit) {
        this.rateUnit = rateUnit;
        return this;
    }

    /**
     * Set dropwizard duration conversion
     */
    public HawkularReporterBuilder convertDurationsTo(TimeUnit durationUnit) {
        this.durationUnit = durationUnit;
        return this;
    }

    /**
     * Set all global tags at once. All metrics generated by this reporter instance will be tagged as such.
     * It overrides any global tag that was already set.
     * @param tags global tags
     */
    public HawkularReporterBuilder globalTags(Map<String, String> tags) {
        this.globalTags.clear();
        this.globalTags.putAll(tags);
        return this;
    }

    /**
     * Set a global tag. All metrics generated by this reporter instance will be tagged as such.
     * @param key tag key
     * @param value tag value
     */
    public HawkularReporterBuilder addGlobalTag(String key, String value) {
        this.globalTags.put(key, value);
        return this;
    }

    /**
     * Set all per-metric tags at once. It overrides any per-metric tag that was already set.
     * @param tags per-metric tags
     */
    public HawkularReporterBuilder perMetricTags(Map<String, Map<String, String>> tags) {
        this.perMetricTags.clear();
        this.regexTags.clear();
        tags.forEach((k,v) -> {
            Optional<RegexContainer<Map<String, String>>> optRegexTags = RegexContainer.checkAndCreate(k, v);
            if (optRegexTags.isPresent()) {
                this.regexTags.add(optRegexTags.get());
            } else {
                this.perMetricTags.put(k, v);
            }
        });
        return this;
    }

    /**
     * Set a tag on a given metric name
     * @param metric the metric name
     * @param key tag key
     * @param value tag value
     */
    public HawkularReporterBuilder addMetricTag(String metric, String key, String value) {
        Optional<RegexContainer<Map<String, String>>> optRegexTags = RegexContainer
                .checkAndCreate(metric, Collections.singletonMap(key, value));
        if (optRegexTags.isPresent()) {
            regexTags.add(optRegexTags.get());
        } else {
            final Map<String, String> tags;
            if (perMetricTags.containsKey(metric)) {
                tags = perMetricTags.get(metric);
            } else {
                tags = new HashMap<>();
                perMetricTags.put(metric, tags);
            }
            tags.put(key, value);
        }
        return this;
    }

    /**
     * Set a tag on metrics matching this regex
     * @param pattern the regex pattern
     * @param key tag key
     * @param value tag value
     */
    public HawkularReporterBuilder addRegexTag(Pattern pattern, String key, String value) {
        regexTags.add(new RegexContainer<>(pattern, Collections.singletonMap(key, value)));
        return this;
    }

    /**
     * Disable auto-tagging composed metrics. By default, it is enabled.<br/>
     * When enabled, some metric types such as Meters or Timers will automatically generate additional information as
     * tags. For instance, a Meter metric will generate a tag "meter:5minrt" on its 5-minutes-rate component.
     */
    public HawkularReporterBuilder disableTagComposition() {
        tagComposition = false;
        return this;
    }

    /**
     * Set all metrics composition at once. It overrides any per-metric composition that was already set.
     * @param conversions per-metric composition
     */
    public HawkularReporterBuilder metricComposition(Map<String, Collection<String>> conversions) {
        this.namedMetricsComposition.clear();
        this.regexComposition.clear();
        conversions.forEach((k,v) -> {
            Set<String> uniqueParts = new HashSet<>(v);
            Optional<RegexContainer<Set<String>>> optRegexTags = RegexContainer.checkAndCreate(k, uniqueParts);
            if (optRegexTags.isPresent()) {
                this.regexComposition.add(optRegexTags.get());
            } else {
                this.namedMetricsComposition.put(k, uniqueParts);
            }
        });
        return this;
    }

    /**
     * Set composing parts for a given metric name
     * @param metric the metric name
     * @param parts metric composed parts (such as "1minrt", "mean", "99perc", etc. - see project documentation)
     */
    public HawkularReporterBuilder setMetricComposition(String metric, Collection<String> parts) {
        Set<String> uniqueParts = new HashSet<>(parts);
        Optional<RegexContainer<Set<String>>> optRegexTags = RegexContainer.checkAndCreate(metric, uniqueParts);
        if (optRegexTags.isPresent()) {
            regexComposition.add(optRegexTags.get());
        } else {
            namedMetricsComposition.put(metric, uniqueParts);
        }
        return this;
    }

    /**
     * Set composing parts for metrics matching this regex
     * @param pattern the regex pattern
     * @param parts metric composed parts (such as "1minrt", "mean", "99perc", etc. - see project documentation)
     */
    public HawkularReporterBuilder setRegexMetricComposition(Pattern pattern, Collection<String> parts) {
        regexComposition.add(new RegexContainer<>(pattern, new HashSet<>(parts)));
        return this;
    }

    /**
     * Set the failover cache duration (in milliseconds)<br/>
     * This cache is used to store post attempts in memory when the hawkular server cannot be reached<br/>
     * Default duration is 10 minutes
     * @param milliseconds number of milliseconds before eviction
     */
    public HawkularReporterBuilder failoverCacheDuration(long milliseconds) {
        failoverCacheDuration = Optional.of(milliseconds);
        return this;
    }

    /**
     * Set the failover cache duration, in minutes<br/>
     * This cache is used to store post attempts in memory when the hawkular server cannot be reached<br/>
     * Default duration is 10 minutes
     * @param minutes number of minutes before eviction
     */
    public HawkularReporterBuilder failoverCacheDurationInMinutes(long minutes) {
        failoverCacheDuration = Optional.of(TimeUnit.MILLISECONDS.convert(minutes, TimeUnit.MINUTES));
        return this;
    }

    /**
     * Set the failover cache duration, in hours<br/>
     * This cache is used to store post attempts in memory when the hawkular server cannot be reached<br/>
     * Default duration is 10 minutes
     * @param hours number of hours before eviction
     */
    public HawkularReporterBuilder failoverCacheDurationInHours(long hours) {
        failoverCacheDuration = Optional.of(TimeUnit.MILLISECONDS.convert(hours, TimeUnit.HOURS));
        return this;
    }

    /**
     * Set the failover cache maximum size, in number of requests<br/>
     * This cache is used to store post attempts in memory when the hawkular server cannot be reached<br/>
     * By default this parameter is unset, which means there's no maximum
     * @param reqs max number of requests to store
     */
    public HawkularReporterBuilder failoverCacheMaxSize(int reqs) {
        failoverCacheMaxSize = Optional.of(reqs);
        return this;
    }

    /**
     * Use a custom {@link HawkularHttpClient}
     * @param httpClientProvider function that provides a custom {@link HawkularHttpClient} from input URI as String
     */
    public HawkularReporterBuilder useHttpClient(Function<String, HawkularHttpClient> httpClientProvider) {
        this.httpClientProvider = Optional.of(httpClientProvider);
        return this;
    }

    /**
     * Build the {@link HawkularReporter}
     */
    public HawkularReporter build() {
        HawkularHttpClient client = httpClientProvider
                .map(provider -> provider.apply(uri))
                .orElseGet(() -> new JdkHawkularHttpClient(uri));
        client.addHeaders(headers);
        client.setFailoverOptions(failoverCacheDuration, failoverCacheMaxSize);
        MetricsDecomposer decomposer = new MetricsDecomposer(namedMetricsComposition, regexComposition);
        MetricsTagger tagger = new MetricsTagger(prefix, globalTags, perMetricTags, regexTags, tagComposition,
                decomposer, client, registry, filter);
        return new HawkularReporter(registry, client, prefix, decomposer, tagger, rateUnit, durationUnit, filter);
    }
}
