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
package org.hawkular.metrics.core.service;

import static java.time.ZoneOffset.UTC;
import static java.util.stream.Collectors.toMap;

import static org.hawkular.metrics.core.service.TimeUUIDUtils.getTimeUUID;
import static org.hawkular.metrics.model.MetricType.AVAILABILITY;
import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.STRING;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.hawkular.metrics.core.service.compress.CompressedPointContainer;
import org.hawkular.metrics.core.service.log.CoreLogger;
import org.hawkular.metrics.core.service.log.CoreLogging;
import org.hawkular.metrics.core.service.transformers.BatchStatementTransformer;
import org.hawkular.metrics.core.service.transformers.BoundBatchStatementTransformer;
import org.hawkular.metrics.datetime.DateTimeService;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.Tenant;
import org.hawkular.rx.cassandra.driver.RxSession;
import org.hawkular.rx.cassandra.driver.RxSessionImpl;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.utils.UUIDs;

import rx.Observable;
import rx.exceptions.Exceptions;

/**
 *
 * @author John Sanda
 */
public class DataAccessImpl implements DataAccess {

    private static final CoreLogger log = CoreLogging.getCoreLogger(DataAccessImpl.class);

    public static final int NUMBER_OF_TEMP_TABLES = 12;
    public static final int POS_OF_OLD_DATA = NUMBER_OF_TEMP_TABLES;
    public static final long DPART = 0;
    private Session session;

    private RxSession rxSession;

    private LoadBalancingPolicy loadBalancingPolicy;

    // Turn to MetricType agnostic
    private PreparedStatement[][] dateRangeExclusive;
    private PreparedStatement[][] dateRangeExclusiveWithLimit;
    private PreparedStatement[][] dataByDateRangeExclusiveASC;
    private PreparedStatement[][] dataByDateRangeExclusiveWithLimitASC;

    private PreparedStatement[] metricsInDatas;
    private PreparedStatement[] allMetricsInDatas;

    private PreparedStatement[][] dataTable;
    private PreparedStatement[][] dataWithTagsTable;

    private PreparedStatement insertTenant;

    private PreparedStatement insertTenantOverwrite;

    private PreparedStatement findAllTenantIds;

    private PreparedStatement findAllTenantIdsFromMetricsIdx;

    private PreparedStatement findTenant;

    private PreparedStatement insertIntoMetricsIndex;

    private PreparedStatement insertIntoMetricsIndexOverwrite;

    private PreparedStatement findMetricInData;

    private PreparedStatement findMetricInDataCompressed;

    private PreparedStatement findAllMetricsInData;

    private PreparedStatement findAllMetricsInDataCompressed;

    private PreparedStatement findMetricInMetricsIndex;

    private PreparedStatement findAllMetricsFromTagsIndex;

    private PreparedStatement getMetricTags;

    private PreparedStatement getTagNames;

    private PreparedStatement getTagNamesWithType;

    private PreparedStatement insertCompressedData;

    private PreparedStatement insertCompressedDataWithTags;

    private PreparedStatement insertStringData;

    private PreparedStatement insertStringDataUsingTTL;

    private PreparedStatement insertStringDataWithTags;

    private PreparedStatement insertStringDataWithTagsUsingTTL;

    private PreparedStatement findCompressedDataByDateRangeExclusive;

    private PreparedStatement findCompressedDataByDateRangeExclusiveWithLimit;

    private PreparedStatement findCompressedDataByDateRangeExclusiveASC;

    private PreparedStatement findCompressedDataByDateRangeExclusiveWithLimitASC;

    private PreparedStatement findStringDataByDateRangeExclusive;

    private PreparedStatement findStringDataByDateRangeExclusiveWithLimit;

    private PreparedStatement findStringDataByDateRangeExclusiveASC;

    private PreparedStatement findStringDataByDateRangeExclusiveWithLimitASC;

    private PreparedStatement findAvailabilityByDateRangeInclusive;

    private PreparedStatement deleteMetricData;

    private PreparedStatement deleteFromMetricRetentionIndex;

    private PreparedStatement deleteMetricFromMetricsIndex;

    private PreparedStatement deleteMetricDataWithLimit;

    private PreparedStatement insertAvailability;

    private PreparedStatement insertAvailabilityUsingTTL;

    private PreparedStatement insertAvailabilityWithTags;

    private PreparedStatement insertAvailabilityWithTagsUsingTTL;

    private PreparedStatement findAvailabilities;

    private PreparedStatement findAvailabilitiesWithLimit;

    private PreparedStatement findAvailabilitiesASC;

    private PreparedStatement findAvailabilitiesWithLimitASC;

    private PreparedStatement updateMetricsIndex;

    private PreparedStatement addTagsToMetricsIndex;

    private PreparedStatement deleteTagsFromMetricsIndex;

    private PreparedStatement readMetricsIndex;

    private PreparedStatement updateRetentionsIndex;

    private PreparedStatement findDataRetentions;

    private PreparedStatement insertMetricsTagsIndex;

    private PreparedStatement deleteMetricsTagsIndex;

    private PreparedStatement findMetricsByTagName;

    private PreparedStatement findMetricsByTagNameValue;

    private PreparedStatement updateMetricExpirationIndex;

    private PreparedStatement deleteFromMetricExpirationIndex;

    private PreparedStatement findMetricExpiration;

    private CodecRegistry codecRegistry;
    private Metadata metadata;

    public DataAccessImpl(Session session) {
        this.session = session;
        rxSession = new RxSessionImpl(session);
        loadBalancingPolicy = session.getCluster().getConfiguration().getPolicies().getLoadBalancingPolicy();
        initPreparedStatements();
        preparedTemporaryTableStatements();

        codecRegistry = session.getCluster().getConfiguration().getCodecRegistry();
        metadata = session.getCluster().getMetadata();
    }

    private void preparedTemporaryTableStatements() {
        // Read statements
        // TODO Could I even fetch compressed data with these same queries?
        dateRangeExclusive = new PreparedStatement[MetricType.all().size()][NUMBER_OF_TEMP_TABLES+1];
        dateRangeExclusiveWithLimit = new PreparedStatement[MetricType.all().size()][NUMBER_OF_TEMP_TABLES+1];
        dataByDateRangeExclusiveASC = new PreparedStatement[MetricType.all().size()][NUMBER_OF_TEMP_TABLES+1];
        dataByDateRangeExclusiveWithLimitASC = new PreparedStatement[MetricType.all().size()][NUMBER_OF_TEMP_TABLES+1];

        String byDateRangeExclusiveBase =
                "SELECT time, %s, tags FROM %s " +
                        "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ? AND time < ?";

        String dateRangeExclusiveWithLimitBase =
                "SELECT time, %s, tags FROM %s " +
                        " WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ? AND time < ?" +
                        " LIMIT ?";

        String dataByDateRangeExclusiveASCBase =
                "SELECT time, %s, tags FROM %s " +
                        "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ?" +
                        " AND time < ? ORDER BY time ASC";

        String dataByDateRangeExclusiveWithLimitASCBase =
                "SELECT time, %s, tags FROM %s" +
                        " WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ?" +
                        " AND time < ? ORDER BY time ASC" +
                        " LIMIT ?";

        // Insert statements
        dataTable = new PreparedStatement[MetricType.all().size()][NUMBER_OF_TEMP_TABLES];
        dataWithTagsTable = new PreparedStatement[MetricType.all().size()][NUMBER_OF_TEMP_TABLES];

        String data = "UPDATE %s " +
                        "SET %s = ? " +
                        "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time = ? ";

        String dataWithTags = "UPDATE %s " +
                        "SET %s = ?, tags = ? " +
                        "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time = ? ";

        // MetricDefinition statements
        metricsInDatas = new PreparedStatement[NUMBER_OF_TEMP_TABLES+1];
        allMetricsInDatas = new PreparedStatement[NUMBER_OF_TEMP_TABLES+1];

        String findMetricInDataBase = "SELECT DISTINCT tenant_id, type, metric, dpart " +
                        "FROM %s " +
                        "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? ";

        String findAllMetricsInDataBases = "SELECT DISTINCT tenant_id, type, metric, dpart " +
                        "FROM %s";

        String tempTableNameFormat = "data_temp_%d";

        // Initialize all the temporary tables for inserts
        for (MetricType<?> metricType : MetricType.all()) {
            for(int k = 0; k < NUMBER_OF_TEMP_TABLES; k++) {
                // String metrics are not yet supported with temp tables
                if(metricType.getCode() == 4) {
                    continue;
                }

                String tempTableName = String.format(tempTableNameFormat, k);

                // Insert statements
                dataTable[metricType.getCode()][k] = session.prepare(
                        String.format(data, tempTableName, metricTypeToColumnName(metricType))
                );
                dataWithTagsTable[metricType.getCode()][k] = session.prepare(
                        String.format(dataWithTags, tempTableName, metricTypeToColumnName(metricType))
                );
                // Read statements
                dateRangeExclusive[metricType.getCode()][k] = session.prepare(
                        String.format(byDateRangeExclusiveBase, metricTypeToColumnName(metricType), tempTableName)
                );
                dateRangeExclusiveWithLimit[metricType.getCode()][k] = session.prepare(
                        String.format(dateRangeExclusiveWithLimitBase, metricTypeToColumnName(metricType), tempTableName)
                );
                dataByDateRangeExclusiveASC[metricType.getCode()][k] = session.prepare(
                        String.format(dataByDateRangeExclusiveASCBase, metricTypeToColumnName(metricType), tempTableName)
                );
                dataByDateRangeExclusiveWithLimitASC[metricType.getCode()][k] = session.prepare(
                        String.format(dataByDateRangeExclusiveWithLimitASCBase, metricTypeToColumnName(metricType), tempTableName)
                );
            }
            // Then initialize the old fashion ones..
            dateRangeExclusive[metricType.getCode()][POS_OF_OLD_DATA] = session.prepare(
                    String.format(byDateRangeExclusiveBase, metricTypeToColumnName(metricType), "data")
            );
            dateRangeExclusiveWithLimit[metricType.getCode()][POS_OF_OLD_DATA] = session.prepare(
                    String.format(dateRangeExclusiveWithLimitBase, metricTypeToColumnName(metricType), "data")
            );
            dataByDateRangeExclusiveASC[metricType.getCode()][POS_OF_OLD_DATA] = session.prepare(
                    String.format(dataByDateRangeExclusiveASCBase, metricTypeToColumnName(metricType), "data")
            );
            dataByDateRangeExclusiveWithLimitASC[metricType.getCode()][POS_OF_OLD_DATA] = session.prepare(
                    String.format(dataByDateRangeExclusiveWithLimitASCBase, metricTypeToColumnName(metricType), "data")
            );
        }

        // MetricDefinition statements
        for(int i = 0; i < NUMBER_OF_TEMP_TABLES; i++) {
            String tempTableName = String.format(tempTableNameFormat, i);
            metricsInDatas[i] = session.prepare(String.format(findMetricInDataBase, tempTableName));
            allMetricsInDatas[i] = session.prepare(String.format(findAllMetricsInDataBases, tempTableName));
        }
        metricsInDatas[POS_OF_OLD_DATA] = session.prepare(String.format(findMetricInDataBase, "data"));
        allMetricsInDatas[POS_OF_OLD_DATA] = session.prepare(String.format(findAllMetricsInDataBases, "data"));
    }

    private String metricTypeToColumnName(MetricType<?> type) {
        switch(type.getCode()) {
            case 0:
                return "n_value";
            case 1:
                return "availability";
            case 2:
                return "l_value";
            case 3:
                return "l_value";
            case 4:
                return "s_value";
            case 5:
                return "n_value";
            default:
                throw new RuntimeException("Unsupported metricType");
        }
    }

    protected void initPreparedStatements() {
        insertTenant = session.prepare(
            "INSERT INTO tenants (id, retentions) VALUES (?, ?) IF NOT EXISTS");

        insertTenantOverwrite = session.prepare(
                "INSERT INTO tenants (id, retentions) VALUES (?, ?)");

        findAllTenantIds = session.prepare("SELECT DISTINCT id FROM tenants");

        findAllTenantIdsFromMetricsIdx = session.prepare("SELECT DISTINCT tenant_id, type FROM metrics_idx");

        findTenant = session.prepare("SELECT id, retentions FROM tenants WHERE id = ?");

        findMetricInData = session.prepare(
            "SELECT DISTINCT tenant_id, type, metric, dpart " +
            "FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? ");

        findMetricInDataCompressed = session.prepare(
                "SELECT DISTINCT tenant_id, type, metric, dpart " +
                        "FROM data_compressed " +
                        "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? ");

        findMetricInMetricsIndex = session.prepare(
            "SELECT metric, tags, data_retention " +
            "FROM metrics_idx " +
            "WHERE tenant_id = ? AND type = ? AND metric = ?");

        getMetricTags = session.prepare(
            "SELECT tags " +
            "FROM metrics_idx " +
            "WHERE tenant_id = ? AND type = ? AND metric = ?");

        getTagNames = session.prepare(
                "SELECT DISTINCT tenant_id, tname " +
                        "FROM metrics_tags_idx"); // Cassandra 3.10 will allow filtering by tenant_id

        getTagNamesWithType = session.prepare(
                "SELECT tenant_id, tname, type " +
                        "FROM metrics_tags_idx");

        insertIntoMetricsIndex = session.prepare(
            "INSERT INTO metrics_idx (tenant_id, type, metric, data_retention, tags) " +
            "VALUES (?, ?, ?, ?, ?) " +
            "IF NOT EXISTS");

        insertIntoMetricsIndexOverwrite = session.prepare(
            "INSERT INTO metrics_idx (tenant_id, type, metric, data_retention, tags) " +
            "VALUES (?, ?, ?, ?, ?) ");

        updateMetricsIndex = session.prepare(
            "INSERT INTO metrics_idx (tenant_id, type, metric) VALUES (?, ?, ?)");

        addTagsToMetricsIndex = session.prepare(
            "UPDATE metrics_idx " +
            "SET tags = tags + ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ?");

        deleteTagsFromMetricsIndex = session.prepare(
            "UPDATE metrics_idx " +
            "SET tags = tags - ?" +
            "WHERE tenant_id = ? AND type = ? AND metric = ?");

        readMetricsIndex = session.prepare(
            "SELECT metric, tags, data_retention " +
            "FROM metrics_idx " +
            "WHERE tenant_id = ? AND type = ? " +
            "ORDER BY metric ASC");

        findAllMetricsInData = session.prepare(
            "SELECT DISTINCT tenant_id, type, metric, dpart " +
            "FROM data");

        findAllMetricsInDataCompressed = session.prepare(
                "SELECT DISTINCT tenant_id, type, metric, dpart " +
                        "FROM data_compressed");

        findAllMetricsFromTagsIndex = session.prepare(
                "SELECT tenant_id, type, metric " +
                        "FROM metrics_tags_idx");

        insertCompressedData = session.prepare(
                "UPDATE data_compressed " +
                        "USING TTL ? " +
                        "SET c_value = ? " +
                        "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time = ? ");

        insertCompressedDataWithTags = session.prepare(
                "UPDATE data_compressed " +
                        "USING TTL ? " +
                        "SET c_value = ?, tags = ? " +
                        "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time = ? ");

        insertStringData = session.prepare(
            "UPDATE data " +
            "SET s_value = ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time = ?");

        insertStringDataUsingTTL = session.prepare(
            "UPDATE data " +
            "USING TTL ? " +
            "SET s_value = ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time = ?");

        insertStringDataWithTags = session.prepare(
            "UPDATE data " +
            "SET s_value = ?, tags = ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time = ? ");

        insertStringDataWithTagsUsingTTL = session.prepare(
              "UPDATE data " +
              "USING TTL ? " +
              "SET s_value = ?, tags = ? " +
              "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time = ? ");

        findCompressedDataByDateRangeExclusive = session.prepare(
                "SELECT time, c_value, tags FROM data_compressed " +
                        "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ? AND time < ?");

        findCompressedDataByDateRangeExclusiveWithLimit = session.prepare(
                "SELECT time, c_value, tags FROM data_compressed " +
                        " WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ? AND time < ?" +
                        " LIMIT ?");

        findCompressedDataByDateRangeExclusiveASC = session.prepare(
                "SELECT time, c_value, tags FROM data_compressed " +
                        "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ?" +
                        " AND time < ? ORDER BY time ASC");

        findCompressedDataByDateRangeExclusiveWithLimitASC = session.prepare(
                "SELECT time, c_value, tags FROM data_compressed" +
                        " WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ?" +
                        " AND time < ? ORDER BY time ASC" +
                        " LIMIT ?");

        findStringDataByDateRangeExclusive = session.prepare(
            "SELECT time, s_value, tags FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ? AND time < ?");

        findStringDataByDateRangeExclusiveWithLimit = session.prepare(
            "SELECT time, s_value, tags FROM data " +
            " WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ? AND time < ?" +
            " LIMIT ?");

        findStringDataByDateRangeExclusiveASC = session.prepare(
            "SELECT time, s_value, tags FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ?" +
            " AND time < ? ORDER BY time ASC");

        findStringDataByDateRangeExclusiveWithLimitASC = session.prepare(
            "SELECT time, s_value, tags FROM data" +
             " WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ?" +
             " AND time < ? ORDER BY time ASC" +
             " LIMIT ?");

        findAvailabilityByDateRangeInclusive = session.prepare(
            "SELECT time, availability, WRITETIME(availability) FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ? AND time <= ?");

        deleteMetricData = session.prepare(
            "DELETE FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ?");

        deleteMetricDataWithLimit = session.prepare(
            "DELETE FROM data " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ? AND time < ?");

        deleteFromMetricRetentionIndex = session.prepare(
            "DELETE FROM retentions_idx " +
            "WHERE tenant_id = ? AND type = ? AND metric = ?");

        deleteMetricFromMetricsIndex = session.prepare(
            "DELETE FROM metrics_idx " +
            "WHERE tenant_id = ? AND type = ? AND metric = ?");

        insertAvailability = session.prepare(
            "UPDATE data " +
            "SET availability = ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time = ?");

        insertAvailabilityUsingTTL = session.prepare(
            "UPDATE data " +
            "USING TTL ? " +
            "SET availability = ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time = ?");

        insertAvailabilityWithTags = session.prepare(
            "UPDATE data " +
            "SET availability = ?, tags = ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time = ?");

        insertAvailabilityWithTagsUsingTTL = session.prepare(
            "UPDATE data " +
            "USING TTL ? " +
            "SET availability = ?, tags = ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time = ?");

        findAvailabilities = session.prepare(
            "SELECT time, availability, tags " +
            " FROM data " +
            " WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ? AND time < ? ");

        findAvailabilitiesWithLimit = session.prepare(
            "SELECT time, availability, tags " +
            " FROM data " +
            " WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ? AND time < ? " +
            " LIMIT ?");

        findAvailabilitiesASC = session.prepare(
            "SELECT time, availability, tags " +
            " FROM data " +
            " WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ? AND time < ? " +
            " ORDER BY time ASC");

        findAvailabilitiesWithLimitASC = session.prepare(
            "SELECT time, availability, tags " +
            " FROM data " +
            " WHERE tenant_id = ? AND type = ? AND metric = ? AND dpart = ? AND time >= ? AND time < ? " +
            " ORDER BY time ASC" +
            " LIMIT ?");

        updateRetentionsIndex = session.prepare(
            "INSERT INTO retentions_idx (tenant_id, type, metric, retention) VALUES (?, ?, ?, ?)");

        findDataRetentions = session.prepare(
            "SELECT tenant_id, type, metric, retention " +
            "FROM retentions_idx " +
            "WHERE tenant_id = ? AND type = ?");

        insertMetricsTagsIndex = session.prepare(
            "INSERT INTO metrics_tags_idx (tenant_id, tname, tvalue, type, metric) VALUES (?, ?, ?, ?, ?)");

        deleteMetricsTagsIndex = session.prepare(
            "DELETE FROM metrics_tags_idx " +
            "WHERE tenant_id = ? AND tname = ? AND tvalue = ? AND type = ? AND metric = ?");

        findMetricsByTagName = session.prepare(
            "SELECT tenant_id, type, metric, tvalue " +
            "FROM metrics_tags_idx " +
            "WHERE tenant_id = ? AND tname = ?");

        findMetricsByTagNameValue = session.prepare(
                "SELECT tenant_id, type, metric, tvalue " +
                "FROM metrics_tags_idx " +
                "WHERE tenant_id = ? AND tname = ? AND tvalue = ?");

        updateMetricExpirationIndex = session.prepare(
                "INSERT INTO metrics_expiration_idx (tenant_id, type, metric, time) VALUES (?, ?, ?, ?)");

        deleteFromMetricExpirationIndex = session.prepare(
                "DELETE FROM metrics_expiration_idx " +
                "WHERE tenant_id = ? AND type = ? AND metric = ?");

        findMetricExpiration = session.prepare(
                "SELECT time " +
                "FROM metrics_expiration_idx " +
                "WHERE tenant_id = ? AND type = ? and metric = ?");
    }

    @Override
    public Observable<ResultSet> insertTenant(Tenant tenant, boolean overwrite) {
        Map<String, Integer> retentions = tenant.getRetentionSettings().entrySet().stream()
                .collect(toMap(entry -> entry.getKey().getText(), Map.Entry::getValue));

        if (overwrite) {
            return rxSession.execute(insertTenantOverwrite.bind(tenant.getId(), retentions));
        }

        return rxSession.execute(insertTenant.bind(tenant.getId(), retentions));
    }

    @Override
    public Observable<Row> findAllTenantIds() {
        return rxSession.executeAndFetch(findAllTenantIds.bind())
                .concatWith(rxSession.executeAndFetch(findAllTenantIdsFromMetricsIdx.bind()));
    }

    @Override
    public Observable<Row> findTenant(String id) {
        return rxSession.executeAndFetch(findTenant.bind(id));
    }

    @Override
    public <T> ResultSetFuture insertMetricInMetricsIndex(Metric<T> metric, boolean overwrite) {
        MetricId<T> metricId = metric.getMetricId();

        if (overwrite) {
            return session.executeAsync(
                    insertIntoMetricsIndexOverwrite.bind(metricId.getTenantId(), metricId.getType().getCode(),
                            metricId.getName(), metric.getDataRetention(), metric.getTags()));
        }

        return session.executeAsync(insertIntoMetricsIndex.bind(metricId.getTenantId(), metricId.getType().getCode(),
                metricId.getName(), metric.getDataRetention(), metric.getTags()));
    }

    @Override
    public <T> Observable<Row> findMetricInData(MetricId<T> id) {
        return Observable.from(metricsInDatas)
                .map(b -> b.bind(id.getTenantId(), id.getType().getCode(), id.getName(), DPART))
                .flatMap(b -> rxSession.executeAndFetch(b))
                .concatWith(rxSession.executeAndFetch(findMetricInData
                        .bind(id.getTenantId(), id.getType().getCode(), id.getName(), DPART))
                        .concatWith(
                                rxSession.executeAndFetch(findMetricInDataCompressed
                                        .bind(id.getTenantId(), id.getType().getCode(), id.getName(), DPART))))
                .take(1);
    }

    @Override
    public <T> Observable<Row> findMetricInMetricsIndex(MetricId<T> id) {
        return rxSession.executeAndFetch(findMetricInMetricsIndex
                .bind(id.getTenantId(), id.getType().getCode(), id.getName()));
    }

    @Override
    public <T> Observable<Row> getMetricTags(MetricId<T> id) {
        return rxSession.executeAndFetch(getMetricTags.bind(id.getTenantId(), id.getType().getCode(), id.getName()));
    }

    @Override
    public Observable<Row> getTagNames() {
        return rxSession.executeAndFetch(getTagNames.bind());
    }

    @Override
    public Observable<Row> getTagNamesWithType() {
        return rxSession.executeAndFetch(getTagNamesWithType.bind());
    }

    @Override
    public <T> Observable<ResultSet> addTags(Metric<T> metric, Map<String, String> tags) {
        MetricId<T> metricId = metric.getMetricId();
        BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);

        batch.add(addTagsToMetricsIndex.bind(tags, metricId.getTenantId(), metricId.getType().getCode(),
                metricId.getName()));
        tags.forEach((key, value) -> batch.add(insertMetricsTagsIndex.bind(metricId.getTenantId(), key, value,
                metricId.getType().getCode(), metricId.getName())));

        return rxSession.execute(batch)
                .compose(applyWriteRetryPolicy("Failed to insert metric tags for metric id " + metricId));
    }

    @Override
    public <T> Observable<ResultSet> deleteTags(Metric<T> metric, Map<String, String> tags) {
        MetricId<T> metricId = metric.getMetricId();
        BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);

        batch.add(deleteTagsFromMetricsIndex.bind(tags.keySet(), metricId.getTenantId(), metricId.getType().getCode(),
                metricId.getName()));
        tags.forEach((key, value) -> batch.add(deleteMetricsTagsIndex.bind(metricId.getTenantId(), key, value,
                metricId.getType().getCode(), metricId.getName())));

        return rxSession.execute(batch)
                .compose(applyWriteRetryPolicy("Failed to delete metric tags for metric id " + metricId));
    }

    @Override
    public <T> Observable<ResultSet> deleteFromMetricsIndexAndTags(MetricId<T> id, Map<String, String> tags) {
        BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);

        batch.add(deleteMetricFromMetricsIndex.bind(id.getTenantId(), id.getType().getCode(), id.getName()));
        tags.forEach((key, value) -> batch.add(deleteMetricsTagsIndex.bind(id.getTenantId(), key, value,
                id.getType().getCode(), id.getName())));

        return rxSession.execute(batch)
                .compose(applyWriteRetryPolicy("Failed to delete metric and tags for metric id " + id));
    }

    @Override
    public <T> Observable<Integer> updateMetricsIndex(Observable<Metric<T>> metrics) {
        return metrics.map(Metric::getMetricId)
                .map(id -> updateMetricsIndex.bind(id.getTenantId(), id.getType().getCode(), id.getName()))
                .compose(new BatchStatementTransformer())
                .flatMap(batch -> rxSession.execute(batch).map(resultSet -> batch.size()));
    }

    @Override
    public <T> Observable<Row> findMetricsInMetricsIndex(String tenantId, MetricType<T> type) {
        return rxSession.executeAndFetch(readMetricsIndex.bind(tenantId, type.getCode()));
    }


    @Override
    public Observable<Row> findAllMetricsInData() {
        return Observable.from(allMetricsInDatas)
                .map(PreparedStatement::bind)
                .flatMap(b -> rxSession.executeAndFetch(b))
                .concatWith(
                        rxSession.executeAndFetch(findAllMetricsInData.bind())
                                .concatWith(rxSession.executeAndFetch(findAllMetricsInDataCompressed.bind())));
    }

    /*
     * Applies micro-batching capabilities by taking advantage of token ranges in the Cassandra
     */
    private Observable.Transformer<BoundStatement, Integer> applyMicroBatching() {
        return tObservable -> tObservable
                .groupBy(b -> {
                    ByteBuffer routingKey = b.getRoutingKey(ProtocolVersion.NEWEST_SUPPORTED,
                            codecRegistry);
                    Token token = metadata.newToken(routingKey);
                    for (TokenRange tokenRange : session.getCluster().getMetadata().getTokenRanges()) {
                        if (tokenRange.contains(token)) {
                            return tokenRange;
                        }
                    }
                    log.warn("Unable to find any Cassandra node to insert token " + token.toString());
                    return session.getCluster().getMetadata().getTokenRanges().iterator().next();
                })
                .flatMap(g -> g.compose(new BoundBatchStatementTransformer()))
                .flatMap(batch -> rxSession
                        .execute(batch)
                        .compose(applyWriteRetryPolicy("Failed to insert batch of data points"))
                        .map(resultSet -> batch.size())
                );
    }

    /*
     * Apply our current retry policy to the insert behavior
     */
    private <T> Observable.Transformer<T, T> applyWriteRetryPolicy(String msg) {
        return tObservable -> tObservable
                .retryWhen(errors -> {
                    Observable<Integer> range = Observable.range(1, 2);
                    return errors
                            .zipWith(range, (t, i) -> {
                                if (t instanceof DriverException) {
                                    return i;
                                }
                                throw Exceptions.propagate(t);
                            })
                            .flatMap(retryCount -> {
                                long delay = (long) Math.min(Math.pow(2, retryCount) * 1000, 3000);
                                log.debug(msg);
                                log.debugf("Retrying batch insert in %d ms", delay);
                                return Observable.timer(delay, TimeUnit.MILLISECONDS);
                            });
                });
    }

    int getBucketIndex(long timestamp) {
        int hour = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), UTC)
                .with(DateTimeService.startOfPreviousEvenHour())
                .getHour();

        return Math.floorDiv(hour, 2);
    }

    @Override
    public <T> Observable<Integer> insertData(Observable<Metric<T>> metrics) {
        return metrics
                .flatMap(m -> Observable.from(m.getDataPoints())
                .compose(mapTempInsertStatement(m)))
                .compose(applyMicroBatching());
    }

    @SuppressWarnings("unchecked")
    private <T> Observable.Transformer<DataPoint<T>, BoundStatement> mapTempInsertStatement(Metric<T> metric) {
        MetricType<T> type = metric.getMetricId().getType();
        MetricId<T> metricId = metric.getMetricId();
        return tO -> tO
                .map(dataPoint -> {
                    BoundStatement bs;
                    int i = 1;
                    if (dataPoint.getTags().isEmpty()) {
                        bs = dataTable[type.getCode()][getBucketIndex(dataPoint.getTimestamp())].bind();
                    } else {
                        bs = dataWithTagsTable[type.getCode()][getBucketIndex(dataPoint.getTimestamp())].bind();
                        bs.setMap(1, dataPoint.getTags());
                        i++;
                    }
                    bindValue(bs, type, dataPoint);
                    return bs
                            .setString(i, metricId.getTenantId())
                            .setByte(++i, metricId.getType().getCode())
                            .setString(++i, metricId.getName())
                            .setLong(++i, DPART)
                            .setTimestamp(++i, new Date(dataPoint.getTimestamp()));
                });
    }

    private <T> void bindValue(BoundStatement bs, MetricType<T> type, DataPoint<T> dataPoint) {
        switch(type.getCode()) {
            case 0:
                bs.setDouble(0, (Double) dataPoint.getValue());
                break;
            case 1:
                bs.setBytes(0, getBytes((DataPoint<AvailabilityType>) dataPoint));
                break;
            case 2:
                bs.setLong(0, (Long) dataPoint.getValue());
                break;
            case 3:
                bs.setLong(0, (Long) dataPoint.getValue());
                break;
            case 4:
                throw new IllegalArgumentException("Not implemented yet");
            case 5:
                bs.setDouble(0, (Double) dataPoint.getValue());
                break;
            default:
                throw new IllegalArgumentException("Unsupported metricType");
        }
    }

    private Observable.Transformer<DataPoint<String>, BoundStatement> mapStringDatapoint(Metric<String> metric, int
            ttl, int maxSize) {
        return tObservable -> tObservable
                .map(dataPoint -> {
                    if (maxSize != -1 && dataPoint.getValue().length() > maxSize) {
                        throw new IllegalArgumentException(dataPoint + " exceeds max string length of " + maxSize +
                                " characters");
                    }

                    if (dataPoint.getTags().isEmpty()) {
                        if (ttl >= 0) {
                            return bindDataPoint(insertStringDataUsingTTL, metric, dataPoint.getValue(),
                                    dataPoint.getTimestamp(), ttl);
                        } else {
                            return bindDataPoint(insertStringData, metric, dataPoint.getValue(),
                                    dataPoint.getTimestamp());
                        }
                    } else {
                        if (ttl >= 0) {
                            return bindDataPoint(insertStringDataWithTagsUsingTTL, metric, dataPoint.getValue(),
                                    dataPoint.getTags(), dataPoint.getTimestamp(), ttl);
                        } else {
                            return bindDataPoint(insertStringDataWithTags, metric, dataPoint.getValue(),
                                    dataPoint.getTags(), dataPoint.getTimestamp());
                        }
                    }
                });
    }

    @Override
    public Observable<Integer> insertStringDatas(Observable<Metric<String>> strings,
            Function<MetricId<String>, Integer> ttlFetcher, int maxSize) {

        return strings
                .flatMap(string -> {
                            int ttl = ttlFetcher.apply(string.getMetricId());
                            return Observable.from(string.getDataPoints())
                                    .compose(mapStringDatapoint(string, ttl, maxSize));
                        }
                )
                .compose(applyMicroBatching());
    }

    @Override
    public Observable<Integer> insertStringData(Metric<String> metric, int maxSize) {
        return insertStringData(metric, -1, maxSize);
    }

    @Override
    public Observable<Integer> insertStringData(Metric<String> metric, int ttl, int maxSize) {
        return Observable.from(metric.getDataPoints())
                .compose(mapStringDatapoint(metric, ttl, maxSize))
                .compose(new BatchStatementTransformer())
                .flatMap(batch -> rxSession.execute(batch).map(resultSet -> batch.size()));
    }

    private BoundStatement bindDataPoint(PreparedStatement statement, Metric<?> metric, Object value, long timestamp) {
        MetricId<?> metricId = metric.getMetricId();
        return statement.bind(value, metricId.getTenantId(), metricId.getType().getCode(), metricId.getName(),
                DPART, getTimeUUID(timestamp));
    }

    private BoundStatement bindDataPoint(PreparedStatement statement, Metric<?> metric, Object value, long timestamp,
            int ttl) {
        MetricId<?> metricId = metric.getMetricId();
        return statement.bind(ttl, value, metricId.getTenantId(), metricId.getType().getCode(), metricId.getName(),
                DPART, getTimeUUID(timestamp));
    }

    private BoundStatement bindDataPoint(PreparedStatement statement, Metric<?> metric, Object value,
            Map<String, String> tags, long timestamp) {
        MetricId<?> metricId = metric.getMetricId();
        return statement.bind(value, tags, metricId.getTenantId(), metricId.getType().getCode(),
                metricId.getName(), DPART, getTimeUUID(timestamp));
    }

    private BoundStatement bindDataPoint(PreparedStatement statement, Metric<?> metric, Object value,
            Map<String, String> tags, long timestamp, int ttl) {
        MetricId<?> metricId = metric.getMetricId();
        return statement.bind(ttl, value, tags, metricId.getTenantId(), metricId.getType().getCode(),
                metricId.getName(), DPART, getTimeUUID(timestamp));
    }

    @Override
    public Observable<Row> findCompressedData(MetricId<?> id, long startTime, long endTime, int limit, Order
            order) {
        if (order == Order.ASC) {
            if (limit <= 0) {
                return rxSession.executeAndFetch(findCompressedDataByDateRangeExclusiveASC.bind(id.getTenantId(),
                        id.getType().getCode(), id.getName(), DPART, new Date(startTime), new Date(endTime)));
            } else {
                return rxSession.executeAndFetch(findCompressedDataByDateRangeExclusiveWithLimitASC.bind(
                        id.getTenantId(), id.getType().getCode(), id.getName(), DPART, new Date(startTime),
                        new Date(endTime), limit));
            }
        } else {
            if (limit <= 0) {
                return rxSession.executeAndFetch(findCompressedDataByDateRangeExclusive.bind(id.getTenantId(),
                        id.getType().getCode(), id.getName(), DPART, new Date(startTime), new Date(endTime)));
            } else {
                return rxSession.executeAndFetch(findCompressedDataByDateRangeExclusiveWithLimit.bind(id.getTenantId(),
                        id.getType().getCode(), id.getName(), DPART, new Date(startTime), new Date(endTime),
                        limit));
            }
        }
    }

    private Integer[] tempBuckets(long startTime, long endTime, Order order) {
        ZonedDateTime endZone = ZonedDateTime.ofInstant(Instant.ofEpochMilli(endTime), UTC)
                .with(DateTimeService.startOfPreviousEvenHour());

        ZonedDateTime startZone = ZonedDateTime.ofInstant(Instant.ofEpochMilli(startTime), UTC)
                .with(DateTimeService.startOfPreviousEvenHour());

        // Max time back is <24 hours.
        if(startZone.isBefore(endZone.minus(23, ChronoUnit.HOURS))) {
            startZone = endZone.minus(23, ChronoUnit.HOURS);
        }

        ConcurrentSkipListSet<Integer> buckets = new ConcurrentSkipListSet<>();

        while(startZone.isBefore(endZone)) {
            buckets.add(getBucketIndex(startZone.toInstant().toEpochMilli()));
            startZone = startZone.plus(1, ChronoUnit.HOURS);
        }

        buckets.add(getBucketIndex(endZone.toInstant().toEpochMilli()));

        if(order == Order.DESC) {
            return buckets.descendingSet().stream().toArray(Integer[]::new);
        } else {
            return buckets.stream().toArray(Integer[]::new);
        }
    }

    @Override
    public <T> Observable<Row> findTempData(MetricId<T> id, long startTime, long endTime, int limit, Order order,
                                            int pageSize) {
        // Find out which tables we'll need to scan
        Integer[] buckets = tempBuckets(startTime, endTime, order);
        MetricType<T> type = id.getType();

        if (order == Order.ASC) {
            if (limit <= 0) {
                return Observable.from(buckets)
                        .concatMap(i -> rxSession.executeAndFetch(dataByDateRangeExclusiveASC[type.getCode()][i]
                                .bind(id.getTenantId(), id.getType().getCode(), id.getName(), DPART,
                                        new Date(startTime), new Date(endTime)).setFetchSize(pageSize)));
            } else {
                return Observable.from(buckets)
                        .concatMap(i -> rxSession.executeAndFetch(dataByDateRangeExclusiveWithLimitASC[type.getCode()][i].bind(
                                id.getTenantId(), id.getType().getCode(), id.getName(), DPART, new Date(startTime),
                                new Date(endTime), limit).setFetchSize(pageSize)));
            }
        } else {
            if (limit <= 0) {
                return Observable.from(buckets)
                        .concatMap(i -> rxSession.executeAndFetch(dateRangeExclusive[type.getCode()][i].bind(id.getTenantId(),
                                id.getType().getCode(), id.getName(), DPART, new Date(startTime), new Date(endTime))
                                .setFetchSize(pageSize)));
            } else {
                return Observable.from(buckets)
                        .concatMap(i -> rxSession.executeAndFetch(dateRangeExclusiveWithLimit[type.getCode()][i].bind(id.getTenantId(),
                                id.getType().getCode(), id.getName(), DPART, new Date(startTime), new Date(endTime),
                                limit).setFetchSize(pageSize)));
            }
        }
    }

    @Override
    public <T> Observable<Row> findOldData(MetricId<T> id, long startTime, long endTime, int limit, Order order,
                                           int pageSize) {
        MetricType<T> type = id.getType();

        if (order == Order.ASC) {
            if (limit <= 0) {
                return rxSession.executeAndFetch(dataByDateRangeExclusiveASC[type.getCode()][POS_OF_OLD_DATA]
                        .bind(id.getTenantId(), id.getType().getCode(), id.getName(), DPART,
                                getTimeUUID(startTime), getTimeUUID(endTime)).setFetchSize(pageSize))
                        .doOnError(Throwable::printStackTrace);
            } else {
                return rxSession.executeAndFetch(dataByDateRangeExclusiveWithLimitASC[type.getCode()][POS_OF_OLD_DATA].bind(
                        id.getTenantId(), id.getType().getCode(), id.getName(), DPART, getTimeUUID(startTime),
                        getTimeUUID(endTime), limit).setFetchSize(pageSize))
                        .doOnError(Throwable::printStackTrace);
            }
        } else {
            if (limit <= 0) {
                return rxSession.executeAndFetch(dateRangeExclusive[type.getCode()][POS_OF_OLD_DATA].bind(id.getTenantId(),
                        id.getType().getCode(), id.getName(), DPART, getTimeUUID(startTime), getTimeUUID(endTime))
                        .setFetchSize(pageSize))
                        .doOnError(Throwable::printStackTrace);
            } else {
                return rxSession.executeAndFetch(dateRangeExclusiveWithLimit[type.getCode()][POS_OF_OLD_DATA].bind(
                        id.getTenantId(), id.getType().getCode(), id.getName(), DPART, getTimeUUID(startTime),
                        getTimeUUID(endTime), limit).setFetchSize(pageSize))
                        .doOnError(Throwable::printStackTrace);
            }
        }
    }

    @Override
    public Observable<Row> findStringData(MetricId<String> id, long startTime, long endTime, int limit, Order order,
            int pageSize) {
        if (order == Order.ASC) {
            if (limit <= 0) {
                return rxSession.executeAndFetch(findStringDataByDateRangeExclusiveASC.bind(id.getTenantId(),
                        STRING.getCode(), id.getName(), DPART, getTimeUUID(startTime), getTimeUUID(endTime))
                        .setFetchSize(pageSize));
            } else {
                return rxSession.executeAndFetch(findStringDataByDateRangeExclusiveWithLimitASC.bind(
                        id.getTenantId(), STRING.getCode(), id.getName(), DPART, getTimeUUID(startTime),
                        getTimeUUID(endTime), limit).setFetchSize(pageSize));
            }
        } else {
            if (limit <= 0) {
                return rxSession.executeAndFetch(findStringDataByDateRangeExclusive.bind(id.getTenantId(),
                        STRING.getCode(), id.getName(), DPART, getTimeUUID(startTime), getTimeUUID(endTime))
                        .setFetchSize(pageSize));
            } else {
                return rxSession.executeAndFetch(findStringDataByDateRangeExclusiveWithLimit.bind(id.getTenantId(),
                        STRING.getCode(), id.getName(), DPART, getTimeUUID(startTime), getTimeUUID(endTime),
                        limit).setFetchSize(pageSize));
            }
        }
    }

    @Override
    public Observable<Row> findAvailabilityData(MetricId<AvailabilityType> id, long startTime, long endTime,
            int limit, Order order, int pageSize) {
        if (order == Order.ASC) {
            if (limit <= 0) {
                return rxSession.executeAndFetch(findAvailabilitiesASC.bind(id.getTenantId(),
                        AVAILABILITY.getCode(), id.getName(), DPART, getTimeUUID(startTime), getTimeUUID(endTime))
                        .setFetchSize(pageSize));
            } else {
                return rxSession.executeAndFetch(findAvailabilitiesWithLimitASC.bind(id.getTenantId(),
                        AVAILABILITY.getCode(), id.getName(), DPART, getTimeUUID(startTime), getTimeUUID(endTime),
                        limit).setFetchSize(pageSize));
            }
        } else {
            if (limit <= 0) {
                return rxSession.executeAndFetch(findAvailabilities.bind(id.getTenantId(), AVAILABILITY.getCode(),
                        id.getName(), DPART, getTimeUUID(startTime), getTimeUUID(endTime)).setFetchSize(pageSize));
            } else {
                return rxSession.executeAndFetch(findAvailabilitiesWithLimit.bind(id.getTenantId(),
                        AVAILABILITY.getCode(), id.getName(), DPART, getTimeUUID(startTime), getTimeUUID(endTime),
                        limit).setFetchSize(pageSize));
            }
        }
    }

    @Override
    public Observable<Row> findAvailabilityData(MetricId<AvailabilityType> id, long timestamp) {
        return rxSession.executeAndFetch(findAvailabilityByDateRangeInclusive.bind(id.getTenantId(),
                AVAILABILITY.getCode(), id.getName(), DPART, UUIDs.startOf(timestamp), UUIDs.endOf(timestamp)));
    }

    @Override
    public <T> Observable<ResultSet> deleteMetricData(MetricId<T> id) {
        return rxSession.execute(deleteMetricData.bind(id.getTenantId(), id.getType().getCode(), id.getName(), DPART));
    }

    @Override
    public <T> Observable<ResultSet> deleteMetricFromRetentionIndex(MetricId<T> id) {
        return rxSession
                .execute(deleteFromMetricRetentionIndex.bind(id.getTenantId(), id.getType().getCode(), id.getName()));
    }

    @Override
    public <T> Observable<ResultSet> deleteMetricFromMetricsIndex(MetricId<T> id) {
        return rxSession
                .execute(deleteMetricFromMetricsIndex.bind(id.getTenantId(), id.getType().getCode(), id.getName()));
    }

    private Observable.Transformer<DataPoint<AvailabilityType>, BoundStatement> mapAvailabilityDatapoint(
            Metric<AvailabilityType> metric, int ttl) {
        return a -> a
                .map(dataPoint -> {
                    if (dataPoint.getTags().isEmpty()) {
                        if (ttl >= 0) {
                            return bindDataPoint(insertAvailabilityUsingTTL, metric, getBytes(dataPoint),
                                    dataPoint.getTimestamp(),
                                    ttl);
                        } else{
                            return bindDataPoint(insertAvailability, metric, getBytes(dataPoint),
                                    dataPoint.getTimestamp());
                        }
                    } else {
                        if (ttl >= 0) {
                            return bindDataPoint(insertAvailabilityWithTagsUsingTTL, metric, getBytes(dataPoint),
                                    dataPoint.getTags(), dataPoint.getTimestamp(), ttl);
                        } else {
                            return bindDataPoint(insertAvailabilityWithTags, metric, getBytes(dataPoint),
                                    dataPoint.getTags(), dataPoint.getTimestamp());
                        }
                    }
                });
    }

    @Override
    public Observable<Integer> insertAvailabilityDatas(Observable<Metric<AvailabilityType>> avail,
            Function<MetricId<AvailabilityType>, Integer> ttlFetcher) {
        return avail
                .flatMap(a -> {
                            int ttl = ttlFetcher.apply(a.getMetricId());
                            return Observable.from(a.getDataPoints())
                                    .compose(mapAvailabilityDatapoint(a, ttl));
                        }
                )
                .compose(applyMicroBatching());
    }

    @Override
    public Observable<Integer> insertAvailabilityData(Metric<AvailabilityType> metric) {
        return insertAvailabilityData(metric, -1);
    }

    @Override
    public Observable<Integer> insertAvailabilityData(Metric<AvailabilityType> metric, int ttl) {
        return Observable.from(metric.getDataPoints())
                .compose(mapAvailabilityDatapoint(metric, ttl))
                .compose(new BatchStatementTransformer())
                .flatMap(batch -> rxSession.execute(batch).map(resultSet -> batch.size()));
    }

    private ByteBuffer getBytes(DataPoint<AvailabilityType> dataPoint) {
        return ByteBuffer.wrap(new byte[]{dataPoint.getValue().getCode()});
    }

    @Override
    public <T> ResultSetFuture findDataRetentions(String tenantId, MetricType<T> type) {
        return session.executeAsync(findDataRetentions.bind(tenantId, type.getCode()));
    }

    @Override
    public <T> Observable<ResultSet> updateRetentionsIndex(String tenantId, MetricType<T> type,
                                                       Map<String, Integer> retentions) {
        return Observable.from(retentions.entrySet())
                .map(entry -> updateRetentionsIndex.bind(tenantId, type.getCode(), entry.getKey(), entry.getValue()))
                .compose(new BatchStatementTransformer())
                .flatMap(rxSession::execute);
    }

    @Override
    public Observable<Row> findMetricsByTagName(String tenantId, String tag) {
        return rxSession.executeAndFetch(findMetricsByTagName.bind(tenantId, tag));
    }

    @Override
    public Observable<Row> findMetricsByTagNameValue(String tenantId, String tag, String tvalue) {
        return rxSession.executeAndFetch(findMetricsByTagNameValue.bind(tenantId, tag, tvalue));
    }

    @Override
    public <T> ResultSetFuture updateRetentionsIndex(Metric<T> metric) {
        return session.executeAsync(updateRetentionsIndex.bind(metric.getMetricId().getTenantId(),
                metric.getMetricId().getType().getCode(), metric.getMetricId().getName(), metric.getDataRetention()));
    }

    @Override
    public <T> Observable<ResultSet> deleteAndInsertCompressedGauge(MetricId<T> id, long timeslice,
                                                                    CompressedPointContainer cpc,
                                                                    long sliceStart, long sliceEnd, int ttl) {

        // ByteBuffer position must be 0!
        Observable.just(cpc.getValueBuffer(), cpc.getTimestampBuffer(), cpc.getTagsBuffer())
                .doOnNext(bb -> {
                    if(bb != null && bb.position() != 0) {
                        bb.rewind();
                    }
                });

        BiConsumer<BoundStatement, Integer> mapper = (b, i) -> {
            b.setString(i, id.getTenantId())
                    .setByte(i+1, id.getType().getCode())
                    .setString(i+2, id.getName())
                    .setLong(i+3, DPART)
                    .setTimestamp(i+4, new Date(timeslice));
        };

        BoundStatement b;
        int i = 0;
        if(cpc.getTagsBuffer() != null) {
            b = insertCompressedDataWithTags.bind()
                    .setInt(i, ttl)
                    .setBytes(i+1, cpc.getValueBuffer())
                    .setBytes(i+2, cpc.getTagsBuffer());
            mapper.accept(b, 3);
        } else {
            b = insertCompressedData.bind()
                    .setInt(i, ttl)
                    .setBytes(i+1, cpc.getValueBuffer());
            mapper.accept(b, 2);
        }

        return Observable.just(deleteMetricDataWithLimit.bind()
                .setString(0, id.getTenantId())
                .setByte(1, id.getType().getCode())
                .setString(2, id.getName())
                .setLong(3, DPART)
                .setUUID(4, getTimeUUID(sliceStart))
                .setUUID(5, getTimeUUID(sliceEnd)))
                .concatWith(Observable.just(b))
                .concatMap(st -> rxSession.execute(st));
    }

    @Override
    public Observable<Row> findAllMetricsFromTagsIndex() {
        return rxSession.executeAndFetch(findAllMetricsFromTagsIndex.bind());
    }

    @Override
    public <T> Observable<ResultSet> updateMetricExpirationIndex(MetricId<T> id, long expirationTime) {
        return rxSession.execute(updateMetricExpirationIndex.bind(id.getTenantId(),
                id.getType().getCode(), id.getName(), new Date(expirationTime)));
    }

    @Override
    public <T> Observable<ResultSet> deleteFromMetricExpirationIndex(MetricId<T> id) {
        return rxSession
                .execute(deleteFromMetricExpirationIndex.bind(id.getTenantId(), id.getType().getCode(), id.getName()));
    }

    @Override
    public <T> Observable<Row> findMetricExpiration(MetricId<T> id) {
        return rxSession
                .executeAndFetch(findMetricExpiration.bind(id.getTenantId(), id.getType().getCode(), id.getName()));
    }
}
