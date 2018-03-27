/*
 * Copyright 2014-2018 Red Hat, Inc. and/or its affiliates
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
import static org.hawkular.metrics.model.MetricType.STRING;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

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

import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.AggregateMetadata;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.FunctionMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.MaterializedViewMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SchemaChangeListener;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

import rx.Observable;
import rx.exceptions.Exceptions;
import rx.schedulers.Schedulers;

/**
 *
 * @author John Sanda
 */
public class DataAccessImpl implements DataAccess {

    private static final CoreLogger log = CoreLogging.getCoreLogger(DataAccessImpl.class);

    public static final String OUT_OF_ORDER_TABLE_NAME = "data_0";
    public static final String TEMP_TABLE_NAME_PROTOTYPE = "data_temp_";
    public static final String TEMP_TABLE_NAME_FORMAT_STRING = TEMP_TABLE_NAME_PROTOTYPE + "%s";

    public static final long DPART = 0;
    private Session session;

    private RxSession rxSession;

    private LoadBalancingPolicy loadBalancingPolicy;

    // Turn to MetricType agnostic
    // See getMapKey(byte, int)
    private NavigableMap<Long, Map<Integer, PreparedStatement>> prepMap;

    // TODO Move all of these to a new class (Cassandra specific temp table) to allow multiple implementations (such
    // as in-memory + WAL in Cassandra)

    private TemporaryTableStatementCreator tableCreator = null;

    private enum StatementType {
        READ, WRITE, SCAN, CREATE, DELETE
    }

    private enum TempStatement {
        // Should I map these to something else..? More like a key to the actual statement
        dateRangeExclusive(byDateRangeExclusiveBase, StatementType.READ),
        dateRangeExclusiveWithLimit(dateRangeExclusiveWithLimitBase, StatementType.READ),
        dataByDateRangeExclusiveASC(dataByDateRangeExclusiveASCBase, StatementType.READ),
        dataByDateRangeExclusiveWithLimitASC(dataByDateRangeExclusiveWithLimitASCBase, StatementType.READ),
        SCAN_WITH_TOKEN_RANGES(scanTableBase, StatementType.SCAN),
        CHECK_EXISTENCE_OF_METRIC_IN_TABLE(findMetricInDataBase, StatementType.SCAN),
        LIST_ALL_METRICS_FROM_TABLE(findAllMetricsInDataBases, StatementType.SCAN),
        INSERT_DATA(data, StatementType.WRITE),
        INSERT_DATA_WITH_TAGS(dataWithTags, StatementType.WRITE),
        CREATE_TABLE(TEMP_TABLE_BASE_CREATE, StatementType.CREATE),
//        CREATE_WAL(TEMP_TABLE_WAL_CREATE, StatementType.CREATE);
        DELETE_DATA(DELETE_FROM_DATA_BASE, StatementType.DELETE);

        private final String statement;
        private StatementType type;

        TempStatement(String st, StatementType t) {
            statement = st;
            type = t;
        }

        public String getStatement() {
            return statement;
        }

        public StatementType getType() {
            return type;
        }
    }

    // Read statement prototypes

    private static String byDateRangeExclusiveBase =
            "SELECT time, %s, tags FROM %s " +
                    "WHERE tenant_id = ? AND type = ? AND metric = ? AND time >= ? AND time < ?";

    private static String dateRangeExclusiveWithLimitBase =
            "SELECT time, %s, tags FROM %s " +
                    " WHERE tenant_id = ? AND type = ? AND metric = ? AND time >= ? AND time < ?" +
                    " LIMIT ?";

    private static String dataByDateRangeExclusiveASCBase =
            "SELECT time, %s, tags FROM %s " +
                    "WHERE tenant_id = ? AND type = ? AND metric = ? AND time >= ?" +
                    " AND time < ? ORDER BY time ASC";

    private static String dataByDateRangeExclusiveWithLimitASCBase =
            "SELECT time, %s, tags FROM %s" +
                    " WHERE tenant_id = ? AND type = ? AND metric = ? AND time >= ?" +
                    " AND time < ? ORDER BY time ASC" +
                    " LIMIT ?";

    private static String scanTableBase =
            "SELECT tenant_id, type, metric, time, n_value, availability, l_value, tags, token(tenant_id, type, " +
                    "metric) FROM %s " +
                    "WHERE token(tenant_id, type, metric) > ? AND token(tenant_id, type, metric) <=" +
                    " ?";

    // Create statement prototype

    private static String TEMP_TABLE_BASE_CREATE = "CREATE TABLE %s ( " +
            "tenant_id text, " +
            "type tinyint, " +
            "metric text, " +
            "time timestamp, " +
            "n_value double, " +
            "availability blob, " +
            "l_value bigint, " +
            "tags map<text,text>, " +
            "PRIMARY KEY ((tenant_id, type, metric), time)" +
            ") WITH CLUSTERING ORDER BY (time DESC)";

    // For in-memory buffering
    /*
    private static String TEMP_TABLE_WAL_CREATE = "CREATE TABLE %s (" +
            "tenant_id text, " +
            "type tinyint, " +
            "metric text, " +
            "dpart bigint, " +
            "time timestamp, " +
            "count int, " +
            "value blob, " +
            "tags blob, " +
            "PRIMARY KEY ((tenant_id, type, metric), time) " +
            ") WITH CLUSTERING ORDER BY (time DESC)";
    */

    // Insert statement prototypes

    private static String data = "UPDATE %s " +
            "SET %s = ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND time = ? ";

    private static String dataWithTags = "UPDATE %s " +
            "SET %s = ?, tags = ? " +
            "WHERE tenant_id = ? AND type = ? AND metric = ? AND time = ? ";

    // Metric definition prototypes

    private static String findMetricInDataBase = "SELECT DISTINCT tenant_id, type, metric " +
            "FROM %s " +
            "WHERE tenant_id = ? AND type = ? AND metric = ?";

    private static String findAllMetricsInDataBases = "SELECT DISTINCT tenant_id, type, metric " +
            "FROM %s";

    // Delete statement prototypes

    private static String DELETE_FROM_DATA_BASE = "DELETE FROM %s " +
            "WHERE tenant_id = ? AND type = ? AND metric = ?";

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

    private PreparedStatement deleteMetricData;

    private PreparedStatement deleteFromMetricRetentionIndex;

    private PreparedStatement deleteMetricFromMetricsIndex;

    private PreparedStatement deleteMetricDataWithLimit;

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

    private static DateTimeFormatter TEMP_TABLE_DATEFORMATTER = (new DateTimeFormatterBuilder())
            .appendValue(ChronoField.YEAR, 4)
            .appendValue(ChronoField.MONTH_OF_YEAR, 2)
            .appendValue(ChronoField.DAY_OF_MONTH, 2)
            .appendValue(ChronoField.HOUR_OF_DAY, 2).toFormatter();

    private CodecRegistry codecRegistry;
    private Metadata metadata;

    public DataAccessImpl(Session session) {
        this.session = session;
        rxSession = new RxSessionImpl(session);
        loadBalancingPolicy = session.getCluster().getConfiguration().getPolicies().getLoadBalancingPolicy();
        codecRegistry = session.getCluster().getConfiguration().getCodecRegistry();
        metadata = session.getCluster().getMetadata();

        initPreparedStatements();
        initializeTemporaryTableStatements();
    }

    /**
     * Creates a key with ordinal and MetricType code for the NavigableMap.
     *
     * First 8 bits are reserved for the metricType code and the rest 24 bits are reserved for the ordinal
     *
     * @param code MetricType code
     * @param ordinal TempStatement ordinal()
     *
     * @return Integer with those two combined
     */
    private Integer getMapKey(byte code, int ordinal) {
        int key = ordinal;
        key |= code << 24;
        return key;
    }

    private Integer getMapKey(MetricType type, TempStatement ts) {
        return getMapKey(type.getCode(), ts.ordinal());
    }

    void prepareTempStatements(String tableName, Long mapKey) {
        // Create an entry to the correct Long
        Map<Integer, PreparedStatement> statementMap = new HashMap<>();

        // Per metricType
        for (MetricType<?> metricType : MetricType.userTypes()) {
            if(metricType == STRING) { continue; } // We don't support String metrics in temp tables yet
            for (TempStatement st : TempStatement.values()) {
                Integer key = getMapKey(metricType, st);

                String formatSt;
                switch(st.getType()) {
                    case READ:
                        formatSt = String.format(st.getStatement(), metricTypeToColumnName(metricType),
                                tableName);
                        break;
                    case WRITE:
                        formatSt = String.format(st.getStatement(), tableName,
                                metricTypeToColumnName(metricType));
                        break;
                    default:
                        // Not supported
                        continue;
                }

                PreparedStatement prepared = session.prepare(formatSt);
                statementMap.put(key, prepared);
            }
        }
        // Untyped
        for (TempStatement st : TempStatement.values()) {
            Integer key = getMapKey(MetricType.UNDEFINED, st);
            String formatSt;
            switch(st.getType()) {
                case SCAN:
                case CREATE:
                case DELETE:
                    formatSt = String.format(st.getStatement(), tableName);
                    break;
                default:
                    continue;
            }
            PreparedStatement prepared = session.prepare(formatSt);
            statementMap.put(key, prepared);
        }
        prepMap.put(mapKey, statementMap);
    }

    @Override
    public Observable<ResultSet> createTempTablesIfNotExists(final Set<Long> timestamps) {
        return Observable.fromCallable(() -> {
            Set<String> tables = timestamps.stream()
                    .map(this::getTempTableName)
                    .collect(Collectors.toSet());

            // TODO This is an IO operation..
            metadata.getKeyspace(session.getLoggedKeyspace()).getTables().stream()
                    .map(AbstractTableMetadata::getName)
                    .filter(t -> t.startsWith(TEMP_TABLE_NAME_PROTOTYPE))
                    .forEach(tables::remove);

            return tables;
        })

                .flatMapIterable(s -> s)
                .zipWith(Observable.interval(300, TimeUnit.MILLISECONDS), (st, l) -> st)
                .concatMap(this::createTemporaryTable);
    }

    Observable<ResultSet> createTemporaryTable(String tempTableName) {
        return Observable.just(tempTableName)
                .map(t -> new SimpleStatement(String.format(TempStatement.CREATE_TABLE.getStatement(), t)))
                .flatMap(st -> rxSession.execute(st));
    }

    private void initializeTemporaryTableStatements() {
        prepMap = new ConcurrentSkipListMap<>();
        setTempTableCreator(new TemporaryTableStatementCreator());

        boolean zeroTableExists = false;

        // At startup we should initialize preparedStatements for all the temporary tables that exists
        for (TableMetadata table : metadata.getKeyspace(session.getLoggedKeyspace()).getTables()) {
            if(table.getName().startsWith(TEMP_TABLE_NAME_PROTOTYPE)) {
                // Proceed to create the preparedStatements against this table
                Long mapKey = tableToMapKey(table.getName());
                prepareTempStatements(table.getName(), mapKey);
            } else if(table.getName().equals(OUT_OF_ORDER_TABLE_NAME)) {
                zeroTableExists = true;
            }
        }

        if(!zeroTableExists) {
            createTemporaryTable(OUT_OF_ORDER_TABLE_NAME).toBlocking().subscribe();
            session.execute((String.format("ALTER TABLE %s WITH default_time_to_live = %d",
                    OUT_OF_ORDER_TABLE_NAME, TimeUnit.DAYS.toSeconds(7))));
            session.execute((String.format("ALTER TABLE %s WITH compaction = " +
                            "{'compaction_window_size': '1', " +
                            "'compaction_window_unit': 'DAYS', " +
                            "'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'}",
                    OUT_OF_ORDER_TABLE_NAME)));
        }

        // Prepare the old fashioned way (data table) as fallback when out-of-order writes happen..
        // These should be transparent in writes
        prepareTempStatements(OUT_OF_ORDER_TABLE_NAME, 0L); // Fall back is always at value 0 (floorKey/floorEntry will hit it)
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
                "WHERE tenant_id = ? AND tname = ? AND tvalue IN ?");
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
        return getPrepForAllTempTables(TempStatement.CHECK_EXISTENCE_OF_METRIC_IN_TABLE)
                .map(b -> b.bind(id.getTenantId(), id.getType().getCode(), id.getName()))
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
        BoundStatement stmt = addTagsToMetricsIndex.bind(tags, metricId.getTenantId(), metricId.getType().getCode(),
                metricId.getName());
        return rxSession.execute(stmt);
    }

    @Override
    public <T> Observable<ResultSet> deleteTags(Metric<T> metric, Set<String> tags) {
        MetricId<T> metricId = metric.getMetricId();
        BoundStatement stmt = deleteTagsFromMetricsIndex.bind(tags, metricId.getTenantId(),
                metricId.getType().getCode(), metricId.getName());
        return rxSession.execute(stmt);
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

    /**
     * Fetch all the data from a temporary table for the compression job. Using TokenRanges avoids fetching first
     * all the metrics' partition keys and then requesting them.
     *
     * Performance can be improved by using data locality and fetching with multiple threads.
     *
     * @param timestamp A timestamp inside the wanted bucket (such as the previous starting row timestamp)
     * @param pageSize How many rows to fetch each time
     * @param maxConcurrency To how many streams should token ranges be split to
     * @return Observable of Observables per partition key
     */
    @Override
    public Observable<Observable<Row>> findAllDataFromBucket(long timestamp, int pageSize, int maxConcurrency) {
        PreparedStatement ts =
                getTempStatement(MetricType.UNDEFINED, TempStatement.SCAN_WITH_TOKEN_RANGES, timestamp);

        // The table does not exists - case such as when starting Hawkular-Metrics for the first time just before
        // compression kicks in.
        if(ts == null || prepMap.floorKey(timestamp) == 0L) {
            return Observable.empty();
        }

        return Observable.from(getTokenRanges())
                .map(tr -> rxSession.executeAndFetch(
                        getTempStatement(MetricType.UNDEFINED, TempStatement.SCAN_WITH_TOKEN_RANGES, timestamp)
                                .bind()
                                .setToken(0, tr.getStart())
                                .setToken(1, tr.getEnd())
                                .setFetchSize(pageSize)));
    }

    private Set<TokenRange> getTokenRanges() {
        Set<TokenRange> tokenRanges = new HashSet<>();
        for (TokenRange tokenRange : metadata.getTokenRanges()) {
            tokenRanges.addAll(tokenRange.unwrap());
        }
        return tokenRanges;
    }

    @Override
    public Observable<ResultSet> dropTempTable(long timestamp) {
        String fullTableName = getTempTableName(timestamp);
        String dropCQL = String.format("DROP TABLE IF EXISTS %s", fullTableName);
        return rxSession.execute(dropCQL);
    }

    private Observable<PreparedStatement> getPrepForAllTempTables(TempStatement ts) {
        return Observable.from(prepMap.entrySet())
                .map(Map.Entry::getValue)
                .map(pMap -> pMap.get(getMapKey(MetricType.UNDEFINED, ts)));
    }

    @Override
    public Observable<Row> findAllMetricIdentifiersInData() {
        return getPrepForAllTempTables(TempStatement.LIST_ALL_METRICS_FROM_TABLE)
                .flatMap(b -> rxSession.executeAndFetch(b.bind()))
                .mergeWith(rxSession.executeAndFetch(findAllMetricsInData.bind()))
                .mergeWith(rxSession.executeAndFetch(findAllMetricsInDataCompressed.bind()));
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
                        .compose(applyInsertRetryPolicy())
                        .map(resultSet -> batch.size())
                );
    }

    /*
     * Apply our current retry policy to the insert behavior
     */
    private <T> Observable.Transformer<T, T> applyInsertRetryPolicy() {
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
                                log.debug("Retrying batch insert in " + delay + " ms");
                                return Observable.timer(delay, TimeUnit.MILLISECONDS);
                            });
                });
    }

    Long tableToMapKey(String tableName) {
        LocalDateTime parsed = LocalDateTime
                .parse(tableName.substring(TEMP_TABLE_NAME_PROTOTYPE.length()),
                        TEMP_TABLE_DATEFORMATTER);
        return Long.valueOf(parsed.toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    String getTempTableName(long timestamp) {
        return String.format(TEMP_TABLE_NAME_FORMAT_STRING,
        ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), UTC)
                .with(DateTimeService.startOfPreviousEvenHour())
                .format(TEMP_TABLE_DATEFORMATTER));
    }

    PreparedStatement getTempStatement(MetricType type, TempStatement ts, long timestamp) {
        Map.Entry<Long, Map<Integer, PreparedStatement>> floorEntry = prepMap
                .floorEntry(timestamp);

        if(floorEntry != null) {
                return floorEntry.getValue()
                    .get(getMapKey(type, ts));
        }
        return null;
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
                    PreparedStatement st;
                    if (dataPoint.getTags().isEmpty()) {
                        st = getTempStatement(type, TempStatement.INSERT_DATA, dataPoint.getTimestamp());

                        if(st == null) {
                            return null;
                        }

                        bs = st.bind();
                    } else {
                        st = getTempStatement(type, TempStatement.INSERT_DATA_WITH_TAGS, dataPoint.getTimestamp());

                        if(st == null) {
                            return null;
                        }

                        bs = st.bind();
                        bs.setMap(1, dataPoint.getTags());
                        i++;
                    }
                    bindValue(bs, type, dataPoint);
                    return bs
                            .setString(i, metricId.getTenantId())
                            .setByte(++i, metricId.getType().getCode())
                            .setString(++i, metricId.getName())
                            .setTimestamp(++i, new Date(dataPoint.getTimestamp()));
                })
                .filter(Objects::nonNull);
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

    // TODO These are only used by the String methods
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

    private SortedMap<Long, Map<Integer, PreparedStatement>> subSetMap(long startTime, long endTime, Order order) {
        Long startKey = prepMap.floorKey(startTime);
        Long endKey = prepMap.floorKey(endTime);

        // The start time is already compressed, start the request from earliest non-compressed
        if(startKey == null) {
            startKey = prepMap.ceilingKey(startTime);
        }

        // Just in case even the end is in the past
        if(endKey == null) {
            endKey = startKey;
        }

        // Depending on the order, these must be read in the correct order also..
        SortedMap<Long, Map<Integer, PreparedStatement>> statementMap;
        if(order == Order.ASC) {
             statementMap = prepMap.subMap(startKey, true, endKey,
                    true);
        } else {
            statementMap = new ConcurrentSkipListMap<>((var0, var2) -> var0 < var2?1:(var0 == var2?0:-1));
            statementMap.putAll(prepMap.subMap(startKey, true, endKey, true));
        }

        return statementMap;
    }

    @Override
    public <T> Observable<Row> findTempData(MetricId<T> id, long startTime, long endTime, int limit, Order order,
                                            int pageSize) {
        MetricType<T> type = id.getType();

        SortedMap<Long, Map<Integer, PreparedStatement>> statementMap = subSetMap(startTime, endTime, order);
        Observable<Map<Integer, PreparedStatement>> buckets = Observable.from(statementMap.values());

        if (order == Order.ASC) {
            if (limit <= 0) {
                return buckets
                        .map(m -> m.get(getMapKey(type, TempStatement.dataByDateRangeExclusiveASC)))
                        .concatMap(p -> rxSession.executeAndFetch(p
                                .bind(id.getTenantId(), id.getType().getCode(), id.getName(),
                                        new Date(startTime), new Date(endTime))
                                .setFetchSize(pageSize)));
            } else {
                return buckets
                        .map(m -> m.get(getMapKey(type, TempStatement.dataByDateRangeExclusiveWithLimitASC)))
                        .concatMap(p -> rxSession.executeAndFetch(p
                                .bind(id.getTenantId(), id.getType().getCode(), id.getName(), new Date(startTime),
                                        new Date(endTime), limit)
                                .setFetchSize(pageSize)));
            }
        } else {
            if (limit <= 0) {
                return buckets
                        .map(m -> m.get(getMapKey(type, TempStatement.dateRangeExclusive)))
                        .concatMap(p -> rxSession.executeAndFetch(p
                                .bind(id.getTenantId(), id.getType().getCode(), id.getName(), new Date(startTime),
                                        new Date(endTime))
                                .setFetchSize(pageSize)));
            } else {
                return buckets
                        .map(m -> m.get(getMapKey(type, TempStatement.dateRangeExclusiveWithLimit)))
                        .concatMap(p -> rxSession.executeAndFetch(p
                                .bind(id.getTenantId(), id.getType().getCode(), id.getName(), new Date(startTime), new Date(endTime),
                                        limit)
                                .setFetchSize(pageSize)));
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
    public <T> Observable<ResultSet> deleteMetricData(MetricId<T> id) {
        if(id.getType() == STRING) {
            return rxSession.execute(deleteMetricData.bind(id.getTenantId(), id.getType().getCode(), id.getName(), DPART));
        }

        return getPrepForAllTempTables(TempStatement.DELETE_DATA)
                .flatMap(p -> rxSession.execute(p.bind(id.getTenantId(), id.getType().getCode(), id.getName())));
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
    public <T> Observable<ResultSet> insertIntoMetricsTagsIndex(Metric<T> metric, Map<String, String> tags) {
        MetricId<T> metricId = metric.getMetricId();
        return tagsUpdates(tags, (name, value) -> insertMetricsTagsIndex.bind(metricId.getTenantId(), name, value,
                metricId.getType().getCode(), metricId.getName()));
    }

    @Override
    public <T> Observable<ResultSet> deleteFromMetricsTagsIndex(MetricId<T> id, Map<String, String> tags) {
        return tagsUpdates(tags, (name, value) -> deleteMetricsTagsIndex.bind(id.getTenantId(), name, value,
                id.getType().getCode(), id.getName()));
    }

    private Observable<ResultSet> tagsUpdates(Map<String, String> tags,
                                              BiFunction<String, String, BoundStatement> bindVars) {
        return Observable.from(tags.entrySet())
                .map(entry -> bindVars.apply(entry.getKey(), entry.getValue()))
                .flatMap(rxSession::execute);
    }

    @Override
    public Observable<Row> findMetricsByTagName(String tenantId, String tag) {
        return rxSession.executeAndFetch(findMetricsByTagName.bind(tenantId, tag));
    }

    @Override
    public Observable<Row> findMetricsByTagNameValue(String tenantId, String tag, String ... tvalues) {
        return rxSession.executeAndFetch(findMetricsByTagNameValue.bind(tenantId, tag, Arrays.asList(tvalues)));
    }

    @Override
    public <T> ResultSetFuture updateRetentionsIndex(Metric<T> metric) {
        return session.executeAsync(updateRetentionsIndex.bind(metric.getMetricId().getTenantId(),
                metric.getMetricId().getType().getCode(), metric.getMetricId().getName(), metric.getDataRetention()));
    }

    @Override
    public <T> Observable<ResultSet> insertCompressedData(MetricId<T> id, long timeslice,
                                                          CompressedPointContainer cpc, int ttl) {
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

        return rxSession.execute(b);
    }

    @Override
    public <T> Observable<ResultSet> deleteAndInsertCompressedGauge(MetricId<T> id, long timeslice,
                                                                    CompressedPointContainer cpc,
                                                                    long sliceStart, long sliceEnd, int ttl) {
        return Observable.just(deleteMetricDataWithLimit.bind()
                .setString(0, id.getTenantId())
                .setByte(1, id.getType().getCode())
                .setString(2, id.getName())
                .setLong(3, DPART)
                .setUUID(4, getTimeUUID(sliceStart))
                .setUUID(5, getTimeUUID(sliceEnd)))
                .concatMap(st -> rxSession.execute(st))
                .concatWith(insertCompressedData(id, timeslice, cpc, ttl));
    }

    @Override
    public Observable<Row> findAllMetricsFromTagsIndex() {
        return rxSession.executeAndFetch(findAllMetricsFromTagsIndex.bind());
    }

    private class TemporaryTableStatementCreator implements SchemaChangeListener {

        private final CoreLogger log = CoreLogging.getCoreLogger(TemporaryTableStatementCreator.class);

        @Override
        public void onTableAdded(TableMetadata tableMetadata) {
            log.debugf("Table added %s", tableMetadata.getName());
            if(tableMetadata.getName().startsWith(TEMP_TABLE_NAME_PROTOTYPE)) {
                log.debugf("Registering prepared statements for table %s", tableMetadata.getName());
                Observable.fromCallable(() -> {
                    prepareTempStatements(tableMetadata.getName(), tableToMapKey(tableMetadata.getName()));
                    return null;
                })
                        .subscribeOn(Schedulers.io())
                        .subscribe();
            }
        }

        @Override
        public void onTableRemoved(TableMetadata tableMetadata) {
            if(tableMetadata.getName().startsWith(TEMP_TABLE_NAME_PROTOTYPE)) {
                log.debugf("Removing prepared statements for table %s", tableMetadata.getName());
                removeTempStatements(tableMetadata.getName());
            }
        }

        // Rest are not interesting to us

        @Override public void onKeyspaceAdded(KeyspaceMetadata keyspaceMetadata) {}

        @Override public void onKeyspaceRemoved(KeyspaceMetadata keyspaceMetadata) {}

        @Override public void onKeyspaceChanged(KeyspaceMetadata keyspaceMetadata, KeyspaceMetadata keyspaceMetadata1) {}


        @Override public void onTableChanged(TableMetadata tableMetadata, TableMetadata tableMetadata1) {
        }

        @Override public void onUserTypeAdded(UserType userType) {}
        @Override public void onUserTypeRemoved(UserType userType) {}
        @Override public void onUserTypeChanged(UserType userType, UserType userType1) {}
        @Override public void onFunctionAdded(FunctionMetadata functionMetadata) {}
        @Override public void onFunctionRemoved(FunctionMetadata functionMetadata) {}
        @Override public void onFunctionChanged(FunctionMetadata functionMetadata, FunctionMetadata functionMetadata1) {}
        @Override public void onAggregateAdded(AggregateMetadata aggregateMetadata) {}
        @Override public void onAggregateRemoved(AggregateMetadata aggregateMetadata) {}
        @Override public void onAggregateChanged(AggregateMetadata aggregateMetadata, AggregateMetadata aggregateMetadata1) {}
        @Override public void onMaterializedViewAdded(MaterializedViewMetadata materializedViewMetadata) {}
        @Override public void onMaterializedViewRemoved(MaterializedViewMetadata materializedViewMetadata) {}
        @Override public void onMaterializedViewChanged(MaterializedViewMetadata materializedViewMetadata,
                                                        MaterializedViewMetadata materializedViewMetadata1) {}
        @Override public void onRegister(Cluster cluster) {}
        @Override public void onUnregister(Cluster cluster) {}
    }

    void removeTempStatements(String tableName) {
        // Find the integer key and remove from prepMap
        Long mapKey = tableToMapKey(tableName);
        prepMap.remove(mapKey);
    }

    @Override public void shutdown() {
        session.getCluster().unregister(tableCreator);
        tableCreator = null;
    }

    public void setTempTableCreator(TemporaryTableStatementCreator creator) {
        if(tableCreator != null) {
            session.getCluster().unregister(tableCreator);
        }
        tableCreator = creator;
        session.getCluster().register(tableCreator);
    }
}
