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
package org.hawkular.metrics.core.service.transformers;

import static org.hawkular.metrics.core.service.Order.ASC;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hawkular.metrics.core.service.Order;
import org.hawkular.metrics.core.service.compress.CompressorHeader;
import org.hawkular.metrics.core.service.compress.TagsDeserializer;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.MetricType;

import com.datastax.driver.core.Row;

import fi.iki.yak.ts.compression.gorilla.BitInput;
import fi.iki.yak.ts.compression.gorilla.ByteBufferBitInput;
import fi.iki.yak.ts.compression.gorilla.Decompressor;
import fi.iki.yak.ts.compression.gorilla.GorillaDecompressor;
import fi.iki.yak.ts.compression.gorilla.Pair;
import rx.Observable;

/**
 * Transforms input rows from compressed format back to DataPoints.
 *
 * @author Michael Burman
 */
public class DataPointDecompressTransformer<T> implements Observable.Transformer<Row, DataPoint<T>> {

    private Order order;
    private int limit;
    private long start;
    private long end;
    private MetricType<T> metricType;

    public DataPointDecompressTransformer(MetricType<T> metricType, Order order, int limit, long start, long end) {
        this.order = order;
        this.limit = limit;
        this.start = start;
        this.end = end;
        this.metricType = metricType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Observable<DataPoint<T>> call(Observable<Row> rows) {

        Observable<DataPoint<T>> datapoints =
                rows.flatMap(r -> {
                    Stream.Builder<DataPoint<T>> dataPointStreamBuilder = Stream.builder();

                    ByteBuffer tagsBuffer = r.getBytes("tags");
                    ByteBuffer compressedValue = r.getBytes("c_value");

                    if (compressedValue != null) {
                        byte header = compressedValue.get();
                        CompressorHeader.Compressor compressorType = CompressorHeader.getCompressor(header);
                        EnumSet settings = CompressorHeader.getSettings(compressorType.getSettingsClass(), header);
                        BitInput in = new ByteBufferBitInput(compressedValue);

                        Map<Long, Map<String, String>> tagMap = null;
                        if(tagsBuffer != null) {
                            long blockStart = r.getTimestamp("time").toInstant().toEpochMilli();
                            TagsDeserializer deserializer = new TagsDeserializer(blockStart);
                            tagMap = deserializer.deserialize(tagsBuffer);
                        }

                        switch(compressorType) {
                            case GORILLA:
                                decompressGorilla(dataPointStreamBuilder, in, tagMap);
                                break;
                            case GORILLA_V2:
                                decompressGorillaV2(dataPointStreamBuilder, in, tagMap, settings);
                                break;
                            default:
                                throw new RuntimeException("Unsupported compressor type " + compressorType);
                        }

                    }
                    return Observable.from(dataPointStreamBuilder.build()
                            .sorted((d1, d2) -> {
                                if (order == ASC) {
                                    return (d1.getTimestamp() > d2.getTimestamp()) ? 1 : -1;
                                }
                                return (d1.getTimestamp() < d2.getTimestamp()) ? 1 : -1;
                            })
                            .collect(Collectors.toList()));
                });
        if(limit > 0) {
            datapoints = datapoints.take(limit);
        }

        return datapoints;
    }

    @SuppressWarnings("unchecked")
    private void decompressGorilla(Stream.Builder<DataPoint<T>> dataPointStreamBuilder, BitInput in,
                                   Map<Long, Map<String, String>> tagMap) {
        Decompressor d = new Decompressor(in);
        Pair pair;
        while ((pair = d.readPair()) != null) {
            if (pair.getTimestamp() >= start && pair.getTimestamp() < end) {
                DataPoint<T> dataPoint;

                switch(metricType.getCode()) {
                    case 0: // GAUGE
                        dataPoint = new DataPoint(pair.getTimestamp(), pair.getDoubleValue());
                        break;
                    case 1: // AVAILABILITY
                        dataPoint = new DataPoint(pair.getTimestamp(), AvailabilityType.fromByte(
                                ((Double) pair.getDoubleValue()).byteValue()));
                        break;
                    case 2: // COUNTER
                        dataPoint = new DataPoint(pair.getTimestamp(), ((Double) pair.getDoubleValue
                                ()).longValue());
                        break;
                    default:
                        // Not supported yet
                        throw new RuntimeException(
                                "Metric of type " + metricType.getText() + " is not supported " +
                                        "in decompression");
                }

                // Add tags from the serialized tags
                if(tagMap != null) {
                    Long key = pair.getTimestamp();

                    if (tagMap.containsKey(key)) {
                        Map<String, String> dpTags = tagMap.get(key);
                        dataPoint = new DataPoint(dataPoint.getTimestamp(), dataPoint
                                .getValue(), dpTags);
                    }
                }

                dataPointStreamBuilder.add(dataPoint);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void decompressGorillaV2(Stream.Builder<DataPoint<T>> dataPointStreamBuilder, BitInput in,
                                     Map<Long, Map<String, String>> tagMap,
                                     EnumSet<CompressorHeader.GorillaSettings> settings) {
        GorillaDecompressor d = new GorillaDecompressor(in);
        Pair pair;
        while ((pair = d.readPair()) != null) {
            if (pair.getTimestamp() >= start && pair.getTimestamp() < end) {
                DataPoint<T> dataPoint;

                switch(metricType.getCode()) {
                    case 0: // GAUGE
                        dataPoint = new DataPoint(pair.getTimestamp(), pair.getDoubleValue());
                        break;
                    case 1: // AVAILABILITY
                        dataPoint = new DataPoint(pair.getTimestamp(), AvailabilityType.fromByte(
                                Long.valueOf(pair.getLongValue()).byteValue()));
                        break;
                    case 2: // COUNTER
                        dataPoint = new DataPoint(pair.getTimestamp(), pair.getLongValue());
                        break;
                    default:
                        // Not supported yet
                        throw new RuntimeException(
                                "Metric of type " + metricType.getText() + " is not supported " +
                                        "in " + CompressorHeader.Compressor.GORILLA_V2.name() + " decompression");
                }

                // Add tags from the serialized tags
                if(tagMap != null) {
                    Long key = pair.getTimestamp();

                    if (tagMap.containsKey(key)) {
                        Map<String, String> dpTags = tagMap.get(key);
                        dataPoint = new DataPoint(dataPoint.getTimestamp(), dataPoint.getValue(), dpTags);
                    }
                }

                dataPointStreamBuilder.add(dataPoint);
            }
        }
    }
}
