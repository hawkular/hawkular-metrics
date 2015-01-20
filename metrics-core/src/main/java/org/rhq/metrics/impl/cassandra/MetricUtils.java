package org.rhq.metrics.impl.cassandra;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.stream.Collectors.toMap;

import java.util.Map;
import java.util.Optional;

/**
 * @author John Sanda
 */
public class MetricUtils {

    public static Map<String, Optional<String>> getTags(Map<String, String> map) {
        return map.entrySet()
            .stream()
            .collect(toMap(Map.Entry::getKey, e -> e.getValue().isEmpty() ? empty() : of(e.getValue())));
    }

    public static Map<String, String> flattenTags(Map<String, Optional<String>> tags) {
        return tags.entrySet()
            .stream()
            .collect(toMap(Map.Entry::getKey, e -> e.getValue().orElse("")));
    }

}
