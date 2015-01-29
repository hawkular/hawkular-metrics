package org.rhq.metrics.impl.cassandra;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.stream.Collectors.toMap;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

/**
 * @author John Sanda
 */
public class MetricUtils {

    public static Map<String, Optional<String>> getTags(Map<String, String> map) {
        return map.entrySet()
            .stream()
            .collect(toMap(Map.Entry::getKey, e -> getOptional(e.getValue())));
    }

    public static Map<String, String> flattenTags(Map<String, Optional<String>> tags) {
        return tags.entrySet()
            .stream()
            .collect(toMap(Map.Entry::getKey, e -> e.getValue().orElse("")));
    }

    public static Map<String, Optional<String>> decodeTags(String tags) {
        return Arrays.stream(tags.split(","))
            .map(s -> s.split(":"))
            .collect(toMap(a -> a[0], a -> getOptional(a[1])));
    }

    private static Optional<String> getOptional(String s) {
        return s == null || s.isEmpty() ? empty() : of(s);
    }

}
