package org.hawkular.metrics.api.jaxrs.jackson;

import java.io.IOException;

import org.hawkular.metrics.core.api.MetricType;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Created by miburman on 8/26/15.
 */
public class MetricTypeDeserializer extends JsonDeserializer<MetricType<?>> {

    @Override public MetricType<?> deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException, JsonProcessingException {
        JsonNode jsonNode = jsonParser.getCodec().readTree(jsonParser);
        return MetricType.fromTextCode(jsonNode.get("type").asText());
    }
}
