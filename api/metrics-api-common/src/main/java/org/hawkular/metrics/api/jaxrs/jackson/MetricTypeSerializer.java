package org.hawkular.metrics.api.jaxrs.jackson;

import java.io.IOException;

import org.hawkular.metrics.core.api.MetricType;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * Created by miburman on 8/26/15.
 */
public class MetricTypeSerializer extends JsonSerializer<MetricType<?>> {

    @Override
    public void serialize(MetricType<?> metricType, JsonGenerator jsonGenerator,
                          SerializerProvider serializerProvider)
            throws IOException, JsonProcessingException {
        jsonGenerator.writeString(metricType.getText());
    }
}
