package org.rhq.metrics.clients.syslogRest;

/**
 * A syslog metric message.
 * In the form "type=metric thread.count=5 thread.active=2 heap.permgen.size=25000000"
 * @author Heiko W. Rupp
 */
public class SyslogMetricEvent {

    String jsonTemplate = "{\"id\":\"%1\"," +
     "\"timestamp\":%2,"+
     "\"value\":%3}";


    private long timestamp;
    private String source;
    private Double value;

    public SyslogMetricEvent() {
    }

    public SyslogMetricEvent(String source, long timestamp, Double value) {
        this.timestamp = timestamp;
        this.source = source;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getSource() {
        return source;
    }

    public Double getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "SyslogMetricEvent{" +
            "timestamp=" + timestamp +
            ", source='" + source + '\'' +
            ", value=" + value +
            '}';
    }

    public String toJson() {
        String payload = jsonTemplate.replaceAll("%1",source)
            .replaceAll("%2", String.valueOf(timestamp))
            .replaceAll("%3", String.valueOf(value));

        return payload;

    }
}
