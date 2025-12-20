package org.myorg.quickstart.job;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.myorg.quickstart.model.MessageEvent;

/**
 * Interface for consuming MessageEvent streams.
 * Implementations can provide different sinks (Kafka, Iceberg, test collectors, etc.)
 */
public interface MessageEventSink {
    /**
     * Adds a sink to the provided DataStream.
     *
     * @param stream the DataStream to add the sink to
     */
    void addSink(DataStream<MessageEvent> stream);
}
