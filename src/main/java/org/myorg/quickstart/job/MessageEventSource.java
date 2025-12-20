package org.myorg.quickstart.job;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.quickstart.model.MessageEvent;

/**
 * Interface for providing a source of MessageEvent streams.
 * Implementations can provide different sources (Kafka, test data, etc.)
 */
public interface MessageEventSource {
    /**
     * Creates and returns a DataStream of MessageEvents.
     *
     * @param env the StreamExecutionEnvironment to create the source in
     * @return a DataStream of MessageEvents
     */
    DataStream<MessageEvent> getSource(StreamExecutionEnvironment env);
}
