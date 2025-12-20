package org.myorg.quickstart.job;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.myorg.quickstart.model.MessageEvent;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for IcebergMessageEventSink.
 */
class IcebergMessageEventSinkTest {

    @Test
    void testAddSinkDoesNotThrow() {
        // Create a minimal test environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MessageEvent testMessage = new MessageEvent();
        testMessage.setMessageId("1");
        testMessage.setMessageBody("test");
        testMessage.setTimestamp("2024-01-01T10:00:00Z");

        DataStream<MessageEvent> stream = env.fromCollection(Collections.singletonList(testMessage));

        IcebergMessageEventSink sink = new IcebergMessageEventSink();

        // Should not throw exception during sink configuration
        // Note: We can't execute the full job without real Iceberg/Polaris infrastructure,
        // but we can verify the sink configuration code doesn't crash
        assertDoesNotThrow(() -> sink.addSink(stream));
    }

    @Test
    void testSinkCreation() {
        // Verify sink can be instantiated
        assertDoesNotThrow(() -> new IcebergMessageEventSink());
    }

    @Test
    void testSinkWithEmptyStream() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<MessageEvent> emptyStream = env.fromCollection(Collections.emptyList());

        IcebergMessageEventSink sink = new IcebergMessageEventSink();

        // Should handle empty streams gracefully
        assertDoesNotThrow(() -> sink.addSink(emptyStream));
    }

    @Test
    void testSinkWithMultipleMessages() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MessageEvent msg1 = new MessageEvent();
        msg1.setMessageId("1");
        msg1.setMessageBody("test1");
        msg1.setTimestamp("2024-01-01T10:00:00Z");

        MessageEvent msg2 = new MessageEvent();
        msg2.setMessageId("2");
        msg2.setMessageBody("test2");
        msg2.setTimestamp("2024-01-01T10:00:01Z");

        DataStream<MessageEvent> stream = env.fromCollection(java.util.List.of(msg1, msg2));

        IcebergMessageEventSink sink = new IcebergMessageEventSink();

        // Should handle multiple messages
        assertDoesNotThrow(() -> sink.addSink(stream));
    }
}
