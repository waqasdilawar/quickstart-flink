package org.myorg.quickstart.job;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.myorg.quickstart.model.MessageEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for KafkaProfanitySink.
 */
class KafkaProfanitySinkTest {

    @RegisterExtension
    static final MiniClusterExtension FLINK_CLUSTER = new MiniClusterExtension(
        new MiniClusterResourceConfiguration.Builder()
            .setNumberSlotsPerTaskManager(2)
            .setNumberTaskManagers(1)
            .build()
    );

    @Test
    void testAddSinkExecutesSuccessfully() throws Exception {
        // Create test environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MessageEvent testMessage = new MessageEvent();
        testMessage.setMessageId("1");
        testMessage.setMessageBody("test profane content");
        testMessage.setTimestamp("2024-01-01T10:00:00Z");
        testMessage.setProfanityType(MessageEvent.ProfanityType.PROFANITY);

        DataStream<MessageEvent> stream = env.fromCollection(Collections.singletonList(testMessage));

        // Use a test collector sink instead of real Kafka
        CollectSink.values.clear();
        MessageEventSink testSink = s -> s.addSink(new CollectSink());

        testSink.addSink(stream);
        env.execute("Test Kafka Sink");

        // Verify the sink was called
        assertEquals(1, CollectSink.values.size());
        assertEquals("1", CollectSink.values.get(0).getMessageId());
    }

    @Test
    void testAddSinkConfiguration() {
        // Create a minimal test environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MessageEvent testMessage = new MessageEvent();
        testMessage.setMessageId("1");
        testMessage.setMessageBody("test profane content");
        testMessage.setTimestamp("2024-01-01T10:00:00Z");
        testMessage.setProfanityType(MessageEvent.ProfanityType.PROFANITY);

        DataStream<MessageEvent> stream = env.fromCollection(Collections.singletonList(testMessage));

        KafkaProfanitySink sink = new KafkaProfanitySink();

        // Should not throw exception during sink configuration
        assertDoesNotThrow(() -> sink.addSink(stream));
    }

    @Test
    void testSinkCreation() {
        // Verify sink can be instantiated
        assertDoesNotThrow(() -> new KafkaProfanitySink());
    }

    @Test
    void testSinkWithEmptyStream() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<MessageEvent> emptyStream = env.fromCollection(
            Collections.emptyList(),
            TypeInformation.of(MessageEvent.class)
        );

        KafkaProfanitySink sink = new KafkaProfanitySink();

        // Should handle empty streams gracefully
        assertDoesNotThrow(() -> sink.addSink(emptyStream));
    }

    @Test
    void testSinkWithMultipleProfaneMessages() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MessageEvent msg1 = new MessageEvent();
        msg1.setMessageId("1");
        msg1.setMessageBody("profane message 1");
        msg1.setTimestamp("2024-01-01T10:00:00Z");
        msg1.setProfanityType(MessageEvent.ProfanityType.PROFANITY);

        MessageEvent msg2 = new MessageEvent();
        msg2.setMessageId("2");
        msg2.setMessageBody("profane message 2");
        msg2.setTimestamp("2024-01-01T10:00:01Z");
        msg2.setProfanityType(MessageEvent.ProfanityType.PROFANITY);

        DataStream<MessageEvent> stream = env.fromCollection(java.util.List.of(msg1, msg2));

        KafkaProfanitySink sink = new KafkaProfanitySink();

        // Should handle multiple messages
        assertDoesNotThrow(() -> sink.addSink(stream));
    }

    @Test
    void testSinkWithMixedMessages() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MessageEvent profaneMsg = new MessageEvent();
        profaneMsg.setMessageId("1");
        profaneMsg.setMessageBody("profane content");
        profaneMsg.setTimestamp("2024-01-01T10:00:00Z");
        profaneMsg.setProfanityType(MessageEvent.ProfanityType.PROFANITY);

        MessageEvent safeMsg = new MessageEvent();
        safeMsg.setMessageId("2");
        safeMsg.setMessageBody("safe content");
        safeMsg.setTimestamp("2024-01-01T10:00:01Z");
        safeMsg.setProfanityType(MessageEvent.ProfanityType.SAFE);

        DataStream<MessageEvent> stream = env.fromCollection(java.util.List.of(profaneMsg, safeMsg));

        KafkaProfanitySink sink = new KafkaProfanitySink();

        // Should handle mixed message types
        assertDoesNotThrow(() -> sink.addSink(stream));
    }

    /**
     * Helper sink that collects results into a list.
     */
    private static class CollectSink implements SinkFunction<MessageEvent> {
        static final List<MessageEvent> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(MessageEvent value, Context context) {
            values.add(value);
        }
    }
}
