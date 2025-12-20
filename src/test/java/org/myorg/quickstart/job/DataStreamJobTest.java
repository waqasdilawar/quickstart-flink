package org.myorg.quickstart.job;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.myorg.quickstart.model.MessageEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for DataStreamJob with dependency injection.
 */
class DataStreamJobTest {

    @RegisterExtension
    static final MiniClusterExtension FLINK_CLUSTER = new MiniClusterExtension(
        new MiniClusterResourceConfiguration.Builder()
            .setNumberSlotsPerTaskManager(2)
            .setNumberTaskManagers(1)
            .build()
    );

    @Test
    void testJobExecutionWithTestSourcesAndSinks() throws Exception {
        // Prepare test data
        MessageEvent cleanMessage = new MessageEvent();
        cleanMessage.setMessageId("1");
        cleanMessage.setMessageBody("Hello world");
        cleanMessage.setTimestamp("2024-01-01T10:00:00Z");
        cleanMessage.setProfanityType(MessageEvent.ProfanityType.SAFE);

        MessageEvent profaneMessage = new MessageEvent();
        profaneMessage.setMessageId("2");
        profaneMessage.setMessageBody("Bad word gun here");
        profaneMessage.setTimestamp("2024-01-01T10:00:01Z");
        profaneMessage.setProfanityType(MessageEvent.ProfanityType.PROFANITY);

        List<MessageEvent> sourceData = List.of(cleanMessage, profaneMessage);
        List<MessageEvent> profanityResults = Collections.synchronizedList(new ArrayList<>());
        List<MessageEvent> icebergResults = Collections.synchronizedList(new ArrayList<>());

        // Create test source
        MessageEventSource testSource = env -> env.fromCollection(sourceData);

        // Create test sinks
        MessageEventSink profanitySink = stream -> stream.addSink(new CollectSink<>(profanityResults));
        MessageEventSink icebergSink = stream -> stream.addSink(new CollectSink<>(icebergResults));

        // Execute job
        DataStreamJob job = new DataStreamJob(testSource, profanitySink, icebergSink);
        job.execute();

        // Verify profanity sink received profane messages
        assertEquals(1, profanityResults.size());
        assertEquals("2", profanityResults.get(0).getMessageId());
        assertEquals(MessageEvent.ProfanityType.PROFANITY, profanityResults.get(0).getProfanityType());

        // Verify iceberg sink received all processed messages
        assertEquals(2, icebergResults.size());
    }

    @Test
    void testJobWithEmptySource() throws Exception {
        List<MessageEvent> profanityResults = Collections.synchronizedList(new ArrayList<>());
        List<MessageEvent> icebergResults = Collections.synchronizedList(new ArrayList<>());

        MessageEventSource testSource = env -> env.fromCollection(Collections.emptyList());
        MessageEventSink profanitySink = stream -> stream.addSink(new CollectSink<>(profanityResults));
        MessageEventSink icebergSink = stream -> stream.addSink(new CollectSink<>(icebergResults));

        DataStreamJob job = new DataStreamJob(testSource, profanitySink, icebergSink);
        job.execute();

        assertEquals(0, profanityResults.size());
        assertEquals(0, icebergResults.size());
    }

    @Test
    void testJobWithOnlySafeMessages() throws Exception {
        MessageEvent safeMessage1 = new MessageEvent();
        safeMessage1.setMessageId("1");
        safeMessage1.setMessageBody("Hello world");
        safeMessage1.setTimestamp("2024-01-01T10:00:00Z");

        MessageEvent safeMessage2 = new MessageEvent();
        safeMessage2.setMessageId("2");
        safeMessage2.setMessageBody("Nice day");
        safeMessage2.setTimestamp("2024-01-01T10:00:01Z");

        List<MessageEvent> sourceData = List.of(safeMessage1, safeMessage2);
        List<MessageEvent> profanityResults = Collections.synchronizedList(new ArrayList<>());
        List<MessageEvent> icebergResults = Collections.synchronizedList(new ArrayList<>());

        MessageEventSource testSource = env -> env.fromCollection(sourceData);
        MessageEventSink profanitySink = stream -> stream.addSink(new CollectSink<>(profanityResults));
        MessageEventSink icebergSink = stream -> stream.addSink(new CollectSink<>(icebergResults));

        DataStreamJob job = new DataStreamJob(testSource, profanitySink, icebergSink);
        job.execute();

        // No profane messages should be sent to profanity sink
        assertEquals(0, profanityResults.size());

        // All messages should be sent to iceberg sink
        assertEquals(2, icebergResults.size());
        assertTrue(icebergResults.stream().allMatch(m -> m.getProfanityType() == MessageEvent.ProfanityType.SAFE));
    }

    @Test
    void testJobWithOnlyProfaneMessages() throws Exception {
        MessageEvent profaneMessage1 = new MessageEvent();
        profaneMessage1.setMessageId("1");
        profaneMessage1.setMessageBody("gun");
        profaneMessage1.setTimestamp("2024-01-01T10:00:00Z");

        MessageEvent profaneMessage2 = new MessageEvent();
        profaneMessage2.setMessageId("2");
        profaneMessage2.setMessageBody("another gun message");
        profaneMessage2.setTimestamp("2024-01-01T10:00:01Z");

        List<MessageEvent> sourceData = List.of(profaneMessage1, profaneMessage2);
        List<MessageEvent> profanityResults = Collections.synchronizedList(new ArrayList<>());
        List<MessageEvent> icebergResults = Collections.synchronizedList(new ArrayList<>());

        MessageEventSource testSource = env -> env.fromCollection(sourceData);
        MessageEventSink profanitySink = stream -> stream.addSink(new CollectSink<>(profanityResults));
        MessageEventSink icebergSink = stream -> stream.addSink(new CollectSink<>(icebergResults));

        DataStreamJob job = new DataStreamJob(testSource, profanitySink, icebergSink);
        job.execute();

        // All messages should be sent to profanity sink
        assertEquals(2, profanityResults.size());
        assertTrue(profanityResults.stream().allMatch(m -> m.getProfanityType() == MessageEvent.ProfanityType.PROFANITY));

        // All messages should also be sent to iceberg sink
        assertEquals(2, icebergResults.size());
    }

    @Test
    void testLoadProfanities() {
        var profanities = DataStreamJob.loadProfanities();
        assertNotNull(profanities);
        assertFalse(profanities.isEmpty());
        assertTrue(profanities.contains("gun"));
    }

    @Test
    void testDefaultConstructor() {
        // Should not throw exception
        assertDoesNotThrow(() -> new DataStreamJob());
    }

    @Test
    void testLoadProfanitiesReturnsNonEmptySet() {
        var profanities = DataStreamJob.loadProfanities();
        assertNotNull(profanities);
        assertFalse(profanities.isEmpty());
        assertTrue(profanities.contains("gun"));
    }

    @Test
    void testJobWithMultipleProfaneAndSafeMessages() throws Exception {
        List<MessageEvent> sourceData = new ArrayList<>();

        // Add multiple profane messages
        for (int i = 0; i < 5; i++) {
            MessageEvent profaneMsg = new MessageEvent();
            profaneMsg.setMessageId("profane-" + i);
            profaneMsg.setMessageBody("This contains gun");
            profaneMsg.setTimestamp("2024-01-01T10:00:00Z");
            sourceData.add(profaneMsg);
        }

        // Add multiple safe messages
        for (int i = 0; i < 5; i++) {
            MessageEvent safeMsg = new MessageEvent();
            safeMsg.setMessageId("safe-" + i);
            safeMsg.setMessageBody("This is safe");
            safeMsg.setTimestamp("2024-01-01T10:00:00Z");
            sourceData.add(safeMsg);
        }

        List<MessageEvent> profanityResults = Collections.synchronizedList(new ArrayList<>());
        List<MessageEvent> icebergResults = Collections.synchronizedList(new ArrayList<>());

        MessageEventSource testSource = env -> env.fromCollection(sourceData);
        MessageEventSink profanitySink = stream -> stream.addSink(new CollectSink<>(profanityResults));
        MessageEventSink icebergSink = stream -> stream.addSink(new CollectSink<>(icebergResults));

        DataStreamJob job = new DataStreamJob(testSource, profanitySink, icebergSink);
        job.execute();

        // Verify profanity detection worked
        assertEquals(5, profanityResults.size());
        assertTrue(profanityResults.stream().allMatch(m -> m.getProfanityType() == MessageEvent.ProfanityType.PROFANITY));

        // Verify all messages sent to iceberg
        assertEquals(10, icebergResults.size());
    }

    /**
     * Helper sink that collects results into a list.
     */
    private static class CollectSink<T> implements SinkFunction<T> {
        private final List<T> results;

        public CollectSink(List<T> results) {
            this.results = results;
        }

        @Override
        public void invoke(T value, Context context) {
            results.add(value);
        }
    }
}
