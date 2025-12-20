package org.myorg.quickstart.job;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.BeforeEach;
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

    @BeforeEach
    void setUp() {
        CollectSink.clearAll();
    }

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

        // Create test source
        MessageEventSource testSource = env -> env.fromCollection(sourceData);

        // Create test sinks
        MessageEventSink profanitySink = stream -> stream.addSink(new CollectSink(CollectSink.Target.PROFANITY));
        MessageEventSink icebergSink = stream -> stream.addSink(new CollectSink(CollectSink.Target.ICEBERG));

        // Execute job
        DataStreamJob job = new DataStreamJob(testSource, profanitySink, icebergSink);
        job.execute();

        List<MessageEvent> profanityResults = CollectSink.getProfanityValues();
        List<MessageEvent> icebergResults = CollectSink.getIcebergValues();

        // Verify profanity sink received profane messages
        assertEquals(1, profanityResults.size());
        assertEquals("2", profanityResults.get(0).getMessageId());
        assertEquals(MessageEvent.ProfanityType.PROFANITY, profanityResults.get(0).getProfanityType());

        // Verify iceberg sink received all processed messages
        assertEquals(2, icebergResults.size());
    }

    @Test
    void testJobWithEmptySource() throws Exception {
        MessageEventSource testSource = env -> env.fromCollection(
            Collections.emptyList(),
            TypeInformation.of(MessageEvent.class)
        );
        MessageEventSink profanitySink = stream -> stream.addSink(new CollectSink(CollectSink.Target.PROFANITY));
        MessageEventSink icebergSink = stream -> stream.addSink(new CollectSink(CollectSink.Target.ICEBERG));

        DataStreamJob job = new DataStreamJob(testSource, profanitySink, icebergSink);
        job.execute();

        List<MessageEvent> profanityResults = CollectSink.getProfanityValues();
        List<MessageEvent> icebergResults = CollectSink.getIcebergValues();

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

        MessageEventSource testSource = env -> env.fromCollection(sourceData);
        MessageEventSink profanitySink = stream -> stream.addSink(new CollectSink(CollectSink.Target.PROFANITY));
        MessageEventSink icebergSink = stream -> stream.addSink(new CollectSink(CollectSink.Target.ICEBERG));

        DataStreamJob job = new DataStreamJob(testSource, profanitySink, icebergSink);
        job.execute();

        List<MessageEvent> profanityResults = CollectSink.getProfanityValues();
        List<MessageEvent> icebergResults = CollectSink.getIcebergValues();

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

        MessageEventSource testSource = env -> env.fromCollection(sourceData);
        MessageEventSink profanitySink = stream -> stream.addSink(new CollectSink(CollectSink.Target.PROFANITY));
        MessageEventSink icebergSink = stream -> stream.addSink(new CollectSink(CollectSink.Target.ICEBERG));

        DataStreamJob job = new DataStreamJob(testSource, profanitySink, icebergSink);
        job.execute();

        List<MessageEvent> profanityResults = CollectSink.getProfanityValues();
        List<MessageEvent> icebergResults = CollectSink.getIcebergValues();

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

        MessageEventSource testSource = env -> env.fromCollection(sourceData);
        MessageEventSink profanitySink = stream -> stream.addSink(new CollectSink(CollectSink.Target.PROFANITY));
        MessageEventSink icebergSink = stream -> stream.addSink(new CollectSink(CollectSink.Target.ICEBERG));

        DataStreamJob job = new DataStreamJob(testSource, profanitySink, icebergSink);
        job.execute();

        List<MessageEvent> profanityResults = CollectSink.getProfanityValues();
        List<MessageEvent> icebergResults = CollectSink.getIcebergValues();

        // Verify profanity detection worked
        assertEquals(5, profanityResults.size());
        assertTrue(profanityResults.stream().allMatch(m -> m.getProfanityType() == MessageEvent.ProfanityType.PROFANITY));

        // Verify all messages sent to iceberg
        assertEquals(10, icebergResults.size());
    }

    /**
     * Helper sink that collects results into a list.
     */
    private static class CollectSink implements SinkFunction<MessageEvent> {
        enum Target {
            PROFANITY,
            ICEBERG
        }

        private static final List<MessageEvent> PROFANITY_VALUES = Collections.synchronizedList(new ArrayList<>());
        private static final List<MessageEvent> ICEBERG_VALUES = Collections.synchronizedList(new ArrayList<>());

        private final Target target;

        private CollectSink(Target target) {
            this.target = target;
        }

        static void clearAll() {
            PROFANITY_VALUES.clear();
            ICEBERG_VALUES.clear();
        }

        static List<MessageEvent> getProfanityValues() {
            return new ArrayList<>(PROFANITY_VALUES);
        }

        static List<MessageEvent> getIcebergValues() {
            return new ArrayList<>(ICEBERG_VALUES);
        }

        @Override
        public void invoke(MessageEvent value, Context context) {
            if (target == Target.PROFANITY) {
                PROFANITY_VALUES.add(value);
            } else {
                ICEBERG_VALUES.add(value);
            }
        }
    }
}
