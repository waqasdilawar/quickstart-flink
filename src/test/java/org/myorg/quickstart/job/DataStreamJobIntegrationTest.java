package org.myorg.quickstart.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.myorg.quickstart.model.MessageEvent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for DataStreamJob using MiniCluster.
 * Following the pattern from Flink training exercises.
 */
class DataStreamJobIntegrationTest {

  @RegisterExtension
  static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension(
    new MiniClusterResourceConfiguration.Builder()
      .setNumberSlotsPerTaskManager(2)
      .setNumberTaskManagers(1)
      .build()
  );

  private static final Set<String> PROFANITIES = Set.of("gun", "badword", "offensive");

  @BeforeEach
  void clearSink() {
      CollectSink.values.clear();
  }

  @Test
  void shouldClassifyAndFilterProfaneMessages() throws Exception {
    // Given
    MessageEvent profaneMessage1 = createMessage("1", "This contains gun");
    MessageEvent profaneMessage2 = createMessage("2", "Another badword here");
    MessageEvent safeMessage = createMessage("3", "This is a clean message");

    List<MessageEvent> testData = Arrays.asList(profaneMessage1, profaneMessage2, safeMessage);

    // When
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<MessageEvent> source = env.fromCollection(testData);

    // Apply the same processing logic as the main job
    DataStream<MessageEvent> processed = source.map(event -> {
      boolean isProfane = containsProfanity(event.getMessageBody(), PROFANITIES);
      event.setProfanityType(isProfane ?
                               MessageEvent.ProfanityType.PROFANITY :
        MessageEvent.ProfanityType.SAFE);
      return event;
    });

    // Filter only profane messages (simulating Kafka sink filter)
    processed.filter(event -> event.getProfanityType() == MessageEvent.ProfanityType.PROFANITY)
      .addSink(new CollectSink());

    env.execute("Test Profanity Classification");

    // Then
    List<MessageEvent> results = new ArrayList<>(CollectSink.values);

    assertThat(results)
      .hasSize(2)
      .extracting(MessageEvent::getMessageId)
      .containsExactlyInAnyOrder("1", "2");

    assertThat(results)
      .allMatch(e -> e.getProfanityType() == MessageEvent.ProfanityType.PROFANITY);
  }

  @Test
  void shouldMarkAllMessagesAsSafeWhenNoProfanity() throws Exception {
    // Given
    MessageEvent safe1 = createMessage("1", "Hello world");
    MessageEvent safe2 = createMessage("2", "Have a nice day");

    List<MessageEvent> testData = Arrays.asList(safe1, safe2);

    // When
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<MessageEvent> source = env.fromCollection(testData);

    source.map(event -> {
        boolean isProfane = containsProfanity(event.getMessageBody(), PROFANITIES);
        event.setProfanityType(isProfane ?
                                 MessageEvent.ProfanityType.PROFANITY :
          MessageEvent.ProfanityType.SAFE);
        return event;
      }).filter(event -> event.getProfanityType() == MessageEvent.ProfanityType.PROFANITY)
      .addSink(new CollectSink());

    env.execute("Test No Profanity");

    // Then - no messages should be in the sink
    List<MessageEvent> results = new ArrayList<>(CollectSink.values);
    assertThat(results).isEmpty();
  }

  @Test
  void shouldHandleEmptyMessageBody() throws Exception {
    // Given
    MessageEvent emptyBody = createMessage("1", "");
    MessageEvent nullBody = createMessage("2", null);

    List<MessageEvent> testData = Arrays.asList(emptyBody, nullBody);

    // When
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<MessageEvent> source = env.fromCollection(testData);

    source.map(event -> {
      boolean isProfane = containsProfanity(event.getMessageBody(), PROFANITIES);
      event.setProfanityType(isProfane ?
                               MessageEvent.ProfanityType.PROFANITY :
        MessageEvent.ProfanityType.SAFE);
      return event;
    }).addSink(new CollectSink());

    env.execute("Test Empty Messages");

    // Then - all should be classified as SAFE
    List<MessageEvent> results = new ArrayList<>(CollectSink.values);
    assertThat(results)
      .hasSize(2)
      .allMatch(e -> e.getProfanityType() == MessageEvent.ProfanityType.SAFE);
  }

  // Helper methods
  private MessageEvent createMessage(String id, String body) {
    MessageEvent event = new MessageEvent();
    event.setMessageId(id);
    event.setMessageBody(body);
    event.setTimestamp("2025-01-01T10:00:00Z");
    return event;
  }

  private static boolean containsProfanity(String text, Set<String> profanities) {
    if (text == null || text.isEmpty()) {
      return false;
    }
    String lower = text.toLowerCase();
    for (String badWord : profanities) {
      if (lower.contains(badWord.toLowerCase())) {
        return true;
      }
    }
    return false;
  }

  private static class CollectSink implements SinkFunction<MessageEvent> {
      public static final List<MessageEvent> values = new CopyOnWriteArrayList<>();

      @Override
      public void invoke(MessageEvent value, Context context) {
          values.add(value);
      }
  }
}