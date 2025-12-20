// package org.myorg.quickstart.job;
//
// import org.apache.flink.api.common.eventtime.WatermarkStrategy;
// import org.apache.flink.api.common.typeinfo.Types;
// import org.apache.flink.configuration.Configuration;
// import org.apache.flink.connector.kafka.source.KafkaSource;
// import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
// import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
// import org.apache.flink.streaming.api.datastream.DataStream;
// import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// import org.apache.flink.table.data.RowData;
// import org.apache.flink.test.junit5.MiniClusterExtension;
// import org.apache.flink.types.Row;
// import org.apache.flink.util.CloseableIterator;
// import org.apache.kafka.clients.producer.KafkaProducer;
// import org.apache.kafka.clients.producer.ProducerRecord;
// import org.apache.kafka.common.serialization.StringSerializer;
// import org.junit.jupiter.api.*;
// import org.junit.jupiter.api.extension.RegisterExtension;
// import org.junit.jupiter.api.io.TempDir;
// import org.testcontainers.containers.KafkaContainer;
// import org.testcontainers.junit.jupiter.Container;
// import org.testcontainers.junit.jupiter.Testcontainers;
// import org.testcontainers.utility.DockerImageName;
//
// import java.io.File;
// import java.io.FileWriter;
// import java.nio.file.Path;
// import java.time.Duration;
// import java.util.ArrayList;
// import java.util.List;
// import java.util.Properties;
// import java.util.concurrent.CompletableFuture;
// import java.util.concurrent.TimeUnit;
//
// import static org.assertj.core.api.Assertions.assertThat;
// import static org.awaitility.Awaitility.await;
//
// /**
//  * Comprehensive Integration Test for DataStreamJob.
//  * Covers:
//  * 1. Kafka Source Integration (using TestContainers)
//  * 2. Profanity Filtering Logic
//  * 3. Watermark/Event-Time Processing
//  * 4. Verification of Output (Simulating Iceberg Sink)
//  */
// @Testcontainers
// class DataStreamJobTest {
//
//   private static final String INPUT_TOPIC = "user-events";
//
//   // 1. FLINK MINI CLUSTER (JUnit 5 Extension)
//   @RegisterExtension
//   static final MiniClusterExtension FLINK = new MiniClusterExtension(
//     new MiniClusterResourceConfiguration.Builder()
//       .setNumberSlotsPerTaskManager(2)
//       .setNumberTaskManagers(1)
//       .build()
//   );
//
//   // 2. KAFKA CONTAINER
//   @Container
//   static final KafkaContainer KAFKA = new KafkaContainer(
//     DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
//   );
//
//   // 3. TEMP DIRECTORY for Profanity Config
//   @TempDir
//   static Path tempDir;
//   static File profanityFile;
//
//   @BeforeAll
//   static void setupInfrastructure() throws Exception {
//     // Create a temporary profanity dictionary file
//     profanityFile = tempDir.resolve("profanities.txt").toFile();
//     try (FileWriter writer = new FileWriter(profanityFile)) {
//       writer.write("badword1\n");
//       writer.write("badword2\n");
//     }
//   }
//
//   @Test
//   @DisplayName("E2E: Should consume Kafka, filter profanity, and output clean rows")
//   void testEndToEndPipeline() throws Exception {
//     // --- GIVEN: Test Data in Kafka ---
//     List<String> inputEvents = List.of(
//       "{\"userId\": 1, \"comment\": \"Hello World\", \"timestamp\": 1000}",
//       "{\"userId\": 2, \"comment\": \"This is a badword1\", \"timestamp\": 2000}", // Should be filtered/masked
//       "{\"userId\": 3, \"comment\": \"Nice Day\", \"timestamp\": 3000}"
//     );
//     produceToKafka(inputEvents);
//
//     // --- WHEN: The Job Runs ---
//     StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//     env.setParallelism(2);
//
//     // Use TestContainer bootstrap server
//     String bootstrapServers = KAFKA.getBootstrapServers();
//
//     // MOCK SOURCE: If your job hardcodes the source, you might need to swap it here.
//     // Assuming we pass the source to the job or the job configures it via properties:
//     KafkaSource<String> testSource = KafkaSource.<String>builder()
//       .setBootstrapServers(bootstrapServers)
//       .setTopics(INPUT_TOPIC)
//       .setGroupId("test-group")
//       .setStartingOffsets(OffsetsInitializer.earliest())
//       .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
//       .build();
//
//     // INJECT SOURCE INTO JOB
//     // NOTE: This assumes you refactored DataStreamJob to accept a source!
//     // If not, replace this with the actual logic from your job's buildTopology method
//     DataStream<String> sourceStream = env.fromSource(
//       testSource,
//       WatermarkStrategy.noWatermarks(),
//       "Kafka Source"
//     );
//
//     // CALL YOUR JOB LOGIC
//     // We pass the path to the temp profanity file we created
//     DataStream<RowData> resultStream = DataStreamJob.buildTopology(
//       env,
//       sourceStream,
//       profanityFile.getAbsolutePath()
//     );
//
//     // --- THEN: Collect and Assert ---
//     // We use executeAndCollect() to run the job and fetch results dynamically
//     // This avoids needing a real Iceberg sink for the logic test
//     try (CloseableIterator<RowData> iterator = resultStream.executeAndCollect()) {
//       List<RowData> results = new ArrayList<>();
//
//       // We expect 3 records (or 2 if filtering drops them completely)
//       // Let's assume logic masks bad words: "This is a *****"
//       await().atMost(10, TimeUnit.SECONDS).until(() -> {
//         if (iterator.hasNext()) {
//           results.add(iterator.next());
//         }
//         return results.size() >= 3;
//       });
//
//       // Assertions
//       assertThat(results).hasSize(3);
//
//       // Convert RowData to usable string for assertion
//       List<String> comments = new ArrayList<>();
//       for (RowData row : results) {
//         // Assuming comment is the 2nd field (index 1)
//         comments.add(row.getString(1).toString());
//       }
//
//       assertThat(comments)
//         .contains("Hello World")
//         .contains("Nice Day")
//         .doesNotContain("badword1"); // Verify filtering worked
//     }
//   }
//
//   // Helper to produce data to the TestContainer Kafka
//   private void produceToKafka(List<String> messages) {
//     Properties props = new Properties();
//     props.put("bootstrap.servers", KAFKA.getBootstrapServers());
//     props.put("key.serializer", StringSerializer.class.getName());
//     props.put("value.serializer", StringSerializer.class.getName());
//
//     try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
//       for (String msg : messages) {
//         producer.send(new ProducerRecord<>(INPUT_TOPIC, msg));
//       }
//       producer.flush();
//     }
//   }
// }