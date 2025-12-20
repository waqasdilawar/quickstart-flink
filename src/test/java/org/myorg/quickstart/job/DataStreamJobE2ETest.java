package org.myorg.quickstart.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.myorg.quickstart.model.MessageEvent;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class DataStreamJobE2ETest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension(
        new MiniClusterResourceConfiguration.Builder()
            .setNumberSlotsPerTaskManager(2)
            .setNumberTaskManagers(1)
            .build()
    );

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
    );

    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;
    private static final Set<String> PROFANITIES = Set.of("gun", "badword", "offensive");

    @BeforeEach
    void setUp() {
        // Setup Kafka producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
                         KAFKA.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
                         StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
                         StringSerializer.class);
        producer = new KafkaProducer<>(producerProps);

        // Setup Kafka consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
                         KAFKA.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
                         StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
                         StringDeserializer.class);
        consumer = new KafkaConsumer<>(consumerProps);
    }

    @Test
    void shouldProcessMessagesEndToEnd() throws Exception {
        // Given: Send messages to Kafka
        String inputTopic = "profanity_words";
        String outputTopic = "output-topic";
        
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        MessageEvent profaneMsg = createMessage("1", "This contains gun");
        MessageEvent safeMsg = createMessage("2", "Hello world");
        
        producer.send(new ProducerRecord<>(inputTopic, 
            mapper.writeValueAsString(profaneMsg)));
        producer.send(new ProducerRecord<>(inputTopic, 
            mapper.writeValueAsString(safeMsg)));
        producer.flush();

        // When: Run Flink job
        StreamExecutionEnvironment env = 
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<MessageEvent> source = KafkaSource.<MessageEvent>builder()
            .setBootstrapServers(KAFKA.getBootstrapServers())
            .setTopics(inputTopic)
            .setGroupId("test-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(
                new JsonDeserializationSchema<>(MessageEvent.class))
            .setBounded(OffsetsInitializer.latest()) // Bounded for testing
            .build();

        // Apply processing logic...
        DataStream<MessageEvent> stream = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "Test Kafka Source"
        );
        
        SingleOutputStreamOperator<MessageEvent> processedStream = stream.map(new ProfanityClassifier(PROFANITIES));

        // ... rest of pipeline with KafkaSink to outputTopic
        KafkaSink<MessageEvent> kafkaSink = KafkaSink.<MessageEvent>builder()
			.setBootstrapServers(KAFKA.getBootstrapServers())
			.setRecordSerializer(KafkaRecordSerializationSchema.builder()
				.setTopic(outputTopic)
				.setValueSerializationSchema(new JsonSerializationSchema<MessageEvent>(
					() -> new ObjectMapper().registerModule(new JavaTimeModule())
						.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)))
				.build()
			)
			.setProperty("acks", "all")
			.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
			.build();

		processedStream.filter(event -> event.getProfanityType() == MessageEvent.ProfanityType.PROFANITY)
			.sinkTo(kafkaSink).name("Kafka Sink (Profane)");

        env.execute("E2E Test");

        // Then: Verify output in Kafka
        consumer.subscribe(List.of(outputTopic));
        ConsumerRecords<String, String> records = 
            consumer.poll(Duration.ofSeconds(10));
        
        List<MessageEvent> results = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            results.add(mapper.readValue(record.value(), MessageEvent.class));
        }

        assertThat(results)
            .hasSize(1)
            .extracting(MessageEvent::getMessageId)
            .containsExactly("1");
    }

    // Helper methods
    private MessageEvent createMessage(String id, String body) {
        MessageEvent event = new MessageEvent();
        event.setMessageId(id);
        event.setMessageBody(body);
        event.setTimestamp("2025-01-01T10:00:00Z");
        return event;
    }

}