package org.myorg.quickstart.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.myorg.quickstart.model.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Kafka-based implementation of MessageEventSource.
 * Reads MessageEvents from a Kafka topic with watermark support.
 */
public class KafkaMessageEventSource implements MessageEventSource {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageEventSource.class);

    @Override
    public DataStream<MessageEvent> getSource(StreamExecutionEnvironment env) {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "broker:29092");
        LOG.info("Configuring Kafka source: bootstrap={}, topic=profanity_words, group=flink-test", bootstrapServers);

        JsonDeserializationSchema<MessageEvent> fcdrJsonFormat = new JsonDeserializationSchema<>(MessageEvent.class);

        KafkaSource<MessageEvent> ksource = KafkaSource.<MessageEvent>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics("profanity_words")
            .setGroupId("flink-test")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(fcdrJsonFormat)
            .setProperty("acks", "all")
            .build();
        LOG.info("Kafka source configured successfully");

        LOG.info("Configuring watermark strategy with 10s bounded out-of-orderness");
        WatermarkStrategy<MessageEvent> watermarkStrategy = WatermarkStrategy
            .<MessageEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
            .withTimestampAssigner((event, recordTimestamp) -> {
                try {
                    String timestampStr = event.getTimestamp();
                    if (timestampStr != null && !timestampStr.isEmpty()) {
                        return java.time.Instant.parse(timestampStr).toEpochMilli();
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to parse timestamp '{}' for event, using current time. Error: {}",
                        event.getTimestamp(), e.getMessage());
                }
                return System.currentTimeMillis();
            });

        return env.fromSource(ksource, watermarkStrategy, "Kafka Source");
    }
}
