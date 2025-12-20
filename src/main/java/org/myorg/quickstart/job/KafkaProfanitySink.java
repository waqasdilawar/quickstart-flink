package org.myorg.quickstart.job;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.myorg.quickstart.model.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka-based implementation of MessageEventSink for profane messages.
 * Writes MessageEvents to a Kafka output topic.
 */
public class KafkaProfanitySink implements MessageEventSink {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProfanitySink.class);

    @Override
    public void addSink(DataStream<MessageEvent> stream) {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "broker:29092");
        LOG.info("Configuring Kafka sink for profane messages: bootstrap={}, topic=output-topic", bootstrapServers);

        KafkaSink<MessageEvent> kafkaSink = KafkaSink.<MessageEvent>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("output-topic")
                .setValueSerializationSchema(new JsonSerializationSchema<MessageEvent>(
                    () -> new ObjectMapper().registerModule(new JavaTimeModule())
                        .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)))
                .build()
            )
            .setProperty("acks", "all")
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        stream.sinkTo(kafkaSink).name("Kafka Sink (Profane)");
        LOG.info("Kafka sink attached for profanity messages");
    }
}
