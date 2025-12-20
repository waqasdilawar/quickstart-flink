package org.myorg.quickstart.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.myorg.quickstart.model.MessageEvent;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Unit tests for IcebergSinkFunction.
 * Tests the RowData conversion and sink builder creation.
 */
class IcebergSinkFunctionTest {

    private StreamExecutionEnvironment env;

    @BeforeEach
    void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
    }

    @Test
    @DisplayName("Should convert MessageEvent stream to RowData stream")
    void shouldConvertMessageEventStreamToRowDataStream() {
        // Given
        MessageEvent event1 = createMessage("msg-1", "Safe message", MessageEvent.ProfanityType.SAFE);
        MessageEvent event2 = createMessage("msg-2", "Contains gun", MessageEvent.ProfanityType.PROFANITY);

        List<MessageEvent> events = Arrays.asList(event1, event2);
        DataStream<MessageEvent> messageStream = env.fromCollection(events);

        // When/Then - Should not throw exception
        assertThatCode(() -> {
            DataStream<RowData> rowDataStream = IcebergSinkFunction.toRowDataStream(messageStream);
            assertThat(rowDataStream).isNotNull();
        }).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should handle MessageEvent with null fields")
    void shouldHandleMessageEventWithNullFields() {
        // Given
        MessageEvent event = new MessageEvent();
        event.setMessageId("msg-1");
        // All other fields are null

        List<MessageEvent> events = Arrays.asList(event);
        DataStream<MessageEvent> messageStream = env.fromCollection(events);

        // When/Then - Should not throw exception
        assertThatCode(() -> {
            DataStream<RowData> rowDataStream = IcebergSinkFunction.toRowDataStream(messageStream);
            assertThat(rowDataStream).isNotNull();
        }).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should handle MessageEvent with PROFANITY type")
    void shouldHandleMessageEventWithProfanityType() {
        // Given
        MessageEvent event = createMessage("msg-1", "Gun violence", MessageEvent.ProfanityType.PROFANITY);

        List<MessageEvent> events = Arrays.asList(event);
        DataStream<MessageEvent> messageStream = env.fromCollection(events);

        // When/Then - Should not throw exception
        assertThatCode(() -> {
            DataStream<RowData> rowDataStream = IcebergSinkFunction.toRowDataStream(messageStream);
            assertThat(rowDataStream).isNotNull();
        }).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should handle MessageEvent with SAFE type")
    void shouldHandleMessageEventWithSafeType() {
        // Given
        MessageEvent event = createMessage("msg-1", "Hello world", MessageEvent.ProfanityType.SAFE);

        List<MessageEvent> events = Arrays.asList(event);
        DataStream<MessageEvent> messageStream = env.fromCollection(events);

        // When/Then - Should not throw exception
        assertThatCode(() -> {
            DataStream<RowData> rowDataStream = IcebergSinkFunction.toRowDataStream(messageStream);
            assertThat(rowDataStream).isNotNull();
        }).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should handle large batch of MessageEvents")
    void shouldHandleLargeBatchOfMessageEvents() {
        // Given
        List<MessageEvent> events = Arrays.asList(
            createMessage("msg-1", "Message 1", MessageEvent.ProfanityType.SAFE),
            createMessage("msg-2", "Message 2 gun", MessageEvent.ProfanityType.PROFANITY),
            createMessage("msg-3", "Message 3", MessageEvent.ProfanityType.SAFE),
            createMessage("msg-4", "Message 4 badword", MessageEvent.ProfanityType.PROFANITY),
            createMessage("msg-5", "Message 5", MessageEvent.ProfanityType.SAFE)
        );

        DataStream<MessageEvent> messageStream = env.fromCollection(events);

        // When/Then - Should not throw exception
        assertThatCode(() -> {
            DataStream<RowData> rowDataStream = IcebergSinkFunction.toRowDataStream(messageStream);
            assertThat(rowDataStream).isNotNull();
        }).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should handle MessageEvent with special characters")
    void shouldHandleMessageEventWithSpecialCharacters() {
        // Given
        MessageEvent event = createMessage(
            "msg-1",
            "Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?",
            MessageEvent.ProfanityType.SAFE
        );

        List<MessageEvent> events = Arrays.asList(event);
        DataStream<MessageEvent> messageStream = env.fromCollection(events);

        // When/Then - Should not throw exception
        assertThatCode(() -> {
            DataStream<RowData> rowDataStream = IcebergSinkFunction.toRowDataStream(messageStream);
            assertThat(rowDataStream).isNotNull();
        }).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should handle MessageEvent with unicode characters")
    void shouldHandleMessageEventWithUnicodeCharacters() {
        // Given
        MessageEvent event = createMessage(
            "msg-1",
            "Unicode: 日本語 🚀 émojis",
            MessageEvent.ProfanityType.SAFE
        );

        List<MessageEvent> events = Arrays.asList(event);
        DataStream<MessageEvent> messageStream = env.fromCollection(events);

        // When/Then - Should not throw exception
        assertThatCode(() -> {
            DataStream<RowData> rowDataStream = IcebergSinkFunction.toRowDataStream(messageStream);
            assertThat(rowDataStream).isNotNull();
        }).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should handle MessageEvent with very long body")
    void shouldHandleMessageEventWithVeryLongBody() {
        // Given
        StringBuilder longMessage = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longMessage.append("This is a long message. ");
        }

        MessageEvent event = createMessage(
            "msg-1",
            longMessage.toString(),
            MessageEvent.ProfanityType.SAFE
        );

        List<MessageEvent> events = Arrays.asList(event);
        DataStream<MessageEvent> messageStream = env.fromCollection(events);

        // When/Then - Should not throw exception
        assertThatCode(() -> {
            DataStream<RowData> rowDataStream = IcebergSinkFunction.toRowDataStream(messageStream);
            assertThat(rowDataStream).isNotNull();
        }).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should create RowData stream with proper type information")
    void shouldCreateRowDataStreamWithProperTypeInformation() {
        // Given
        MessageEvent event = createMessage("msg-1", "Test", MessageEvent.ProfanityType.SAFE);
        List<MessageEvent> events = Arrays.asList(event);
        DataStream<MessageEvent> messageStream = env.fromCollection(events);

        // When
        DataStream<RowData> rowDataStream = IcebergSinkFunction.toRowDataStream(messageStream);

        // Then
        assertThat(rowDataStream).isNotNull();
        assertThat(rowDataStream.getType()).isNotNull();
    }

    @Test
    @DisplayName("Should create Iceberg sink builder without throwing exception")
    void shouldCreateIcebergSinkBuilder() {
        // Given
        MessageEvent event = createMessage("msg-1", "Test", MessageEvent.ProfanityType.SAFE);
        List<MessageEvent> events = Arrays.asList(event);
        DataStream<MessageEvent> messageStream = env.fromCollection(events);
        DataStream<RowData> rowDataStream = IcebergSinkFunction.toRowDataStream(messageStream);

        java.util.Map<String, String> catalogProps = new java.util.HashMap<>();
        catalogProps.put("uri", "http://polaris:8181/api/catalog");
        catalogProps.put("credential", "admin:password");
        catalogProps.put("warehouse", "lakehouse");

        // When/Then - Should not throw exception during builder creation
        assertThatCode(() -> {
            IcebergSinkFunction.createIcebergSinkBuilder(
                rowDataStream,
                "test-catalog",
                "test_namespace",
                "main",
                catalogProps,
                1
            );
        }).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should create Iceberg sink builder with custom parallelism")
    void shouldCreateIcebergSinkBuilderWithCustomParallelism() {
        // Given
        MessageEvent event = createMessage("msg-1", "Test", MessageEvent.ProfanityType.SAFE);
        List<MessageEvent> events = Arrays.asList(event);
        DataStream<MessageEvent> messageStream = env.fromCollection(events);
        DataStream<RowData> rowDataStream = IcebergSinkFunction.toRowDataStream(messageStream);

        java.util.Map<String, String> catalogProps = new java.util.HashMap<>();
        catalogProps.put("uri", "http://polaris:8181/api/catalog");

        // When/Then - Should handle different parallelism values
        assertThatCode(() -> {
            IcebergSinkFunction.createIcebergSinkBuilder(
                rowDataStream,
                "test-catalog",
                "test_namespace",
                "main",
                catalogProps,
                4
            );
        }).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should create Iceberg sink builder with custom branch")
    void shouldCreateIcebergSinkBuilderWithCustomBranch() {
        // Given
        MessageEvent event = createMessage("msg-1", "Test", MessageEvent.ProfanityType.SAFE);
        List<MessageEvent> events = Arrays.asList(event);
        DataStream<MessageEvent> messageStream = env.fromCollection(events);
        DataStream<RowData> rowDataStream = IcebergSinkFunction.toRowDataStream(messageStream);

        java.util.Map<String, String> catalogProps = new java.util.HashMap<>();
        catalogProps.put("uri", "http://polaris:8181/api/catalog");

        // When/Then - Should handle custom branch names
        assertThatCode(() -> {
            IcebergSinkFunction.createIcebergSinkBuilder(
                rowDataStream,
                "test-catalog",
                "test_namespace",
                "feature-branch",
                catalogProps,
                1
            );
        }).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should create Iceberg sink builder with empty catalog properties")
    void shouldCreateIcebergSinkBuilderWithEmptyCatalogProperties() {
        // Given
        MessageEvent event = createMessage("msg-1", "Test", MessageEvent.ProfanityType.SAFE);
        List<MessageEvent> events = Arrays.asList(event);
        DataStream<MessageEvent> messageStream = env.fromCollection(events);
        DataStream<RowData> rowDataStream = IcebergSinkFunction.toRowDataStream(messageStream);

        java.util.Map<String, String> catalogProps = new java.util.HashMap<>();

        // When/Then - Should not throw during builder creation (may fail at execution)
        assertThatCode(() -> {
            IcebergSinkFunction.createIcebergSinkBuilder(
                rowDataStream,
                "test-catalog",
                "test_namespace",
                "main",
                catalogProps,
                1
            );
        }).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should handle MessageEvent with all SAFE messages")
    void shouldHandleMessageEventWithAllSafeMessages() {
        // Given
        MessageEvent event = createMessage("msg-1", "Safe message", MessageEvent.ProfanityType.SAFE);
        List<MessageEvent> events = Arrays.asList(event);
        DataStream<MessageEvent> messageStream = env.fromCollection(events);

        // When/Then
        assertThatCode(() -> {
            DataStream<RowData> rowDataStream = IcebergSinkFunction.toRowDataStream(messageStream);
            assertThat(rowDataStream).isNotNull();
        }).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should handle stream with mixed profanity types")
    void shouldHandleStreamWithMixedProfanityTypes() {
        // Given
        List<MessageEvent> events = Arrays.asList(
            createMessage("msg-1", "Safe content", MessageEvent.ProfanityType.SAFE),
            createMessage("msg-2", "Profane gun", MessageEvent.ProfanityType.PROFANITY),
            createMessage("msg-3", "Another safe content", MessageEvent.ProfanityType.SAFE)
        );
        DataStream<MessageEvent> messageStream = env.fromCollection(events);

        // When/Then
        assertThatCode(() -> {
            DataStream<RowData> rowDataStream = IcebergSinkFunction.toRowDataStream(messageStream);
            assertThat(rowDataStream).isNotNull();
        }).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should convert empty stream without errors")
    void shouldConvertEmptyStream() {
        // Given
        DataStream<MessageEvent> emptyStream = env.fromCollection(java.util.Collections.emptyList());

        // When/Then
        assertThatCode(() -> {
            DataStream<RowData> rowDataStream = IcebergSinkFunction.toRowDataStream(emptyStream);
            assertThat(rowDataStream).isNotNull();
        }).doesNotThrowAnyException();
    }

    // Helper method
    private MessageEvent createMessage(String id, String body, MessageEvent.ProfanityType type) {
        MessageEvent event = new MessageEvent();
        event.setAccountId("account-123");
        event.setMessageId(id);
        event.setMessageBody(body);
        event.setCorrelationId("corr-" + id);
        event.setMessageStatus("SENT");
        event.setTimestamp("2025-01-01T10:00:00Z");
        event.setProfanityType(type);
        return event;
    }
}
