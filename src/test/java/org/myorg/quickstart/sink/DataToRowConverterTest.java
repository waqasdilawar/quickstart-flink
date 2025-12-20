package org.myorg.quickstart.sink;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.myorg.quickstart.model.MessageEvent;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for DataToRowConverter.
 * Tests the MapFunction that converts MessageEvent to RowData for Iceberg.
 */
class DataToRowConverterTest {

    private DataToRowConverter converter;

    @BeforeEach
    void setUp() {
        converter = new DataToRowConverter();
    }

    @Test
    @DisplayName("Should convert complete MessageEvent to RowData")
    void shouldConvertCompleteMessageEvent() throws Exception {
        // Given
        MessageEvent event = new MessageEvent();
        event.setAccountId("account-123");
        event.setMessageId("msg-456");
        event.setMessageBody("This is a test message");
        event.setCorrelationId("corr-789");
        event.setMessageStatus("SENT");
        event.setTimestamp("2025-01-01T10:00:00Z");
        event.setProfanityType(MessageEvent.ProfanityType.SAFE);

        // When
        RowData result = converter.map(event);

        // Then
        assertThat(result).isNotNull();
        assertThat(result.getArity()).isEqualTo(7);
        assertThat(result.getString(0).toString()).isEqualTo("account-123");
        assertThat(result.getString(1).toString()).isEqualTo("msg-456");
        assertThat(result.getString(2).toString()).isEqualTo("This is a test message");
        assertThat(result.getString(3).toString()).isEqualTo("corr-789");
        assertThat(result.getString(4).toString()).isEqualTo("SENT");
        assertThat(result.getTimestamp(5, 6)).isNotNull();
        assertThat(result.getString(6).toString()).isEqualTo("SAFE");
    }

    @Test
    @DisplayName("Should convert MessageEvent with PROFANITY type")
    void shouldConvertMessageEventWithProfanityType() throws Exception {
        // Given
        MessageEvent event = new MessageEvent();
        event.setMessageId("msg-999");
        event.setMessageBody("This contains gun");
        event.setTimestamp("2025-01-01T12:00:00Z");
        event.setProfanityType(MessageEvent.ProfanityType.PROFANITY);

        // When
        RowData result = converter.map(event);

        // Then
        assertThat(result.getString(6).toString()).isEqualTo("PROFANITY");
    }

    @Test
    @DisplayName("Should handle null accountId")
    void shouldHandleNullAccountId() throws Exception {
        // Given
        MessageEvent event = new MessageEvent();
        event.setAccountId(null);
        event.setMessageId("msg-123");
        event.setMessageBody("Test");
        event.setTimestamp("2025-01-01T10:00:00Z");
        event.setProfanityType(MessageEvent.ProfanityType.SAFE);

        // When
        RowData result = converter.map(event);

        // Then
        assertThat(result.getString(0).toString()).isEmpty();
    }

    @Test
    @DisplayName("Should handle null messageId")
    void shouldHandleNullMessageId() throws Exception {
        // Given
        MessageEvent event = new MessageEvent();
        event.setMessageId(null);
        event.setMessageBody("Test");
        event.setTimestamp("2025-01-01T10:00:00Z");
        event.setProfanityType(MessageEvent.ProfanityType.SAFE);

        // When
        RowData result = converter.map(event);

        // Then
        assertThat(result.getString(1).toString()).isEmpty();
    }

    @Test
    @DisplayName("Should handle null messageBody")
    void shouldHandleNullMessageBody() throws Exception {
        // Given
        MessageEvent event = new MessageEvent();
        event.setMessageId("msg-123");
        event.setMessageBody(null);
        event.setTimestamp("2025-01-01T10:00:00Z");
        event.setProfanityType(MessageEvent.ProfanityType.SAFE);

        // When
        RowData result = converter.map(event);

        // Then
        assertThat(result.getString(2).toString()).isEmpty();
    }

    @Test
    @DisplayName("Should handle null correlationId")
    void shouldHandleNullCorrelationId() throws Exception {
        // Given
        MessageEvent event = new MessageEvent();
        event.setMessageId("msg-123");
        event.setCorrelationId(null);
        event.setTimestamp("2025-01-01T10:00:00Z");
        event.setProfanityType(MessageEvent.ProfanityType.SAFE);

        // When
        RowData result = converter.map(event);

        // Then
        assertThat(result.getString(3).toString()).isEmpty();
    }

    @Test
    @DisplayName("Should handle null messageStatus")
    void shouldHandleNullMessageStatus() throws Exception {
        // Given
        MessageEvent event = new MessageEvent();
        event.setMessageId("msg-123");
        event.setMessageStatus(null);
        event.setTimestamp("2025-01-01T10:00:00Z");
        event.setProfanityType(MessageEvent.ProfanityType.SAFE);

        // When
        RowData result = converter.map(event);

        // Then
        assertThat(result.getString(4).toString()).isEmpty();
    }

    @Test
    @DisplayName("Should handle null timestamp by using current time")
    void shouldHandleNullTimestamp() throws Exception {
        // Given
        MessageEvent event = new MessageEvent();
        event.setMessageId("msg-123");
        event.setTimestamp(null);
        event.setProfanityType(MessageEvent.ProfanityType.SAFE);

        long beforeConversion = Instant.now().toEpochMilli();

        // When
        RowData result = converter.map(event);

        long afterConversion = Instant.now().toEpochMilli();

        // Then
        assertThat(result.getTimestamp(5, 6)).isNotNull();
        long resultTimestamp = result.getTimestamp(5, 6).getMillisecond();
        assertThat(resultTimestamp).isBetween(beforeConversion, afterConversion);
    }

    @Test
    @DisplayName("Should handle null profanityType by defaulting to SAFE")
    void shouldHandleNullProfanityType() throws Exception {
        // Given
        MessageEvent event = new MessageEvent();
        event.setMessageId("msg-123");
        event.setTimestamp("2025-01-01T10:00:00Z");
        event.setProfanityType(null);

        // When
        RowData result = converter.map(event);

        // Then
        assertThat(result.getString(6).toString()).isEqualTo("SAFE");
    }

    @Test
    @DisplayName("Should handle empty strings")
    void shouldHandleEmptyStrings() throws Exception {
        // Given
        MessageEvent event = new MessageEvent();
        event.setAccountId("");
        event.setMessageId("");
        event.setMessageBody("");
        event.setCorrelationId("");
        event.setMessageStatus("");
        event.setTimestamp("2025-01-01T10:00:00Z");
        event.setProfanityType(MessageEvent.ProfanityType.SAFE);

        // When
        RowData result = converter.map(event);

        // Then
        assertThat(result.getString(0).toString()).isEmpty();
        assertThat(result.getString(1).toString()).isEmpty();
        assertThat(result.getString(2).toString()).isEmpty();
        assertThat(result.getString(3).toString()).isEmpty();
        assertThat(result.getString(4).toString()).isEmpty();
    }

    @Test
    @DisplayName("Should parse valid ISO 8601 timestamp")
    void shouldParseValidTimestamp() throws Exception {
        // Given
        MessageEvent event = new MessageEvent();
        event.setMessageId("msg-123");
        event.setTimestamp("2025-12-25T15:30:45.123Z");
        event.setProfanityType(MessageEvent.ProfanityType.SAFE);

        // When
        RowData result = converter.map(event);

        // Then
        assertThat(result.getTimestamp(5, 6)).isNotNull();
        Instant expected = Instant.parse("2025-12-25T15:30:45.123Z");
        assertThat(result.getTimestamp(5, 6).getMillisecond()).isEqualTo(expected.toEpochMilli());
    }

    @Test
    @DisplayName("Should handle completely null MessageEvent by using defaults")
    void shouldHandleCompletelyNullMessageEvent() throws Exception {
        // Given
        MessageEvent event = new MessageEvent();

        // When
        RowData result = converter.map(event);

        // Then
        assertThat(result).isNotNull();
        assertThat(result.getArity()).isEqualTo(7);
        assertThat(result.getString(0).toString()).isEmpty();
        assertThat(result.getString(1).toString()).isEmpty();
        assertThat(result.getString(2).toString()).isEmpty();
        assertThat(result.getString(3).toString()).isEmpty();
        assertThat(result.getString(4).toString()).isEmpty();
        assertThat(result.getTimestamp(5, 6)).isNotNull();
        assertThat(result.getString(6).toString()).isEqualTo("SAFE");
    }

    @Test
    @DisplayName("Should handle special characters in message body")
    void shouldHandleSpecialCharacters() throws Exception {
        // Given
        MessageEvent event = new MessageEvent();
        event.setMessageId("msg-123");
        event.setMessageBody("Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?");
        event.setTimestamp("2025-01-01T10:00:00Z");
        event.setProfanityType(MessageEvent.ProfanityType.SAFE);

        // When
        RowData result = converter.map(event);

        // Then
        assertThat(result.getString(2).toString()).isEqualTo("Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?");
    }

    @Test
    @DisplayName("Should handle unicode characters in message body")
    void shouldHandleUnicodeCharacters() throws Exception {
        // Given
        MessageEvent event = new MessageEvent();
        event.setMessageId("msg-123");
        event.setMessageBody("Unicode: 日本語 🚀 émojis");
        event.setTimestamp("2025-01-01T10:00:00Z");
        event.setProfanityType(MessageEvent.ProfanityType.SAFE);

        // When
        RowData result = converter.map(event);

        // Then
        assertThat(result.getString(2).toString()).isEqualTo("Unicode: 日本語 🚀 émojis");
    }

    @Test
    @DisplayName("Should handle very long message body")
    void shouldHandleVeryLongMessageBody() throws Exception {
        // Given
        StringBuilder longMessage = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longMessage.append("This is a very long message. ");
        }

        MessageEvent event = new MessageEvent();
        event.setMessageId("msg-123");
        event.setMessageBody(longMessage.toString());
        event.setTimestamp("2025-01-01T10:00:00Z");
        event.setProfanityType(MessageEvent.ProfanityType.SAFE);

        // When
        RowData result = converter.map(event);

        // Then
        assertThat(result.getString(2).toString()).hasSize(longMessage.length());
    }
}
