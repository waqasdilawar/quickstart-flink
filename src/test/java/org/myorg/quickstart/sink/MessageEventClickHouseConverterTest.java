package org.myorg.quickstart.sink;

import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.utils.Serialize;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.myorg.quickstart.model.MessageEvent;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
public class MessageEventClickHouseConverterTest {

    @Mock
    private OutputStream outputStream;

    @Test
    public void testInstrument() throws IOException {
        MessageEvent event = new MessageEvent();
        event.setAccountId("acc1");
        event.setMessageId("msg1");
        event.setMessageBody("hello");
        event.setCorrelationId("corr1");
        event.setMessageStatus("SENT");
        event.setTimestamp("2023-01-01T12:00:00Z");
        event.setProfanityType(MessageEvent.ProfanityType.SAFE);

        MessageEventClickHouseConverter converter = new MessageEventClickHouseConverter();

        try (MockedStatic<Serialize> serializeMock = Mockito.mockStatic(Serialize.class)) {
            converter.instrument(outputStream, event);

            // Verify calls in order (conceptually, though mockStatic verification is tricky for order across different static methods)
            // We just verify they were called with correct arguments
            
            // 1. account_id
            serializeMock.verify(() -> Serialize.writeString(eq(outputStream), eq("acc1"), eq(false), eq(false), eq(ClickHouseDataType.String), eq(false), eq("account_id")));

            // 2. message_id
            serializeMock.verify(() -> Serialize.writeString(eq(outputStream), eq("msg1"), eq(false), eq(false), eq(ClickHouseDataType.String), eq(false), eq("message_id")));

            // 6. event_time
            long expectedEpoch = Instant.parse("2023-01-01T12:00:00Z").toEpochMilli();
            serializeMock.verify(() -> Serialize.writeInt64(eq(outputStream), eq(expectedEpoch), eq(false), eq(false), eq(ClickHouseDataType.DateTime64), eq(false), eq("event_time")));
            
            // 7. profanity_type
            serializeMock.verify(() -> Serialize.writeString(eq(outputStream), eq("SAFE"), eq(false), eq(false), eq(ClickHouseDataType.String), eq(false), eq("profanity_type")));
        }
    }

    @Test
    public void testInstrumentWithNulls() throws IOException {
        MessageEvent event = new MessageEvent();
        // All nulls

        MessageEventClickHouseConverter converter = new MessageEventClickHouseConverter();

        try (MockedStatic<Serialize> serializeMock = Mockito.mockStatic(Serialize.class)) {
            converter.instrument(outputStream, event);

            // Verify defaults
            serializeMock.verify(() -> Serialize.writeString(eq(outputStream), eq(""), eq(false), eq(false), eq(ClickHouseDataType.String), eq(false), eq("account_id")));
            
            // For timestamp, it uses current time, so we just check it was called
            serializeMock.verify(() -> Serialize.writeInt64(eq(outputStream), any(Long.class), eq(false), eq(false), eq(ClickHouseDataType.DateTime64), eq(false), eq("event_time")));
            
             // Profanity default
            serializeMock.verify(() -> Serialize.writeString(eq(outputStream), eq("SAFE"), eq(false), eq(false), eq(ClickHouseDataType.String), eq(false), eq("profanity_type")));
        }
    }
}
