package org.myorg.quickstart.sink;

import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.utils.Serialize;
import java.time.LocalDateTime;
import org.apache.flink.connector.clickhouse.convertor.POJOConvertor;
import org.myorg.quickstart.model.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;

/**
 * Converts MessageEvent POJOs to ClickHouse RowBinary format.
 * Matches the ClickHouse table schema:
 * (account_id, message_id, message_body, correlation_id, message_status, event_time, profanity_type)
 */
public class MessageEventClickHouseConverter extends POJOConvertor<MessageEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(MessageEventClickHouseConverter.class);

    @Override
    public void instrument(OutputStream out, MessageEvent r) throws IOException {
        try {
            // 1. account_id String
            Serialize.writeString(out, r.getAccountId() != null ? r.getAccountId() : "", false, false, ClickHouseDataType.String, false, "account_id");

            // 2. message_id String
            Serialize.writeString(out, r.getMessageId() != null ? r.getMessageId() : "", false, false, ClickHouseDataType.String, false, "message_id");

            // 3. message_body String
            Serialize.writeString(out, r.getMessageBody() != null ? r.getMessageBody() : "", false, false, ClickHouseDataType.String, false, "message_body");

            // 4. correlation_id String
            Serialize.writeString(out, r.getCorrelationId() != null ? r.getCorrelationId() : "", false, false, ClickHouseDataType.String, false, "correlation_id");

            // 5. message_status String
            Serialize.writeString(out, r.getMessageStatus() != null ? r.getMessageStatus() : "", false, false, ClickHouseDataType.String, false, "message_status");

            // 6. event_time DateTime64(3)
            // Parse String timestamp to epoch millis. Default to now if null/invalid.
            long epochMillis;
            try {
                if (r.getTimestamp() != null) {
                    epochMillis = Instant.parse(r.getTimestamp()).toEpochMilli();
                } else {
                    epochMillis = System.currentTimeMillis();
                }
            } catch (Exception e) {
                epochMillis = System.currentTimeMillis();
            }
            // Use writeInt64 for DateTime64 as it is physically an Int64 in RowBinary
            Serialize.writeInt64(out, epochMillis, false, false, ClickHouseDataType.DateTime64, false, "event_time");

            // 7. profanity_type String
            String type = r.getProfanityType() != null ? r.getProfanityType().name() : "SAFE";
            Serialize.writeString(out, type, false, false, ClickHouseDataType.String, false, "profanity_type");

        } catch (IOException e) {
            LOG.error("Failed to serialize record: {}", r.getMessageId(), e);
            throw e;
        }
    }
}
