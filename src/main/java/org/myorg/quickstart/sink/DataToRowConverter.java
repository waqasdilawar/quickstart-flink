package org.myorg.quickstart.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import java.time.Instant;
import org.myorg.quickstart.model.MessageEvent;

/**
 * Converts FcdrEvent POJO to RowData format required by Iceberg sink.
 *
 * This converter creates RowData matching the FCDR_SCHEMA with 6 fields:
 * - account_id (String) - from account.id
 * - message_id (String) - from data.body.messageId
 * - message_body (String) - from data.body.messageBody
 * - correlation_id (String) - from correlation_id
 * - message_status (String) - from data.body.messageDeliveryStatus
 * - timestamp (Timestamp) - from timestamp
 */
public class DataToRowConverter implements MapFunction<MessageEvent, RowData> {

    @Override
    public RowData map(MessageEvent event) throws Exception {
        GenericRowData row = new GenericRowData(6); // 6 fields in schema

        try {
            // Field 0: account_id
            String accountId = event.getAccountId();
            row.setField(0, StringData.fromString(accountId != null ? accountId : ""));

            // Field 1: message_id
            String messageId = event.getMessageId();
            row.setField(1, StringData.fromString(messageId != null ? messageId : ""));

            // Field 2: message_body
            String messageBody = event.getMessageBody();
            row.setField(2, StringData.fromString(messageBody != null ? messageBody : ""));

            // Field 3: correlation_id
            String correlationId = event.getCorrelationId();
            row.setField(3, StringData.fromString(correlationId != null ? correlationId : ""));

            // Field 4: message_status
            String messageStatus = event.getMessageStatus();
            row.setField(4, StringData.fromString(messageStatus != null ? messageStatus : ""));

            // Field 5: timestamp
            String timestampStr = event.getTimestamp();
            Instant instant = timestampStr != null ? Instant.parse(timestampStr) : Instant.now();
            row.setField(5, TimestampData.fromInstant(instant));

        } catch (Exception e) {
            // If parsing fails, set default values
            row.setField(0, StringData.fromString(""));
            row.setField(1, StringData.fromString(""));
            row.setField(2, StringData.fromString(""));
            row.setField(3, StringData.fromString(""));
            row.setField(4, StringData.fromString("ERROR"));
            row.setField(5, TimestampData.fromInstant(Instant.now()));
        }

        return row;
    }
}
