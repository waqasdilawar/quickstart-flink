package org.myorg.quickstart.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import java.time.Instant;
import org.myorg.quickstart.model.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataToRowConverter implements MapFunction<MessageEvent, RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(DataToRowConverter.class);

    @Override
    public RowData map(MessageEvent event) throws Exception {
        GenericRowData row = new GenericRowData(7);

        try {
            String accountId = event.getAccountId();
            row.setField(0, StringData.fromString(accountId != null ? accountId : ""));

            String messageId = event.getMessageId();
            row.setField(1, StringData.fromString(messageId != null ? messageId : ""));

            String messageBody = event.getMessageBody();
            row.setField(2, StringData.fromString(messageBody != null ? messageBody : ""));

            String correlationId = event.getCorrelationId();
            row.setField(3, StringData.fromString(correlationId != null ? correlationId : ""));

            String messageStatus = event.getMessageStatus();
            row.setField(4, StringData.fromString(messageStatus != null ? messageStatus : ""));

            String timestampStr = event.getTimestamp();
            Instant instant = timestampStr != null ? Instant.parse(timestampStr) : Instant.now();
            row.setField(5, TimestampData.fromInstant(instant));

            MessageEvent.ProfanityType type = event.getProfanityType();
            row.setField(6, StringData.fromString(type != null ? type.name() : "SAFE"));

            LOG.debug("Successfully converted MessageEvent to RowData: messageId={}, profanityType={}",
                event.getMessageId(), type);

        } catch (Exception e) {
            LOG.error("Failed to convert MessageEvent to RowData. Event details: messageId={}, accountId={}, correlationId={}. Error: {}",
                event.getMessageId(), event.getAccountId(), event.getCorrelationId(), e.getMessage(), e);
            row.setField(0, StringData.fromString(""));
            row.setField(1, StringData.fromString(""));
            row.setField(2, StringData.fromString(""));
            row.setField(3, StringData.fromString(""));
            row.setField(4, StringData.fromString("ERROR"));
            row.setField(5, TimestampData.fromInstant(Instant.now()));
            row.setField(6, StringData.fromString("SAFE"));
            LOG.warn("Created RowData with default error values for failed conversion");
        }

        return row;
    }
}
