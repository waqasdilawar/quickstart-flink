package org.myorg.quickstart.sink;

import com.clickhouse.data.ClickHouseFormat;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor;
import org.apache.flink.connector.clickhouse.data.ClickHousePayload;
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSink;
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig;
import org.myorg.quickstart.model.MessageEvent;

public class ClickHouseSinkFactory {
    public static ClickHouseAsyncSink<MessageEvent> create(
            String jdbcUrl,
            String username,
            String password,
            String database,
            String tableName
    ) {
        ClickHouseClientConfig config = new ClickHouseClientConfig(
                jdbcUrl,
                username,
                password,
                database,
                tableName
        );

        final int maxBatchSize = 5000;
        final int maxInFlightRequests = 15;
        final int maxBufferedRequests = 10000;
        final long maxBatchBytes = 10L * 1024 * 1024; // 10MB
        final long maxTimeInBufferMs = 300;
        final long maxRecordBytes = 4L * 1024 * 1024; // 4MB

        ElementConverter<MessageEvent, ClickHousePayload> elementConverter =
                new ClickHouseConvertor<>(MessageEvent.class, new MessageEventClickHouseConverter());

        ClickHouseAsyncSink<MessageEvent> sink = new ClickHouseAsyncSink<>(
                elementConverter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchBytes,
                maxTimeInBufferMs,
                maxRecordBytes,
                config
        );
        sink.setClickHouseFormat(ClickHouseFormat.RowBinary);
        return sink;
    }
}
