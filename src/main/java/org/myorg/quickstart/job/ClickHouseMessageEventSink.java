package org.myorg.quickstart.job;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.myorg.quickstart.model.MessageEvent;
import org.myorg.quickstart.sink.ClickHouseSinkFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClickHouseMessageEventSink implements MessageEventSink {
    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseMessageEventSink.class);

    @Override
    public void addSink(DataStream<MessageEvent> stream) {
        String jdbcUrl = System.getenv().getOrDefault("CLICKHOUSE_JDBC_URL", "http://clickhouse:8123");
        String user = System.getenv().getOrDefault("CLICKHOUSE_USER", "default");
        String password = System.getenv().getOrDefault("CLICKHOUSE_PASSWORD", "changeme");
        String database = System.getenv().getOrDefault("CLICKHOUSE_DATABASE", "default");
        String table = System.getenv().getOrDefault("CLICKHOUSE_TABLE", "message_events");

        LOG.info("Configuring ClickHouse sink: url={}, db={}, table={}", jdbcUrl, database, table);

        try {
            var sink = ClickHouseSinkFactory.create(jdbcUrl, user, password, database, table);
            stream.sinkTo(sink).name("ClickHouse Sink");
            LOG.info("ClickHouse sink attached successfully");
        } catch (Exception e) {
            LOG.error("Failed to create ClickHouse sink", e);
            throw new RuntimeException("Failed to create ClickHouse sink", e);
        }
    }
}
