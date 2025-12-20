package org.myorg.quickstart.job;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.myorg.quickstart.model.MessageEvent;
import org.myorg.quickstart.sink.ClickHouseSinkFactory;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ClickHouseMessageEventSinkTest {

    @Mock
    private DataStream<MessageEvent> stream;
    @Mock
    private DataStreamSink<MessageEvent> sink;

    @Test
    void testAddSink() {
        when(stream.sinkTo(any())).thenReturn(sink);
        
        ClickHouseMessageEventSink clickHouseSink = new ClickHouseMessageEventSink();
        assertDoesNotThrow(() -> clickHouseSink.addSink(stream));
        
        verify(stream).sinkTo(any());
    }
    
    @Test
    void testFactoryCreate() {
        assertDoesNotThrow(() -> ClickHouseSinkFactory.create(
            "http://localhost:8123", "user", "pass", "db", "table"
        ));
    }
}
