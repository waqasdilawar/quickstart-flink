package org.myorg.quickstart.sink;

import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.types.Types;
import org.myorg.quickstart.model.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSinkFunction {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkFunction.class);

  private static final Schema FILTERED_MESSAGE_SCHEMA = new Schema(
    Types.NestedField.optional(1, "account_id", Types.StringType.get()),
    Types.NestedField.optional(2, "message_id", Types.StringType.get()),
    Types.NestedField.optional(3, "message_body", Types.StringType.get()),
    Types.NestedField.optional(4, "correlation_id", Types.StringType.get()),
    Types.NestedField.optional(5, "message_status", Types.StringType.get()),
    Types.NestedField.optional(6, "timestamp", Types.TimestampType.withZone()),
    Types.NestedField.optional(7, "profanity_type", Types.StringType.get())
  );

  /**
   * Creates an Iceberg Dynamic sink BUILDER (does NOT attach to the stream).
   * Caller must invoke {@code builder.append()} explicitly.
   */
  public static DynamicIcebergSink.Builder<RowData> createIcebergSinkBuilder(
    DataStream<RowData> rowDataStream,
    String catalogName,
    String namespace,
    String branch,
    Map<String, String> catalogProps,
    int writeParallelism
  ) {
    LOG.info("Creating Iceberg Dynamic Sink Builder - Catalog: {}, Namespace: {}, Branch: {}, Write Parallelism: {}",
      catalogName, namespace, branch, writeParallelism);

    DynamicIcebergSink.Builder<RowData> builder = DynamicIcebergSink.forInput(rowDataStream)
      .generator((row, out) -> {
        String profanityType = row.getString(6).toString();
        String tableName = "safe_messages";
        if ("PROFANITY".equals(profanityType)) {
          tableName = "profanity_messages";
        }
        out.collect(
          new DynamicRecord(
            TableIdentifier.of(namespace, tableName),
            branch,
            FILTERED_MESSAGE_SCHEMA,
            row,
            PartitionSpec.unpartitioned(),
            DistributionMode.HASH,
            1
          )
        );
      })
      .catalogLoader(CatalogLoader.rest(catalogName, new org.apache.hadoop.conf.Configuration(), catalogProps))
      .writeParallelism(writeParallelism)
      .immediateTableUpdate(true);

    LOG.info("Iceberg Dynamic Sink Builder created successfully - namespace: {}", namespace);
    return builder;
  }

  /**
   * Converts SmsMessage stream to RowData stream for Iceberg.
   *
   * @param smsStream The source stream of SmsMessage objects
   * @return DataStream of RowData ready for Iceberg sink
   */
  public static DataStream<RowData> toRowDataStream(DataStream<MessageEvent> smsStream) {
    RowType flinkRowType = FlinkSchemaUtil.convert(FILTERED_MESSAGE_SCHEMA);
    return smsStream
      .map(new DataToRowConverter())
      .returns(InternalTypeInfo.of(flinkRowType));
  }
}
