package org.myorg.quickstart.job;

import static org.myorg.quickstart.sink.IcebergSinkFunction.createIcebergSinkBuilder;
import static org.myorg.quickstart.sink.IcebergSinkFunction.toRowDataStream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.myorg.quickstart.model.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Iceberg-based implementation of MessageEventSink.
 * Writes MessageEvents to an Iceberg table via dynamic sink.
 */
public class IcebergMessageEventSink implements MessageEventSink {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergMessageEventSink.class);

    @Override
    public void addSink(DataStream<MessageEvent> stream) {
        String polarisUri = System.getenv().getOrDefault("POLARIS_URI", "http://polaris:8181/api/catalog");
        String polarisCredential = System.getenv().getOrDefault("POLARIS_CREDENTIAL", "admin:password");
        String polarisWarehouse = System.getenv().getOrDefault("POLARIS_WAREHOUSE", "lakehouse");
        String polarisScope = System.getenv().getOrDefault("POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL");
        LOG.info("Polaris catalog configuration: uri={}, warehouse={}, scope={}", polarisUri, polarisWarehouse, polarisScope);

        String s3Endpoint = System.getenv().getOrDefault("S3_ENDPOINT", "http://minio:9000");
        String s3AccessKey = System.getenv().getOrDefault("S3_ACCESS_KEY", "admin");
        String s3SecretKey = System.getenv().getOrDefault("S3_SECRET_KEY", "password");
        String s3PathStyleAccess = System.getenv().getOrDefault("S3_PATH_STYLE_ACCESS", "true");
        String clientRegion = System.getenv().getOrDefault("CLIENT_REGION", "us-east-1");
        LOG.info("S3 configuration: endpoint={}, region={}, path-style-access={}", s3Endpoint, clientRegion, s3PathStyleAccess);

        String oauth2ServerUri = System.getenv().getOrDefault(
            "OAUTH2_SERVER_URI",
            "http://polaris:8181/api/catalog/v1/oauth/tokens"
        );
        String ioImpl = System.getenv().getOrDefault(
            "IO_IMPL",
            "org.apache.iceberg.aws.s3.S3FileIO"
        );
        LOG.info("OAuth2 server: {}, IO implementation: {}", oauth2ServerUri, ioImpl);

        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("uri", polarisUri);
        catalogProps.put("credential", polarisCredential);
        catalogProps.put("warehouse", polarisWarehouse);
        catalogProps.put("scope", polarisScope);
        catalogProps.put("s3.endpoint", s3Endpoint);
        catalogProps.put("s3.access-key-id", s3AccessKey);
        catalogProps.put("s3.secret-access-key", s3SecretKey);
        catalogProps.put("s3.path-style-access", s3PathStyleAccess);
        catalogProps.put("client.region", clientRegion);
        catalogProps.put("io-impl", ioImpl);
        catalogProps.put("rest.auth.type", "oauth2");
        catalogProps.put("oauth2-server-uri", oauth2ServerUri);

        String catalogName = System.getenv().getOrDefault("CATALOG_NAME", "polaris");
        String icebergNamespace = System.getenv().getOrDefault("ICEBERG_NAMESPACE", "raw_data");
        String icebergBranch = System.getenv().getOrDefault("ICEBERG_BRANCH", "main");
        int writeParallelism = Integer.parseInt(System.getenv().getOrDefault("WRITE_PARALLELISM", "1"));
        LOG.info("Creating Iceberg dynamic sink: catalog={}, namespace={}, branch={}, parallelism={}",
            catalogName, icebergNamespace, icebergBranch, writeParallelism);

        DataStream<RowData> rowStream = toRowDataStream(stream);
        createIcebergSinkBuilder(
            rowStream,
            catalogName,
            icebergNamespace,
            icebergBranch,
            catalogProps,
            writeParallelism
        ).append();
        LOG.info("Iceberg dynamic sink attached successfully");
    }
}
