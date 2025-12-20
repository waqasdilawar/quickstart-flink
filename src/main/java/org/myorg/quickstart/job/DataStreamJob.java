/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart.job;

import static org.myorg.quickstart.sink.IcebergSinkFunction.createIcebergSinkBuilder;
import static org.myorg.quickstart.sink.IcebergSinkFunction.toRowDataStream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import org.myorg.quickstart.model.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStreamJob {
	private static final Logger LOG = LoggerFactory.getLogger(DataStreamJob.class);

	public static void main(String[] args) throws Exception {
		LOG.info("Starting Flink Iceberg Integration Job");
		var env = StreamExecutionEnvironment.getExecutionEnvironment();

		String jobName = System.getenv().getOrDefault("JOB_NAME", "Flink_Iceberg_Integration_Job");
		LOG.info("Job name: {}", jobName);
		String stateBackend = System.getenv().getOrDefault("STATE_BACKEND", "rocksdb");
		String checkpointStorage = System.getenv().getOrDefault("CHECKPOINT_STORAGE", "filesystem");
		String checkpointsDirectory = System.getenv().getOrDefault("CHECKPOINTS_DIRECTORY", "file:///tmp/checkpoints");
		String savepointsDirectory = System.getenv().getOrDefault("SAVEPOINT_DIRECTORY", "file:///tmp/savepoints");
		String s3AccessKey = System.getenv().getOrDefault("S3_ACCESS_KEY", "admin");
		String s3SecretKey = System.getenv().getOrDefault("S3_SECRET_KEY", "password");

		LOG.info("Configuring state backend: {}, checkpoint storage: {}", stateBackend, checkpointStorage);
		LOG.info("Checkpoints directory: {}, Savepoints directory: {}", checkpointsDirectory, savepointsDirectory);

		Configuration config = new Configuration();
		config.set(StateBackendOptions.STATE_BACKEND, stateBackend);
		config.set(CheckpointingOptions.CHECKPOINT_STORAGE, checkpointStorage);
		config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointsDirectory);
		config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointsDirectory);
		config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
		config.set(CheckpointingOptions.FS_WRITE_BUFFER_SIZE, 64 * 1024);

		env.configure(config);

		env.enableCheckpointing(30000);
		LOG.info("Checkpointing enabled with 30s interval");

		CheckpointConfig checkpointConfig = env.getCheckpointConfig();
		checkpointConfig.setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
		checkpointConfig.setMinPauseBetweenCheckpoints(30000);
		checkpointConfig.setCheckpointTimeout(600000);
		checkpointConfig.setMaxConcurrentCheckpoints(1);
		checkpointConfig.setTolerableCheckpointFailureNumber(3);
		LOG.info("Checkpoint config: mode=EXACTLY_ONCE, timeout=600s, max concurrent=1");

		String bootstrap_servers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "broker:29092");
		LOG.info("Configuring Kafka source: bootstrap={}, topic=profanity_words, group=flink-test", bootstrap_servers);

		JsonDeserializationSchema<MessageEvent> fcdrJsonFormat = new JsonDeserializationSchema<>(MessageEvent.class);

		KafkaSource<MessageEvent> ksource = KafkaSource.<MessageEvent>builder()
						.setBootstrapServers(bootstrap_servers)
						.setTopics("profanity_words")
						.setGroupId("flink-test")
						.setStartingOffsets(OffsetsInitializer.earliest())
						.setValueOnlyDeserializer(fcdrJsonFormat)
						.setProperty("acks", "all")
						.build();
		LOG.info("Kafka source configured successfully");

		LOG.info("Configuring watermark strategy with 10s bounded out-of-orderness");
		WatermarkStrategy<MessageEvent> watermarkStrategy = WatermarkStrategy
						.<MessageEvent>forBoundedOutOfOrderness(java.time.Duration.ofSeconds(10))
						.withTimestampAssigner((event, recordTimestamp) -> {
							try {
								String timestampStr = event.getTimestamp();
								if (timestampStr != null && !timestampStr.isEmpty()) {
									return java.time.Instant.parse(timestampStr).toEpochMilli();
								}
							} catch (Exception e) {
								LOG.warn("Failed to parse timestamp '{}' for event, using current time. Error: {}",
									event.getTimestamp(), e.getMessage());
							}
							return System.currentTimeMillis();
						});

		DataStreamSource<MessageEvent> stream = env.fromSource(
						ksource,
						watermarkStrategy,
						"Kafka Source"
		);

		Set<String> profanities = loadProfanities();
		LOG.info("Loaded {} profanity words for message classification", profanities.size());

		SingleOutputStreamOperator<MessageEvent> processedStream = stream.map(event -> {
			boolean isProfane = containsProfanity(event.getMessageBody(), profanities);
			event.setProfanityType(isProfane ? MessageEvent.ProfanityType.PROFANITY : MessageEvent.ProfanityType.SAFE);
			LOG.debug("Message {} classified as: {} (body: '{}')",
				event.getMessageId(), event.getProfanityType(), event.getMessageBody());
			return event;
		});

		LOG.info("Configuring Kafka sink for profane messages: topic=output-topic");
		KafkaSink<MessageEvent> kafkaSink = KafkaSink.<MessageEvent>builder()
			.setBootstrapServers(bootstrap_servers)
			.setRecordSerializer(KafkaRecordSerializationSchema.builder()
														 .setTopic("output-topic")
														 .setValueSerializationSchema(new JsonSerializationSchema<MessageEvent>(
															 () -> new ObjectMapper().registerModule(new JavaTimeModule())
																 .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)))
														 .build()
			)
			.setProperty("acks", "all")
			.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
			.build();

		processedStream.filter(event -> event.getProfanityType() == MessageEvent.ProfanityType.PROFANITY)
			.sinkTo(kafkaSink).name("Kafka Sink (Profane)");
		LOG.info("Kafka sink attached for profanity messages");

		String polarisUri = System.getenv().getOrDefault("POLARIS_URI", "http://polaris:8181/api/catalog");
		String polarisCredential = System.getenv().getOrDefault("POLARIS_CREDENTIAL", "admin:password");
		String polarisWarehouse = System.getenv().getOrDefault("POLARIS_WAREHOUSE", "lakehouse");
		String polarisScope = System.getenv().getOrDefault("POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL");
		LOG.info("Polaris catalog configuration: uri={}, warehouse={}, scope={}", polarisUri, polarisWarehouse, polarisScope);

		String s3Endpoint = System.getenv().getOrDefault("S3_ENDPOINT", "http://minio:9000");
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

		DataStream<RowData> rowStream = toRowDataStream(processedStream);
		createIcebergSinkBuilder(
			rowStream,
			catalogName,
			icebergNamespace,
			icebergBranch,
			catalogProps,
			writeParallelism
		).append();
		LOG.info("Iceberg dynamic sink attached successfully");

		LOG.info("Starting job execution: {}", jobName);
		env.execute(jobName);
		LOG.info("Job execution completed: {}", jobName);
	}

	public static Set<String> loadProfanities() {
		Set<String> profanities = new HashSet<>();
		try (InputStream is = DataStreamJob.class.getClassLoader().getResourceAsStream("profanity_list.txt")) {
			if (is != null) {
				try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
					profanities = reader.lines().map(String::trim).filter(line -> !line.isEmpty()).collect(Collectors.toSet());
					LOG.info("Successfully loaded profanity list from profanity_list.txt");
				}
			} else {
				LOG.warn("Could not find profanity_list.txt in resources. Using default profanity word: 'gun'");
				profanities.add("gun");
			}
		} catch (Exception e) {
			LOG.error("Error loading profanity list from file: {}. Using default profanity word: 'gun'", e.getMessage(), e);
			profanities.add("gun");
		}
		return profanities;
	}

	public static boolean containsProfanity(String text, Set<String> profanities) {
		if (text == null || text.isEmpty()) {
			return false;
		}
		String lower = text.toLowerCase();
		for (String badWord : profanities) {
			if (lower.contains(badWord.toLowerCase())) {
				return true;
			}
		}
		return false;
	}
}
