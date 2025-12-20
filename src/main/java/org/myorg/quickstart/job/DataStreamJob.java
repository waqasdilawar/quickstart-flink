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

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		// ============================================================
		// 1. Setup Execution Environment with Configuration
		// ============================================================
		var env = StreamExecutionEnvironment.getExecutionEnvironment();

		String jobName = System.getenv().getOrDefault("JOB_NAME", "Flink_Iceberg_Integration_Job");
		String stateBackend = System.getenv().getOrDefault("STATE_BACKEND", "rocksdb");
		String checkpointStorage = System.getenv().getOrDefault("CHECKPOINT_STORAGE", "filesystem");
		String checkpointsDirectory = System.getenv().getOrDefault("CHECKPOINTS_DIRECTORY", "file:///tmp/checkpoints");
		String savepointsDirectory = System.getenv().getOrDefault("SAVEPOINT_DIRECTORY", "file:///tmp/savepoints");
		String s3AccessKey = System.getenv().getOrDefault("S3_ACCESS_KEY", "admin");
		String s3SecretKey = System.getenv().getOrDefault("S3_SECRET_KEY", "password");

		Configuration config = new Configuration();
		config.set(StateBackendOptions.STATE_BACKEND, stateBackend);
		config.set(CheckpointingOptions.CHECKPOINT_STORAGE, checkpointStorage);
		config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointsDirectory);
		config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointsDirectory);
		config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
		config.set(CheckpointingOptions.FS_WRITE_BUFFER_SIZE, 64 * 1024); // 64KB

		env.configure(config);

		env.enableCheckpointing(30000);

		CheckpointConfig checkpointConfig = env.getCheckpointConfig();
		checkpointConfig.setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
		checkpointConfig.setMinPauseBetweenCheckpoints(30000); // Min 30 seconds between checkpoints
		checkpointConfig.setCheckpointTimeout(600000); // 10 minutes timeout
		checkpointConfig.setMaxConcurrentCheckpoints(1); // Only 1 checkpoint at a time
		checkpointConfig.setTolerableCheckpointFailureNumber(3);


		// ============================================================
		// 3. Configure Kafka Source with JSON Deserialization
		// ============================================================
		String bootstrap_servers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "broker:29092");

		JsonDeserializationSchema<MessageEvent> fcdrJsonFormat = new JsonDeserializationSchema<>(MessageEvent.class);

		KafkaSource<MessageEvent> ksource = KafkaSource.<MessageEvent>builder()
						.setBootstrapServers(bootstrap_servers)
						.setTopics("profanity_words")
						.setGroupId("flink-test")
						.setStartingOffsets(OffsetsInitializer.earliest())
						.setValueOnlyDeserializer(fcdrJsonFormat)
						.setProperty("acks", "all")
						.build();

		// Configure watermark strategy based on timestamp field
		WatermarkStrategy<MessageEvent> watermarkStrategy = WatermarkStrategy
						.<MessageEvent>forBoundedOutOfOrderness(java.time.Duration.ofSeconds(10))
						.withTimestampAssigner((event, recordTimestamp) -> {
							try {
								// Parse ISO-8601 timestamp from event
								String timestampStr = event.getTimestamp();
								if (timestampStr != null && !timestampStr.isEmpty()) {
									return java.time.Instant.parse(timestampStr).toEpochMilli();
								}
							} catch (Exception e) {
								// If parsing fails, use current time
							}
							return System.currentTimeMillis();
						});

		DataStreamSource<MessageEvent> stream = env.fromSource(
						ksource,
						watermarkStrategy,
						"Kafka Source"
		);

		// Load profanity list
		Set<String> profanities = loadProfanities();

		// 1. Tag messages with profanity type
		SingleOutputStreamOperator<MessageEvent> processedStream = stream.map(event -> {
			boolean isProfane = containsProfanity(event.getMessageBody(), profanities);
			event.setProfanityType(isProfane ? MessageEvent.ProfanityType.PROFANITY : MessageEvent.ProfanityType.SAFE);
			return event;
		});

		// 2. Kafka Sink (only profane)
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

		String polarisUri = System.getenv().getOrDefault("POLARIS_URI", "http://polaris:8181/api/catalog");
		String polarisCredential = System.getenv().getOrDefault("POLARIS_CREDENTIAL", "admin:password");
		String polarisWarehouse = System.getenv().getOrDefault("POLARIS_WAREHOUSE", "lakehouse");
		String polarisScope = System.getenv().getOrDefault("POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL");

		String s3Endpoint = System.getenv().getOrDefault("S3_ENDPOINT", "http://minio:9000");
		String s3PathStyleAccess = System.getenv().getOrDefault("S3_PATH_STYLE_ACCESS", "true");
		String clientRegion = System.getenv().getOrDefault("CLIENT_REGION", "us-east-1");

		String oauth2ServerUri = System.getenv().getOrDefault(
			"OAUTH2_SERVER_URI",
			"http://polaris:8181/api/catalog/v1/oauth/tokens"
		);
		String ioImpl = System.getenv().getOrDefault(
			"IO_IMPL",
			"org.apache.iceberg.aws.s3.S3FileIO"
		);

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

		// ============================================================
		// 7. Create and Attach Iceberg Sink
		// ============================================================
		String catalogName = System.getenv().getOrDefault("CATALOG_NAME", "polaris");
		String icebergNamespace = System.getenv().getOrDefault("ICEBERG_NAMESPACE", "raw_data");
		String icebergBranch = System.getenv().getOrDefault("ICEBERG_BRANCH", "main");
		int writeParallelism = Integer.parseInt(System.getenv().getOrDefault("WRITE_PARALLELISM", "1"));

		// Dynamic Sink
		DataStream<RowData> rowStream = toRowDataStream(processedStream);
		createIcebergSinkBuilder(
			rowStream,
			catalogName,
			icebergNamespace,
			icebergBranch,
			catalogProps,
			writeParallelism
		).append();

		// ============================================================
		// 8. Optional: Add PrintSink for debugging
		// ============================================================
		// Uncomment to print events to console
		// stream.print();

		// ============================================================
		// 9. Execute the Job
		// ============================================================
		env.execute(jobName);
	}

	private static Set<String> loadProfanities() {
		Set<String> profanities = new HashSet<>();
		try (InputStream is = DataStreamJob.class.getClassLoader().getResourceAsStream("profanity_list.txt")) {
			if (is != null) {
				try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
					profanities = reader.lines().map(String::trim).filter(line -> !line.isEmpty()).collect(Collectors.toSet());
				}
			} else {
				System.err.println("Could not find profanity_list.txt in resources. Using default.");
				profanities.add("gun");
			}
		} catch (Exception e) {
			System.err.println("Error loading profanity list: " + e.getMessage());
			profanities.add("gun");
		}
		return profanities;
	}

	private static boolean containsProfanity(String text, Set<String> profanities) {
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
