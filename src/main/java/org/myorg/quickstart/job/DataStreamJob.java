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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Set;
import java.util.HashSet;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import org.myorg.quickstart.model.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main Flink Streaming Job for processing MessageEvents with profanity detection.
 * Uses dependency injection for sources and sinks to enable testability.
 */
public class DataStreamJob {
	private static final Logger LOG = LoggerFactory.getLogger(DataStreamJob.class);

	private final MessageEventSource source;
	private final MessageEventSink profanitySink;
	private final MessageEventSink icebergSink;
	private final MessageEventSink clickHouseSink;

	/**
	 * Creates a job using the provided source and sinks (for testing).
	 *
	 * @param source the source of MessageEvents
	 * @param profanitySink the sink for profane messages
	 * @param icebergSink the sink for all messages to Iceberg
	 * @param clickHouseSink the sink for all messages to ClickHouse
	 */
	public DataStreamJob(MessageEventSource source, MessageEventSink profanitySink, MessageEventSink icebergSink, MessageEventSink clickHouseSink) {
		this.source = source;
		this.profanitySink = profanitySink;
		this.icebergSink = icebergSink;
		this.clickHouseSink = clickHouseSink;
	}

	/**
	 * Creates a job with default Kafka source and production sinks.
	 */
	public DataStreamJob() {
		this(
			new KafkaMessageEventSource(),
			new KafkaProfanitySink(),
			new IcebergMessageEventSink(),
			new ClickHouseMessageEventSink()
		);
	}

	public static void main(String[] args) throws Exception {
		DataStreamJob job = new DataStreamJob();
		job.execute();
	}

	/**
	 * Executes the Flink streaming job.
	 *
	 * @throws Exception if job execution fails
	 */
	public void execute() throws Exception {
		LOG.info("Starting Flink Iceberg Integration Job");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		configureEnvironment(env);

		// Get source stream
		DataStream<MessageEvent> stream = source.getSource(env);

		// Process stream
		Set<String> profanities = loadProfanities();
		LOG.info("Loaded {} profanity words for message classification", profanities.size());

		SingleOutputStreamOperator<MessageEvent> processedStream = stream.map(new ProfanityClassifier(profanities));

		// Attach sinks
		profanitySink.addSink(
			processedStream.filter(event -> event.getProfanityType() == MessageEvent.ProfanityType.PROFANITY)
		);

		icebergSink.addSink(processedStream);

		clickHouseSink.addSink(processedStream);

		String jobName = System.getenv().getOrDefault("JOB_NAME", "Flink_Iceberg_Integration_Job");
		LOG.info("Starting job execution: {}", jobName);
		env.execute(jobName);
		LOG.info("Job execution completed: {}", jobName);
	}

	/**
	 * Configures the execution environment with checkpointing and state backend settings.
	 *
	 * @param env the StreamExecutionEnvironment to configure
	 */
	private void configureEnvironment(StreamExecutionEnvironment env) {
		String stateBackend = System.getenv().getOrDefault("STATE_BACKEND", "rocksdb");
		String checkpointStorage = System.getenv().getOrDefault("CHECKPOINT_STORAGE", "filesystem");
		String checkpointsDirectory = System.getenv().getOrDefault("CHECKPOINTS_DIRECTORY", "file:///tmp/checkpoints");
		String savepointsDirectory = System.getenv().getOrDefault("SAVEPOINT_DIRECTORY", "file:///tmp/savepoints");

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
		long checkpointInterval = Long.parseLong(System.getenv().getOrDefault("CHECKPOINT_INTERVAL", "300000"));
		long minPause = Long.parseLong(System.getenv().getOrDefault("MIN_PAUSE_BETWEEN_CHECKPOINTS", "60000"));

		env.enableCheckpointing(checkpointInterval);
		LOG.info("Checkpointing enabled with {}ms interval", checkpointInterval);

		CheckpointConfig checkpointConfig = env.getCheckpointConfig();
		checkpointConfig.setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
		checkpointConfig.setMinPauseBetweenCheckpoints(minPause);
		checkpointConfig.setCheckpointTimeout(600000);
		checkpointConfig.setMaxConcurrentCheckpoints(1);
		checkpointConfig.setTolerableCheckpointFailureNumber(3);
		LOG.info("Checkpoint config: mode=EXACTLY_ONCE, timeout=600s, max concurrent=1");
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

}
