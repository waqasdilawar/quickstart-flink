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

package org.myorg.quickstart;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
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
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// Execute program, beginning computation.
		env.enableCheckpointing(1000);

		//ParameterTool parameters = ParameterTool.fromPropertiesFile("./.env");
		String bootstrap_servers = "broker:29092";//parameters.get("127.0.0.1:9092");
		KafkaSource<String> ksource = KafkaSource.<String>builder()
						.setBootstrapServers(bootstrap_servers)
						.setTopics("profanity_words")
						.setGroupId("flink-test")
						.setStartingOffsets(OffsetsInitializer.earliest())
						.setValueOnlyDeserializer(new SimpleStringSchema())
						.setProperty("acks", "all")
						.build();


		DataStreamSource<String> stream = env.fromSource(
						ksource,
						WatermarkStrategy.noWatermarks(),
						"Kafka Source"
		);

		SingleOutputStreamOperator<String> filtered = stream.filter(string -> string.contains("Gun"));

		PrintSink<String> sink = new PrintSink<>(true);

		filtered.sinkTo(sink); // add the print sink to the stream

		KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
						.setBootstrapServers(bootstrap_servers)
						.setRecordSerializer(KafkaRecordSerializationSchema.builder()
										.setTopic("output-topic")
										.setValueSerializationSchema(new SimpleStringSchema())
										.build()
						)
						.setProperty("acks", "all")
						.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
						.build();


		filtered.sinkTo(kafkaSink); // add the Kafka sink to the stream

		env.execute("Flink Java API Skeleton");
	}
}
