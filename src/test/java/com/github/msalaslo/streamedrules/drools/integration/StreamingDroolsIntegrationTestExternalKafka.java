package com.github.msalaslo.streamedrules.drools.integration;

/**
* Copyright 2016 Confluent Inc.
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
* in compliance with the License. You may obtain a copy of the License at
* http://www.apache.org/licenses/LICENSE-2.0
* Unless required by applicable law or agreed to in writing, software distributed under the License
* is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
* or implied. See the License for the specific language governing permissions and limitations under
* the License.
*/

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Before;
import org.junit.Test;

import com.github.msalaslo.streamedrules.KafkaStreamsRunner;
import com.github.msalaslo.streamedrules.configuration.PropertiesUtil;

/**
 * End-to-end integration test based on WordCountLambdaExample, using an
 * embedded Kafka cluster.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class StreamingDroolsIntegrationTestExternalKafka {

	private AdminClient client = null;
	private String inputTopic = null;
	private String outputTopic = null;
	private String bootstrapServers = null;
	Map<String, Object> conf = new HashMap<>();
	Properties properties = null;

	@Before
	public void setup() throws Exception {
		properties = PropertiesUtil.loadProperties("config.properties");

		inputTopic = (String) properties.getProperty("inputTopic");
		outputTopic = (String) properties.getProperty("outputTopic");
		bootstrapServers = (String) properties.getProperty("bootstrap.servers");

		conf = new HashMap<>();
		conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
		client = AdminClient.create(conf);

	}

	@Test
	public void testApplication() throws Exception {

		CreateTopicsResult result = client.createTopics(
				Arrays.asList(new NewTopic(inputTopic, 1, (short) 1), new NewTopic(outputTopic, 1, (short) 1)));
		try {
			result.all().get();
		} catch (InterruptedException | ExecutionException e) {
			throw new IllegalStateException(e);
		}

		List<String> inputValues = Arrays.asList("Hello", "Canal", "Camel");
		List<String> expectedOutput = Arrays.asList("0Hello", "Canal", "0Camel");

		KafkaStreams streams = KafkaStreamsRunner.runKafkaStream(properties);

		Properties producerConfig = createProducerConfig(bootstrapServers);

//        IntegrationTestUtils.produceValuesSynchronously(inputTopic, inputValues, producerConfig);
//        Properties consumerConfig = createConsumerConfig(bootstrapServers);
//        List<String> actualOutput = IntegrationTestUtils
//                .waitUntilMinValuesRecordsReceived(consumerConfig, outputTopic, expectedOutput.size());
//        assertThat(actualOutput).containsExactlyElementsOf(expectedOutput);
		streams.close();
	}

	private Properties createProducerConfig(String bootstrapServers) {
		Properties producerConfig = new Properties();
		producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
		producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
		producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return producerConfig;
	}

	private Properties createConsumerConfig(String bootstrapServers) {
		Properties consumerConfig = new Properties();
		consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "integration-test-consumer");
		consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		return consumerConfig;
	}
}