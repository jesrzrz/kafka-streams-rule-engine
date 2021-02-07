package com.github.msalaslo.streamedrules;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.msalaslo.streamedrules.configuration.PropertiesUtil;

public class SimpleProducer {
	
	private final static Logger LOGGER = LoggerFactory.getLogger(SimpleProducer.class);

	public static void main(String[] args) throws Exception {
		
        Properties properties = PropertiesUtil.loadProperties("config.properties");
        String bootstrapServers = properties.getProperty("bootstrap.servers");
        String topicName = properties.getProperty("inputTopic");
        
		// create instance for properties to access producer configs
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		// Reduce the no of requests less than 0
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		// The buffer.memory controls the total amount of memory available to the
		// producer for buffering.
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		for (int i = 0; i < 1000000; i++) {
			producer.send(new ProducerRecord<String, String>(topicName, "Hello"+ Integer.toString(i)));
			LOGGER.info("Message sent successfully");
		}
		producer.close();
	}
}
