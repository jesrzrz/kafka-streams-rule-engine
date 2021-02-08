package com.github.msalaslo.streamedrules;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.msalaslo.streamedrules.configuration.PropertiesUtil;
import com.github.msalaslo.streamedrules.model.Incidence;


public class InstallationMaintenanceProducer {
	
	private final static Logger LOGGER = LoggerFactory.getLogger(InstallationMaintenanceProducer.class);

	public static void main(String[] args) throws Exception {
		
        Properties properties = PropertiesUtil.loadProperties("config.properties");
        String bootstrapServers = properties.getProperty("bootstrap.servers");
        String topicName = properties.getProperty("inputTopic");
               
		// create instance for properties to access producer config
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
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		
		ObjectMapper mapper = new ObjectMapper();
		Serde<Incidence> domainEventSerde = new JsonSerde<>(Incidence.class, mapper);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, domainEventSerde.serializer().getClass());
		
		Producer<String, Incidence> producer = new KafkaProducer<String, Incidence>(props);

		for (int i = 0; i < 1; i++) {
			Incidence incidence = Incidence.builder().
					id(""+ i).
					type("SS").
					param1("ALTO").
					param2("FG").
					param3(29).
					param4(false).
					build();
			
			producer.send(new ProducerRecord<String, Incidence>(topicName, incidence.getId(), incidence));
			LOGGER.info("Message sent successfully");
		}
		producer.close();
		domainEventSerde.close();
	}
}
