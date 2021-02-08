package com.github.msalaslo.streamedrules;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.msalaslo.streamedrules.drools.DroolsRulesApplier;
import com.github.msalaslo.streamedrules.model.Incidence;

/**
 * Runs the Kafka Streams job.
 */
public class KafkaStreamsRunner {

    private KafkaStreamsRunner() {
        //To prevent instantiation
    }

    /**
     * Runs the Kafka Streams job.
     *
     * @param properties the configuration for the job
     * @return the Kafka Streams instance
     */
    public static KafkaStreams runKafkaStream(Properties properties) {
        String droolsSessionName = properties.getProperty("droolsSessionName");
        DroolsRulesApplier rulesApplier = new DroolsRulesApplier(droolsSessionName);
        StreamsBuilder builder = new StreamsBuilder();

        String inputTopic = properties.getProperty("inputTopic");
        String outputTopic = properties.getProperty("outputTopic");
        KStream<String, Incidence> inputData = builder.stream(inputTopic);
        KStream<String, Incidence> outputData = inputData.mapValues(rulesApplier::applyRuleForIncidence);
        outputData.to(outputTopic);

        Properties streamsConfig = createStreamConfig(properties);
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        return streams;
    }

    /**
     * Creates the Kafka Streams configuration.
     *
     * @param properties the configuration for the job
     * @return the Kafka Streams configuration in a Properties object
     */
    private static Properties createStreamConfig(Properties properties) {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, properties.getProperty("applicationName"));
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("bootstrap.servers"));
        
        
		ObjectMapper mapper = new ObjectMapper();
		Serde<Incidence> domainEventSerde = new JsonSerde<>(Incidence.class, mapper);
		
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, domainEventSerde.getClass());
        streamsConfiguration.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        
		
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        return streamsConfiguration;
    }
}
