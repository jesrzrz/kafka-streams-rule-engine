package com.github.msalaslo.streamedrules;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.github.msalaslo.streamedrules.drools.DroolsRulesApplier;

/**
 * Runs the Kafka Streams job.
 */
public class SimpleKafkaStreamsRunner {

    private SimpleKafkaStreamsRunner() {
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
        KStream<byte[], String> inputData = builder.stream(inputTopic);
        KStream<byte[], String> outputData = inputData.mapValues(rulesApplier::applyRule);
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
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        return streamsConfiguration;
    }
}
