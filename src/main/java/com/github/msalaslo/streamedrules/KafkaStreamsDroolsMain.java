package com.github.msalaslo.streamedrules;

import java.io.IOException;
import java.util.Properties;

import com.github.msalaslo.streamedrules.configuration.PropertiesUtil;

public class KafkaStreamsDroolsMain {

    public static void main(String[] args) {
        Properties properties;
		try {
			properties = PropertiesUtil.loadProperties("config.properties");
			KafkaStreamsRunner.runKafkaStream(properties);
		} catch (IOException e) {
			e.printStackTrace();
		}
        
    }
}
