package com.github.msalaslo.streamedrules.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {

	public static Properties loadProperties(String fileName) throws IOException {
		InputStream input = PropertiesUtil.class.getClassLoader().getResourceAsStream(fileName);
		Properties prop = new Properties();
		if (input == null) {
			throw new IOException("Sorry, unable to find " + fileName);
		}
		prop.load(input);
		return prop;
	}

}
