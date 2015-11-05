package com.blackboard.logstash.parser;

import java.io.IOException;
import java.util.Properties;

public class LogPattern {
	public static Properties properties;

	static {
		properties = new Properties();
		try {
			properties.load(LogPattern.class.getClassLoader().getResourceAsStream("pattern.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String get(String patternName) {
		return properties.getProperty(patternName);
	}

	public static void main(String[] args) {
		System.out.println(get("GREEDY"));
	}

}
