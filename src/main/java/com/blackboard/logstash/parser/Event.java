package com.blackboard.logstash.parser;

import java.util.Collections;
import java.util.Map;

public class Event {
	public final Map<String, Object> fields;
	public final String host;
	public final String index;
	public final String type;
	public final String id;

	public Event(Map<String, Object> fields, String host, String index, String type) {
		this(fields, host, index, type, null);
	}

	public Event(Map<String, Object> fields, String host, String index, String type, String id) {
		this.fields = Collections.unmodifiableMap(fields);
		this.host = host;
		this.index = index;
		this.type = type;
		this.id = id;
	}

}
