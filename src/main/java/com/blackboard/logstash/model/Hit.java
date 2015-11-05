/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.model;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * ClassName: Hit Function: TODO
 *
 * @Author: dtang
 * @Date: 11/3/15, 1:25 PM
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Hit {
	@JsonProperty("_index")
	private String index;

	@JsonProperty("_type")
	private String type;

	@JsonProperty("_id")
	private String id;

	@JsonProperty("_source")
	private Map<String, Object> source;

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Map<String, Object> getSource() {
		return source;
	}

	public void setSource(Map<String, Object> source) {
		this.source = source;
	}

	@Override
	public String toString() {
		return "Hit{" +
				"index='" + index + '\'' +
				", type='" + type + '\'' +
				", id='" + id + '\'' +
				", source=" + source +
				'}';
	}
}
