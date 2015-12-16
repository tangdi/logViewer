/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * ClassName: ElasticSearchResponse Function: TODO
 *
 * @Author: dtang
 * @Date: 11/3/15, 1:23 PM
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ElasticSearchResponse {
	private Hits hits;

	@JsonProperty("_scroll_id") private String scollId;

	public Hits getHits() {
		return hits;
	}

	public void setHits(Hits hits) {
		this.hits = hits;
	}

	public String getScollId() {
		return scollId;
	}

	public void setScollId(String scollId) {
		this.scollId = scollId;
	}

}
