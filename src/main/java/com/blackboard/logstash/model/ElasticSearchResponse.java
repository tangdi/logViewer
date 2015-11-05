/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * ClassName: ElasticSearchResponse Function: TODO
 *
 * @Author: dtang
 * @Date: 11/3/15, 1:23 PM
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ElasticSearchResponse {
	private Hits hits;

	public Hits getHits() {
		return hits;
	}

	public void setHits(Hits hits) {
		this.hits = hits;
	}
}
