/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * ClassName: Hits Function: TODO
 *
 * @Author: dtang
 * @Date: 11/3/15, 1:24 PM
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Hits {
	private Long total;


	private List<Hit> hits;

	public Long getTotal() {
		return total;
	}

	public void setTotal(Long total) {
		this.total = total;
	}

	public List<Hit> getHits() {
		return hits;
	}

	public void setHits(List<Hit> hits) {
		this.hits = hits;
	}
}
