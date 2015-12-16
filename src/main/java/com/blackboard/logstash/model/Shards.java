/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * ClassName: Shards Function: TODO
 *
 * @Author: dtang
 * @Date: 12/16/15, 3:30 PM
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Shards {
	private int total;
	private int successful;
	private int failed;

	public int getTotal() {
		return total;
	}

	public void setTotal(int total) {
		this.total = total;
	}

	public int getSuccessful() {
		return successful;
	}

	public void setSuccessful(int successful) {
		this.successful = successful;
	}

	public int getFailed() {
		return failed;
	}

	public void setFailed(int failed) {
		this.failed = failed;
	}
}
