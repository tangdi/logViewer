/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.util;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * ClassName: ObjectMapperUtil Function: TODO
 *
 * @Author: dtang
 * @Date: 11/6/15, 10:49 AM
 */
public class ObjectMapperUtil {
	private static ObjectMapper instance;

	public static ObjectMapper getObjectMapper() {
		if (instance == null) {
			synchronized (ObjectMapperUtil.class) {
				if (instance == null) {
					instance = new ObjectMapper();
				}
			}
		}
		return instance;
	}
}
