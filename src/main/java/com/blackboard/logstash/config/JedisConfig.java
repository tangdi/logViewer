/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import redis.clients.jedis.JedisPoolConfig;

/**
 * ClassName: JedisConfig Function: TODO
 *
 * @Author: dtang
 * @Date: 11/5/15, 5:45 PM
 */
@Configuration
public class JedisConfig {
	@Bean
	@Autowired
	public RedisTemplate redisTemplate(JedisConnectionFactory jedisConnectionFactory){
		RedisTemplate redisTemplate = new RedisTemplate();
		redisTemplate.setConnectionFactory(jedisConnectionFactory);
		return redisTemplate;
	}

	@Bean
	public JedisPoolConfig jedisPoolConfig(){
		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
		jedisPoolConfig.setMaxTotal(20);
		jedisPoolConfig.setMaxIdle(10);
		jedisPoolConfig.setMaxWaitMillis(10000);
		jedisPoolConfig.setTestOnBorrow(true);
		return jedisPoolConfig;
	}

	@Bean
	@Autowired
	public JedisConnectionFactory jedisConnectionFactory(JedisPoolConfig jedisPoolConfig){
		JedisConnectionFactory jedisConnectionFactory = new JedisConnectionFactory();
		jedisConnectionFactory.setPoolConfig(jedisPoolConfig);
		jedisConnectionFactory.setHostName("localhost");
		jedisConnectionFactory.setPort(6379);
		return jedisConnectionFactory;
	}
}
