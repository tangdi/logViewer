/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.job;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.*;

import akka.actor.ActorRef;
import com.blackboard.logstash.model.Hit;
import com.blackboard.logstash.parser.Event;
import com.blackboard.logstash.parser.Filter;
import com.blackboard.logstash.util.ObjectMapperUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

/**
 * ClassName: ElasticPersistence Function: TODO
 *
 * @Author: dtang
 * @Date: 11/3/15, 1:31 PM
 */

public class ElasticPersistence {

	private static final Logger LOG = LogManager.getLogger(ElasticPersistence.class);
	private final ObjectMapper OBJECT_MAPPER = ObjectMapperUtil.getObjectMapper();
	private final ActorRef accessLogExtracter;
	private final RestTemplate restTemplate;
	private final StringRedisTemplate redisTemplate;

	public ElasticPersistence(ActorRef accessLogExtracter, RestTemplate restTemplate, StringRedisTemplate redisTemplate) {
		this.accessLogExtracter = accessLogExtracter;
		this.restTemplate = restTemplate;
		this.redisTemplate = redisTemplate;
	}

	public void store(ElasticCrawlJob elasticCrawlJob, String logType, List<ZonedDateTime> dates) {
		if (CollectionUtils.isEmpty(dates)) {
			LOG.debug("no dates to process");
			return;
		}
		try {

			for (ZonedDateTime date : dates) {
				String index = elasticCrawlJob.getDestinationIndex(date);
				String redisKey = ElasticToRedis.getRedisKey(date, elasticCrawlJob, logType);

				Long totalCount = redisTemplate.opsForList().size(redisKey);

				if (totalCount == 0L) {
					LOG.debug("redis key {} does not exist or contains no element", redisKey);
					continue;
				}
				createIndexType(date, elasticCrawlJob, logType);
				LOG.debug("send log from redis key {} to dest, total log event count is {}", redisKey, totalCount);

				long size = 600;
				long from = 0;
				while (from < totalCount) {
					final long rfrom = from;
					final long rsize = size;
					List<String> values= redisTemplate.opsForList().range(redisKey, rfrom, rfrom + rsize);
					from += values.size();
					List<Hit> hitList = new ArrayList<>();
					for (String value: values) {
						try {
							Hit hit = ObjectMapperUtil.getObjectMapper().readValue(value, Hit.class);
							hitList.add(hit);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

					hitList.stream().forEach(hit -> {
						Event event = generateEvent(hit, elasticCrawlJob.getDestinationHost(), index, logType, elasticCrawlJob.getFilter().orElse(null));
						accessLogExtracter.tell(event, ActorRef.noSender());
					});
				}
				LOG.debug("success");
			}
			// wait for actor system to terminate
		} catch (Throwable e) {
			LOG.error(e);
		} finally {
		}
	}

	public Event generateEvent(Hit hit, String host, String index, String logType, Filter filter) {
		Event event = new Event(hit.getSource(), host, index, logType, hit.getId(), filter);
		return event;
	}

	public void createIndexType(ZonedDateTime date, ElasticCrawlJob elasticCrawlJob, String logType) {
		boolean indexExist = false;
		try {
			ResponseEntity<Void> responseEntity = restTemplate.exchange(elasticCrawlJob.getDestinationUrl(date), HttpMethod.GET, null, new ParameterizedTypeReference<Void>() {
			});
			indexExist = responseEntity != null && ElasticToRedis.isSuccess(responseEntity);
		} catch (HttpClientErrorException e) {
		}

		if (indexExist) {
			LOG.warn("index exists, delete type");
			try {
				restTemplate.delete(elasticCrawlJob.getDestinationUrl(date, logType));
			} catch (HttpClientErrorException e) {
				LOG.error("type {} does not exists in index {}", logType, elasticCrawlJob.getDestinationIndex(date));
			}

		} else {
			LOG.warn("index does not exist, create index");
			try {
				restTemplate.put(elasticCrawlJob.getDestinationUrl(date), null);
			} catch (Throwable e) {
				LOG.error(e.getMessage());
				throw new RuntimeException(e);
			}
		}
		String mappingValue = redisTemplate.opsForValue().get(ElasticToRedis.getMappingRedisKey(date, elasticCrawlJob, logType));
		try {
			HashMap<String, Object> mapping = OBJECT_MAPPER.readValue(mappingValue, HashMap.class);
			HttpEntity<HashMap> httpEntity = new HttpEntity<>(mapping);
			restTemplate.exchange(elasticCrawlJob.getDestinationUrl(date, logType) + "_mapping", HttpMethod.PUT, httpEntity, new ParameterizedTypeReference<Void>() {
			});
		} catch (Throwable e) {
			LOG.error(e);
			throw new RuntimeException(e);
		}
	}

}
