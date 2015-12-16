/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.job;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.blackboard.logstash.model.Hit;
import com.blackboard.logstash.parser.*;
import com.blackboard.logstash.parser.Scanner;
import com.blackboard.logstash.util.ObjectMapperUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisTemplate;
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

	private final RestTemplate restTemplate;
	private final RedisTemplate<String, Object> redisTemplate;

	private final DateTimeFormatter cloudElasticDateFormat;

	private final DateTimeFormatter localElasticDateFormat;

	private final List<ZonedDateTime> dates;

	private final ObjectMapper OBJECT_MAPPER = ObjectMapperUtil.getObjectMapper();

	private final String logType;
	private static final String HOST = "http://localhost:9200";

	public ElasticPersistence(RestTemplate restTemplate, RedisTemplate<String, Object> redisTemplate, DateTimeFormatter cloudElasticDateFormat, DateTimeFormatter localElasticDateFormat,
			List<ZonedDateTime> dates, String logType) {
		this.restTemplate = restTemplate;
		this.redisTemplate = redisTemplate;
		this.cloudElasticDateFormat = cloudElasticDateFormat;
		this.localElasticDateFormat = localElasticDateFormat;
		this.logType = logType;
		this.dates = dates;
	}

	public void store() {
		if (CollectionUtils.isEmpty(dates)) {
			LOG.debug("no dates to process");
			return;
		}
		ActorSystem system = ActorSystem.create("logApp");
		Filter filter = new Filter();

		//filter to get device platform from user agent
		filter.add("request.headers.agent", "%{GREEDY}%{PLATFORM:platform}%{GREEDY}?", null, null);
		ActorRef storageMaster = system.actorOf(MasterHandler.createMasterProp(20, EventStorageHandler.class, restTemplate, HOST), "storage");
		ActorRef reaper = system.actorOf(Props.create(ActorReaper.class));
		ActorRef extractMaster = system.actorOf(MasterHandler.createMasterProp(10, EventExtractHandler.class, filter, storageMaster), "extract");

		reaper.tell(new ActorReaper.WatchMe(storageMaster), ActorRef.noSender());
		reaper.tell(new ActorReaper.WatchMe(extractMaster), ActorRef.noSender());

		for (ZonedDateTime date : dates) {
			String index = generateDestIndex(date);
			String redisKey = ElasticToRedis.getRedisKey(generateSrcDateString(date), logType);

			Long totalCount = redisTemplate.execute((RedisConnection connection) -> {
				return connection.lLen(redisKey.getBytes(ElasticToRedis.UTF_8_CHARSET));
			});

			if (totalCount == 0L) {
				LOG.debug("redis key {} contains no element", redisKey);
				continue;
			}
			createIndexType(date, logType, HOST);
			LOG.debug("send log from redis key {} to dest, total log event count is {}", ElasticToRedis.getRedisKey(generateSrcDateString(date), logType), totalCount);

			long size = 600;
			long from = 0;
			Set<String> set = new HashSet<>();

			while (from < totalCount) {
				final long rfrom = from;
				final long rsize = size;
				List<byte[]> rawByteList = redisTemplate.execute((RedisConnection connection) -> {
					return connection.lRange(redisKey.getBytes(ElasticToRedis.UTF_8_CHARSET), rfrom, rfrom + rsize);
				});
				from += rawByteList.size();
				List<Hit> hitList = new ArrayList<>();
				for (byte[] bytes : rawByteList) {
					try {
						Hit hit = ObjectMapperUtil.getObjectMapper().readValue(bytes, Hit.class);
						hitList.add(hit);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}

				hitList.stream().forEach(hit -> {
					Event event = generateEvent(hit, index);
//					if(set.contains(event.id)){
//						System.out.println("repeat " +event.id);
//					}else{
//						set.add(event.id);
//					}
//					if("AVGn5PatoFXRq9Wj9KAL".equals(event.id)){
//						System.out.println(event.fields);
//
//					}
					extractMaster.tell(event, ActorRef.noSender());
				});

			}
		}
		storageMaster.tell(Scanner.FINISH_WORK, ActorRef.noSender());
		// wait for actor system to terminate
		system.awaitTermination();
		LOG.error("actor system is terminated");

	}

	public Event generateEvent(Hit hit, String index) {
		Event event = new Event(hit.getSource(), index, logType, hit.getId());
		return event;
	}

	public String generateDestIndex(ZonedDateTime date) {
		return "log." + generateDestDateString(date);
	}

	public String generateSrcDateString(ZonedDateTime date) {
		return cloudElasticDateFormat.format(date);
	}

	public String generateDestDateString(ZonedDateTime date) {
		return localElasticDateFormat.format(date);
	}

	public void createIndexType(ZonedDateTime date, String logType, String host) {
		boolean indexExist = false;
		try {
			ResponseEntity<Void> responseEntity = restTemplate.exchange(host + "/{index}", HttpMethod.GET, null, new ParameterizedTypeReference<Void>() {
			}, generateDestIndex(date));
			indexExist = responseEntity != null && ElasticToRedis.isSuccess(responseEntity);
		} catch (HttpClientErrorException e) {
		}

		if (indexExist) {
			LOG.warn("index exists, delete type");
			try {
				restTemplate.delete(host + "/{index}/{type}", generateDestIndex(date), logType);
			} catch (HttpClientErrorException e) {
				LOG.error("{} log does not exists in {}", logType, generateDestIndex(date));
			}

		} else {
			LOG.warn("index does not exist, create index");
			try {
				restTemplate.put(host + "/{index}", null, generateDestIndex(date));
			} catch (Throwable e) {
				LOG.error(e.getMessage());
				throw new RuntimeException(e);
			}
		}
		byte[] rawMappingValue = redisTemplate.execute((RedisConnection connection) -> {
			return connection.get(ElasticToRedis.getMappingRedisKey(generateSrcDateString(date), logType).getBytes(ElasticToRedis.UTF_8_CHARSET));

		});
		try {
			HashMap<String, Object> mapping = OBJECT_MAPPER.readValue(rawMappingValue, HashMap.class);
			HttpEntity<HashMap> httpEntity = new HttpEntity<>(mapping);
			restTemplate.exchange(host + "/{index}/{type}/_mapping", HttpMethod.PUT, httpEntity, new ParameterizedTypeReference<Void>() {
			}, generateDestIndex(date), logType);
		} catch (Throwable e) {
			LOG.error(e);
			throw new RuntimeException(e);
		}
	}

}
