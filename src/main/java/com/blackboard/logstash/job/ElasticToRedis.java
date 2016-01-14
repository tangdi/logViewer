/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.job;

import java.nio.charset.Charset;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import javax.annotation.PostConstruct;

import akka.actor.ActorRef;
import com.blackboard.logstash.model.ElasticSearchResponse;
import com.blackboard.logstash.util.ObjectMapperUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

/**
 * ClassName: ElasticToRedis Function: TODO
 *
 * @Author: dtang
 * @Date: 11/5/15, 5:54 PM
 */
@Component
public class ElasticToRedis {
	private static final Logger LOG = LogManager.getLogger(ElasticToRedis.class);

	@Autowired private RestTemplate restTemplate;

	@Autowired private StringRedisTemplate redisTemplate;
	@Autowired private ActorRef logExtracter;
	@Autowired private List<ElasticCrawlJob> elasticCrawlJobList;

	private final ObjectMapper OBJECT_MAPPER = ObjectMapperUtil.getObjectMapper();

	private final int MAX_TIMES = 3;

	private final CountDownLatch DAILY_CRAWL = new CountDownLatch(1);

	@PostConstruct
	private void savehistoryData() {
		Thread thread = new Thread(() -> {
			ZonedDateTime start = ZonedDateTime.of(2016, 01, 10, 0, 0, 0, 0, ZoneOffset.UTC);
			ZonedDateTime end = ZonedDateTime.now(ZoneOffset.UTC).minusDays(2);
			LOG.warn("initialize: crawl history data if not in redis and elastic");
			LOG.warn("start date is {}, end date is {}", start.format(DateTimeFormatter.BASIC_ISO_DATE), end.format(DateTimeFormatter.BASIC_ISO_DATE));
			try {
				crawl(start, end, false, false);
			} catch (Throwable e) {
				LOG.error(e);
			}
			LOG.warn("initialize finished");
			DAILY_CRAWL.countDown();
		});
		thread.start();

	}

	//TODO modify to cronjob
	@Scheduled(fixedRate = 24 * 3600 * 1000)
	private void crawl() {
		ZonedDateTime start = ZonedDateTime.now(ZoneOffset.UTC).minusDays(1);
		try {
			DAILY_CRAWL.await();
		} catch (InterruptedException e) {
			LOG.error(e);
			return;
		}
		ZonedDateTime today = ZonedDateTime.now(ZoneOffset.UTC);
		LOG.warn("doing daily crawl, start date is {}, end date is {}", start.format(DateTimeFormatter.BASIC_ISO_DATE), today.format(DateTimeFormatter.BASIC_ISO_DATE));
		crawl(start, today, false, false);
	}

	public void crawl(ZonedDateTime startDate, ZonedDateTime endDate, boolean overwriteRedis, boolean overwriteElastic) {
		for (ElasticCrawlJob elasticCrawlJob : elasticCrawlJobList) {
			for (String type : elasticCrawlJob.getSourceTypes()) {
				List<ZonedDateTime> dates = new ArrayList<>();
				for (ZonedDateTime date = startDate; date.isBefore(endDate); date = date.plusDays(1)) {
					LOG.debug("elasticSource is is {}, log type is {}, index is {}", elasticCrawlJob.getSourceUniqueId(), type, elasticCrawlJob.getSourceIndex(date));
					if (crawl(date, type, elasticCrawlJob, overwriteRedis)) {
						dates.add(date);
					}else if(overwriteElastic){
						dates.add(date);
					}
					LOG.warn("skip index {} type {}", elasticCrawlJob.getSourceIndex(date), type);
				}
				ElasticPersistence elasticPersistence = new ElasticPersistence(logExtracter, restTemplate, redisTemplate);
				elasticPersistence.store(elasticCrawlJob, type, dates);
			}
		}
	}

	public boolean crawl(ZonedDateTime targetDate, String logType, ElasticCrawlJob elasticCrawlJob, boolean overwriteRedis) {
		boolean existing = existsInRedis(targetDate, logType, elasticCrawlJob);

		if (existing) {
			if (!overwriteRedis) {
				LOG.warn("target index {} type {} already exists in redis, skip overwrite redis", elasticCrawlJob.getSourceIndex(targetDate), logType);
				return false;
			} else {
				LOG.warn("target index {} type {} already exists in redis, delete it", elasticCrawlJob.getSourceIndex(targetDate), logType);
				deleteRedisKey(targetDate, elasticCrawlJob, logType);
			}
		}
		// TODO add error handling
		long starTime = System.currentTimeMillis();
		boolean sucessful = false;
		for (int i = 0; i < MAX_TIMES && (!sucessful); i++) {
			try {
				saveMappingInRedis(targetDate, elasticCrawlJob, logType);
				saveDataInRedis(targetDate, elasticCrawlJob, logType);
				sucessful = true;
			} catch (Throwable e) {
				LOG.error("there is error when crawling from Prod, error is: {}, retry if possible", e.getLocalizedMessage());
				deleteRedisKey(targetDate, elasticCrawlJob, logType);
			}
		}
		if (!sucessful) {
			LOG.error("crawing for index {} type {} failed", elasticCrawlJob.getSourceIndex(targetDate), logType);
			return false;
		}
		LOG.warn("crawing for index {} type {} success, take {} millis", elasticCrawlJob.getSourceIndex(targetDate), logType, System.currentTimeMillis() - starTime);

		// return true means we did crawl
		return true;
	}

	public static boolean isSuccess(ResponseEntity<?> responseEntity) {
		int statusCode = responseEntity.getStatusCode().value();
		return statusCode >= 200 && statusCode < 300;
	}

	public static String getRedisKey(ZonedDateTime date, ElasticCrawlJob elasticCrawlJob, String logType) {
		return elasticCrawlJob.getSourceUniqueId() + "|" + elasticCrawlJob.getSourceIndex(date) + "|" + logType + "|data";

	}

	public static String getMappingRedisKey(ZonedDateTime date, ElasticCrawlJob elasticCrawlJob, String logType) {
		return elasticCrawlJob.getSourceUniqueId() + "|" + elasticCrawlJob.getSourceIndex(date) + "|" + logType + "|mapping";
	}

	public boolean existsInRedis(ZonedDateTime date, String logType, ElasticCrawlJob elasticCrawlJob) {
		return redisTemplate.hasKey(getRedisKey(date, elasticCrawlJob, logType)) && redisTemplate.hasKey(getMappingRedisKey(date, elasticCrawlJob, logType));
	}

	public void deleteRedisKey(ZonedDateTime date, ElasticCrawlJob elasticCrawlJob, String logType) {
		List<String> keys = new ArrayList<>();
		keys.add(getRedisKey(date, elasticCrawlJob, logType));
		keys.add(getMappingRedisKey(date, elasticCrawlJob, logType));
		redisTemplate.delete(keys);
	}

	public void saveMappingInRedis(ZonedDateTime date, ElasticCrawlJob elasticCrawlJob, String logType) {
		String mappingRedisKey = getMappingRedisKey(date, elasticCrawlJob, logType);
		ResponseEntity<HashMap<String, HashMap<String, HashMap<String, Object>>>> responseEntity = restTemplate
				.exchange(elasticCrawlJob.getSourceUrl(date, logType) + "_mapping", HttpMethod.GET, elasticCrawlJob.getSourceRequestEntity(),
						new ParameterizedTypeReference<HashMap<String, HashMap<String, HashMap<String, Object>>>>() {
						});
		if (responseEntity != null && isSuccess(responseEntity)) {
			HashMap<String, HashMap<String, HashMap<String, Object>>> mapping = responseEntity.getBody();
			if (mapping.size() == 1) {
				HashMap<String, HashMap<String, Object>> details = mapping.values().stream().findFirst().get();
				Object typeMapping = details.get("mappings").get(logType);
				if (typeMapping != null) {
					String value = null;
					try {
						value = OBJECT_MAPPER.writeValueAsString(typeMapping);
					} catch (JsonProcessingException e) {
						LOG.error(e);
					}
					if(value == null){
						return;
					}
					redisTemplate.opsForValue().set(mappingRedisKey, value);
					return;
				}

			}
			LOG.error("mapping query has more than one index");

		}
		LOG.error("index {}, type {} has error response for mapping query", elasticCrawlJob.getSourceIndex(date), logType);
		throw new RuntimeException("error mapping query response");

	}

	public void saveDataInRedis(ZonedDateTime date, ElasticCrawlJob elasticCrawlJob, String logType) {
		long size = 300;
		ResponseEntity<ElasticSearchResponse> responseEntity;
		//TODO current beast Elastic does not support Scan and Scroll, which is performance optimal

		String host = elasticCrawlJob.getSourceUrl(date, logType);
		try {
			responseEntity = restTemplate
					.exchange(host + "_search?preference=xyzabc123&size={size}", HttpMethod.POST, elasticCrawlJob.getSourceRequestEntity(), new ParameterizedTypeReference<ElasticSearchResponse>() {
					}, 0);
		} catch (HttpClientErrorException e) {
			LOG.error(e.getResponseBodyAsString());
			throw e;
		}
		Long totalCount;

		if (responseEntity != null && isSuccess(responseEntity)) {
			//TODO potential NPE
			totalCount = responseEntity.getBody().getHits().getTotal();
			LOG.debug("{} {} has {} log events", elasticCrawlJob.getSourceIndex(date), logType, totalCount);

			String redisKey = getRedisKey(date, elasticCrawlJob, logType);
			long accu = 0;
			ConcurrentMap<String, Long> map = new ConcurrentHashMap<>();
			while (true) {
				responseEntity = restTemplate.exchange(host + "_search?preference=xyzabc123&from={accu}&size={size}", HttpMethod.POST, elasticCrawlJob.getSourceRequestEntity(),
						new ParameterizedTypeReference<ElasticSearchResponse>() {
						}, accu, size);
				long hitsSize = responseEntity.getBody().getHits().getHits().size();
				if (hitsSize == 0) {
					break;
				}
				System.out.println(accu);
				responseEntity.getBody().getHits().getHits().parallelStream().forEach(hit -> {
					String value = null;
					try {
						value = OBJECT_MAPPER.writeValueAsString(hit);
					} catch (JsonProcessingException e) {
						LOG.error(e);
					}
					if(value == null){
						return;
					}
					redisTemplate.opsForList().rightPush(redisKey, value);
					return;
				});
				accu += hitsSize;
			}
		}
	}

	public long getSizePerShard(long totalSize, ZonedDateTime date) {
		//TODO add size per shard support
		return totalSize;
	}
}
