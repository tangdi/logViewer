/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.job;

import java.nio.charset.Charset;
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

	@Autowired private RedisTemplate<String, Object> redisTemplate;
	@Autowired private ActorRef logExtracter;
	@Autowired private List<ElasticCrawlJob> elasticCrawlJobList;

	private final ObjectMapper OBJECT_MAPPER = ObjectMapperUtil.getObjectMapper();

	public static final Charset UTF_8_CHARSET = Charset.forName("UTF-8");

	private final int MAX_TIMES = 3;

	private final CountDownLatch DAILY_CRAWL = new CountDownLatch(1);

	@PostConstruct
	private void saveLastMonthData() {
		Thread thread = new Thread(() -> {
			LOG.warn("initialize: crawl last month data if not in redis and elastic");
			try {
				ZonedDateTime end = ZonedDateTime.now(ZoneOffset.UTC).minusDays(1);
				ZonedDateTime start = end.minusDays(6);
				crawl(start, end, false, false);
			} catch (Throwable e) {
				LOG.error(e);
			}
			LOG.warn("initialize finished");
			DAILY_CRAWL.countDown();
		});
		thread.start();

	}

	@Scheduled(fixedRate = 24 * 3600 * 1000)
	private void crawl() {
		ZonedDateTime today = ZonedDateTime.now(ZoneOffset.UTC);
		try {
			DAILY_CRAWL.await();
		} catch (InterruptedException e) {
			LOG.error(e);
			return;
		}
		ZonedDateTime start = today.minusDays(1);
		LOG.warn("crawl one day {} data", start.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));

		//TODO fill the real elastic host
		//		String host = "https://mbaas-prod-elk-664782521.us-east-1.elb.amazonaws.com/elasticsearch/logstash-{date}/{LOG_TYPE}/";

		crawl(start, today, false, true);
	}

	public void crawl(ZonedDateTime startDate, ZonedDateTime endDate, boolean overwriteRedis, boolean overwriteElastic) {
		List<ZonedDateTime> dates = new ArrayList<>();
		for (ElasticCrawlJob elasticCrawlJob : elasticCrawlJobList) {
			for (String type : elasticCrawlJob.getSourceTypes()) {
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
		boolean existing = redisTemplate.execute((RedisConnection connection) -> {
			return connection.exists(getRedisKey(date, elasticCrawlJob, logType).getBytes(UTF_8_CHARSET));
		}) && redisTemplate.execute((RedisConnection connection) -> {
			return connection.exists(getMappingRedisKey(date, elasticCrawlJob, logType).getBytes(UTF_8_CHARSET));
		});
		return existing;
	}

	public void deleteRedisKey(ZonedDateTime date, ElasticCrawlJob elasticCrawlJob, String logType) {
		redisTemplate.execute((RedisConnection connection) -> {
			return connection.del(getRedisKey(date, elasticCrawlJob, logType).getBytes(UTF_8_CHARSET));
		});
		redisTemplate.execute((RedisConnection connection) -> {
			return connection.del(getMappingRedisKey(date, elasticCrawlJob, logType).getBytes(UTF_8_CHARSET));
		});
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
					redisTemplate.execute((RedisConnection connection) -> {
						String value = null;
						try {
							value = OBJECT_MAPPER.writeValueAsString(typeMapping);
						} catch (JsonProcessingException e) {
							LOG.error(e);
						}
						if (value == null) {
							return null;
						}
						connection.set(mappingRedisKey.getBytes(UTF_8_CHARSET), value.getBytes(UTF_8_CHARSET));
						return null;
					});
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
		//TODO current beast does not support Scan and Scroll, which is performance optimal

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
					redisTemplate.execute((RedisConnection connection) -> {
						String value = null;
						try {
							value = OBJECT_MAPPER.writeValueAsString(hit);
						} catch (JsonProcessingException e) {
							e.printStackTrace();
						}
						if (value == null) {
							return null;
						}
						connection.rPush(redisKey.getBytes(UTF_8_CHARSET), value.getBytes(UTF_8_CHARSET));
						return null;
					});
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
