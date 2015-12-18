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
import java.util.Map;
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
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.Base64Utils;
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

	private final ObjectMapper OBJECT_MAPPER = ObjectMapperUtil.getObjectMapper();

	public static final Charset UTF_8_CHARSET = Charset.forName("UTF-8");

	private DateTimeFormatter cloudElasticDateFormat;

	private DateTimeFormatter localElasticDateFormat;

	private String host = "https://telemetry-prod-bastion01.cloud.bb";

	private final int MAX_TIMES = 3;

	private final String LOG_TYPE = "AccessLog";

	private final CountDownLatch DAILY_CRAWL = new CountDownLatch(1);

	public ElasticToRedis() {
		cloudElasticDateFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
		//		cloudElasticDateFormat = DateTimeFormatter.ofPattern("yyyy.MM.dd");

		localElasticDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");

	}

	@PostConstruct
	private void saveLastMonthData() {
		LOG.warn("initialize: crawl last month data if not in redis and elastic");
		try {
			ZonedDateTime end = ZonedDateTime.now(ZoneOffset.UTC);
			end = end.minusDays(1);
			ZonedDateTime start = end.minusDays(6);
			crawl(start, end, LOG_TYPE, false, false);
		} catch (Throwable e) {
			LOG.error(e);
		}
		LOG.warn("initialize finished");
		DAILY_CRAWL.countDown();

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
		LOG.warn("crawl one day {} data", generateDateString(start));

		//TODO fill the real elastic host
		//		String host = "https://mbaas-prod-elk-664782521.us-east-1.elb.amazonaws.com/elasticsearch/logstash-{date}/{LOG_TYPE}/";

		crawl(start, today, LOG_TYPE, false, true);
	}

	public void crawl(ZonedDateTime startDate, ZonedDateTime endDate, String logType, boolean overwriteRedis, boolean overwriteElastic) {
		List<ZonedDateTime> dates = new ArrayList<>();
		for (ZonedDateTime date = startDate; date.isBefore(endDate); date = date.plusDays(1)) {
			LOG.debug("host is {}, log type is {}, index is {}", host, logType, generateSrcIndex(date));
			if (crawl(date, logType, overwriteRedis) || overwriteElastic) {
				dates.add(date);
			}
			LOG.warn("skip {} since it already exists", generateDateString(date));
		}
		ElasticPersistence elasticPersistence = new ElasticPersistence(logExtracter, cloudElasticDateFormat, localElasticDateFormat, restTemplate, redisTemplate);
		elasticPersistence.store(logType, dates);

	}

	public boolean crawl(ZonedDateTime targetDate, String logType, boolean overwriteRedis) {
		boolean existing = existsInRedis(targetDate, logType);
		String dateString = generateDateString(targetDate);

		if (existing) {
			if (!overwriteRedis) {
				LOG.warn("target Date {} already exists in redis, skip overwrite", dateString);
				return false;
			} else {
				LOG.warn("target Date {} already exists in redis, delete it", dateString);
				deleteRedisKey(targetDate, logType);
			}
		}

		//		String body = "{ \"query\" : { \"match_all\" : {} } }";
		String body = "{\"query\": { \"match_all\": {} }, \"fields\": [\"*\"], \"_source\": true }";
		//		String body = "{ \"query\" : { \"match\" : { \"message\" : \"No matched institution found for institution\" } } }";
		if (!host.endsWith("/")) {
			host += "/";
		}
		// TODO add error handling
		long starTime = System.currentTimeMillis();
		boolean sucessful = false;
		for (int i = 0; i < MAX_TIMES; i++) {
			try {
				saveMappingInRedis(targetDate, logType, host, body);
				saveDataInRedis(targetDate, logType, host, body, "10m");
			} catch (Throwable e) {
				LOG.error("there is error when crawling from Prod, error is: {}, retry if possible", e.getLocalizedMessage());
				deleteRedisKey(targetDate, logType);
				continue;
			}
			sucessful = true;
			break;
		}
		if (!sucessful) {
			LOG.error("crawing for date {} failed", generateDateString(targetDate));
			return false;
		}
		LOG.warn("crawing for date {} success, take {} millis", generateDateString(targetDate), System.currentTimeMillis() - starTime);

		// return true means we did crawl
		return true;
	}

	public String generateAuthEncoded() {
		//TODO fill the real user password
				String user = "";
				String password = "";

		String authEncoded = new String(Base64Utils.encode((user + ":" + password).getBytes()));
		return authEncoded;
	}

	public static boolean isSuccess(ResponseEntity<?> responseEntity) {
		int statusCode = responseEntity.getStatusCode().value();
		return statusCode >= 200 && statusCode < 300;
	}

	public static String getRedisKey(String dateString, String logType) {
		return "indexDate:" + dateString + "_type:" + logType + "_data";

	}

	public static String getMappingRedisKey(String dateString, String logType) {
		return "indexDate:" + dateString + "_type:" + logType + "_mapping";
	}

	public boolean existsInRedis(ZonedDateTime date, String logType) {
		String dateString = generateDateString(date);
		boolean existing = redisTemplate.execute((RedisConnection connection) -> {
			return connection.exists(getRedisKey(dateString, logType).getBytes(UTF_8_CHARSET));
		}) && redisTemplate.execute((RedisConnection connection) -> {
			return connection.exists(getMappingRedisKey(dateString, logType).getBytes(UTF_8_CHARSET));
		});
		return existing;
	}

	public void deleteRedisKey(ZonedDateTime date, String logType) {
		String dateString = generateDateString(date);
		redisTemplate.execute((RedisConnection connection) -> {
			return connection.del(getRedisKey(dateString, logType).getBytes(UTF_8_CHARSET));
		});
		redisTemplate.execute((RedisConnection connection) -> {
			return connection.del(getMappingRedisKey(dateString, logType).getBytes(UTF_8_CHARSET));
		});
	}

	public void saveMappingInRedis(ZonedDateTime date, String logType, String host, String queryBody) {
		String dateString = generateDateString(date);
		String mappingRedisKey = getMappingRedisKey(dateString, logType);
		ResponseEntity<HashMap<String, HashMap<String, HashMap<String, Object>>>> responseEntity = restTemplate
				.exchange(host + "/{index}/{type}/_mapping", HttpMethod.GET, generateHttpEntityWithMap(queryBody),
						new ParameterizedTypeReference<HashMap<String, HashMap<String, HashMap<String, Object>>>>() {
						}, generateSrcIndex(date), logType);
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
		LOG.error("date {}, type {} has error response for mapping query", dateString, logType);
		throw new RuntimeException("error mapping query response");

	}

	public HttpEntity generateHttpEntityWithMap(String queryBody) {
		Map<String, Object> bodyMap = null;
		try {

			bodyMap = ObjectMapperUtil.getObjectMapper().readValue(queryBody, HashMap.class);
		} catch (Throwable e) {
			LOG.error(e);
		}
		HttpHeaders headers = new HttpHeaders();
		headers.set("Authorization", "Basic " + generateAuthEncoded());
		HttpEntity httpEntity = new HttpEntity(bodyMap, headers);
		return httpEntity;
	}

	public HttpEntity generateHttpEntityWithString(String queryBody) {
		HttpHeaders headers = new HttpHeaders();
		headers.set("Authorization", "Basic " + generateAuthEncoded());
		HttpEntity<String> httpEntity = new HttpEntity(queryBody, headers);
		return httpEntity;
	}

	public String generateSrcIndex(ZonedDateTime date) {
		return "event." + generateDateString(date);
	}

	public String generateDateString(ZonedDateTime date) {
		return cloudElasticDateFormat.format(date);

	}

	public void saveDataInRedis(ZonedDateTime date, String logType, String host, String queryBody, String scrollTime) {
		long size = 300;
		ResponseEntity<ElasticSearchResponse> responseEntity;
		String index = generateSrcIndex(date);
		//TODO current beast does not support Scan and Scroll, which is performance optimal
		//		try {
		//			responseEntity = restTemplate.exchange(host + "/{index}/{type}/_search?size={size}&&search_type=scan&scroll={scrollTime}", HttpMethod.POST, generateHttpEntityWithMap(queryBody),
		//					new ParameterizedTypeReference<ElasticSearchResponse>() {
		//					}, index, LOG_TYPE, getSizePerShard(size, date), scrollTime);
		//		} catch (HttpClientErrorException e) {
		//			LOG.error(e.getResponseBodyAsString());
		//			throw e;
		//		}
		try {
			responseEntity = restTemplate.exchange(host + "/{index}/{type}/_search?preference=xyzabc123&size={size}", HttpMethod.POST, generateHttpEntityWithMap(queryBody),
					new ParameterizedTypeReference<ElasticSearchResponse>() {
					}, index, logType, 0);
		} catch (HttpClientErrorException e) {
			LOG.error(e.getResponseBodyAsString());
			throw e;
		}
		Long totalCount;

		if (responseEntity != null && isSuccess(responseEntity)) {
			//TODO potential NPE
			totalCount = responseEntity.getBody().getHits().getTotal();
			LOG.debug("{} {} has {} log events", index, logType, totalCount);

			String redisKey = getRedisKey(generateDateString(date), logType);
			long accu = 0;
			ConcurrentMap<String, Long> map = new ConcurrentHashMap<>();
			while (true) {
				responseEntity = restTemplate.exchange(host + "/{index}/{type}/_search?preference=xyzabc123&from={accu}&size={size}", HttpMethod.POST, generateHttpEntityWithMap(queryBody),
						new ParameterizedTypeReference<ElasticSearchResponse>() {
						}, index, logType, accu, size);
				long hitsSize = responseEntity.getBody().getHits().getHits().size();
				if (hitsSize == 0) {
					break;
				}
				System.out.println(accu);
				long current_accu = accu;
				responseEntity.getBody().getHits().getHits().parallelStream().forEach(hit -> {
					Long val = map.putIfAbsent(hit.getId(), current_accu);
					if (val != null) {
						LOG.error("duplicate ids!. current accu is {}, prev accu is {}, id is {}", current_accu, val, hit.getId());
					}
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
