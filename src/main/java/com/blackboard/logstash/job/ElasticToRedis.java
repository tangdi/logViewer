/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.job;

import java.nio.charset.Charset;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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

	private final boolean OVERWRITE_REDIS = true;
	private final boolean OVERWRITE_ELASTIC = true;

	private final ObjectMapper OBJECT_MAPPER = ObjectMapperUtil.getObjectMapper();
	public static final Charset UTF_8_CHARSET = Charset.forName("UTF-8");

	private DateTimeFormatter cloudElasticDateFormat;

	private DateTimeFormatter localElasticDateFormat;

	private String host = "https://telemetry-prod-bastion01.cloud.bb";

	public ElasticToRedis() {
		TimeZone gmt = TimeZone.getTimeZone("GMT");
		cloudElasticDateFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
		//		cloudElasticDateFormat = DateTimeFormatter.ofPattern("yyyy.MM.dd");

		localElasticDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");

	}

	@Scheduled(fixedDelay = 24 * 3600 * 1000)
	public void crawl() {

		//TODO fill the real elastic host
		final String logType = "AccessLog";
		//		String logType = "mbaas-rolling";
		//		String host = "https://mbaas-prod-elk-664782521.us-east-1.elb.amazonaws.com/elasticsearch/logstash-{date}/{logType}/";

		ZonedDateTime today = ZonedDateTime.now(ZoneOffset.UTC);
		//		ZonedDateTime today = ZonedDateTime.of(2015, 11, 22, 0, 0, 0, 0, ZoneOffset.UTC);
		//		ZonedDateTime today = ZonedDateTime.of(2015, 11, 18, 0, 0, 0, 0, ZoneOffset.UTC);
		//		ZonedDateTime start = ZonedDateTime.of(2015, 11, 21, 0, 0, 0, 0, ZoneOffset.UTC);
		crawl(today.minusDays(1), today, logType);
	}

	public void crawl(ZonedDateTime startDate, ZonedDateTime endDate, String logType) {
		List<ZonedDateTime> dates = new ArrayList<>();
		for (ZonedDateTime date = startDate; date.isBefore(endDate); date = date.plusDays(1)) {
			LOG.debug("host is {}, log type is {}, index is {}", host, logType, generateSrcIndex(date));
			if (crawl(date, logType) || OVERWRITE_ELASTIC) {
				dates.add(date);
			}
		}
		ElasticPersistence crawler = new ElasticPersistence(restTemplate, redisTemplate, cloudElasticDateFormat, localElasticDateFormat, dates, logType);
		crawler.store();

	}

	public boolean crawl(ZonedDateTime targetDate, String logType) {
		boolean existing = existsInRedis(targetDate, logType);

		if (existing) {
			if (!OVERWRITE_REDIS) {
				LOG.warn("target Date {} already exists in redis, skip overwrite", targetDate);
				return false;
			} else {
				LOG.warn("target Date {} already exists in redis, delete it", targetDate);
				deleteRedisKey(targetDate, logType);
			}
		}

		//		String body = "{ \"query\" : { \"match_all\" : {} } }";
		String body = "{\"query\": { \"match_all\": {} }, \"fields\": [\"*\"], \"_source\": true }";
		//		String body = "{ \"query\" : { \"match\" : { \"message\" : \"No matched institution found for institution\" } } }";
		if (!host.endsWith("/")) {
			host += "/";
		}
		saveMappingInRedis(targetDate, logType, host, body);
		saveDataInRedis(targetDate, logType, host, body, "10m");
		// return true means we did crawl
		return true;
	}

	public String generateAuthEncoded() {
		//TODO fill the real user password
		String user = "kwang";
		String password = "Wdsaowp";
		//    String user = "kibanaadmin";
		//    String password = "Black@1";
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
		}) || redisTemplate.execute((RedisConnection connection) -> {
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
		//		try {
		//			responseEntity = restTemplate.exchange(host + "/{index}/{type}/_search?size={size}&&search_type=scan&scroll={scrollTime}", HttpMethod.POST, generateHttpEntityWithMap(queryBody),
		//					new ParameterizedTypeReference<ElasticSearchResponse>() {
		//					}, index, logType, getSizePerShard(size, date), scrollTime);
		//		} catch (HttpClientErrorException e) {
		//			LOG.error(e.getResponseBodyAsString());
		//			throw e;
		//		}
		try {
			responseEntity = restTemplate
					.exchange(host + "/{index}/{type}/_search?size={size}", HttpMethod.POST, generateHttpEntityWithMap(queryBody), new ParameterizedTypeReference<ElasticSearchResponse>() {
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
				responseEntity = restTemplate.exchange(host + "/{index}/{type}/_search?from={accu}&size={size}", HttpMethod.POST, generateHttpEntityWithMap(queryBody),
						new ParameterizedTypeReference<ElasticSearchResponse>() {
						}, index, logType, accu, size);
				long hitsSize = responseEntity.getBody().getHits().getHits().size();
				if (hitsSize == 0) {
					break;
				}
				System.out.println(accu);
//				long current_accu = accu;
				responseEntity.getBody().getHits().getHits().parallelStream().forEach(hit -> {
//					Long val = map.putIfAbsent(hit.getId(), current_accu);
//					if (val != null) {
//						LOG.error("current accu is {}, prev accu is {}, id is {}", current_accu, val, hit.getId());
//					}
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
