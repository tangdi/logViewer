/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.job;

import java.nio.charset.Charset;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

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

	private final boolean OVERWRITE_REDIS = false;
	private final boolean OVERWRITE_ELASTIC = true;

	public static final Charset UTF_8_CHARSET = Charset.forName("UTF-8");

	private DateTimeFormatter cloudElasticDateFormat;

	private DateTimeFormatter localElasticDateFormat;

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
		String host = "https://telemetry-prod-bastion01.cloud.bb/event.{date}/{logType}/";
//		String logType = "mbaas-rolling";
//		String host = "https://mbaas-prod-elk-664782521.us-east-1.elb.amazonaws.com/elasticsearch/logstash-{date}/{logType}/";

//		ZonedDateTime today = ZonedDateTime.now(ZoneOffset.UTC);
		ZonedDateTime today = ZonedDateTime.of(2015, 11, 22, 0, 0, 0, 0, ZoneOffset.UTC);
//		ZonedDateTime today = ZonedDateTime.of(2015, 11, 18, 0, 0, 0, 0, ZoneOffset.UTC);
		ZonedDateTime start = ZonedDateTime.of(2015, 11, 21, 0, 0, 0, 0, ZoneOffset.UTC);
		crawl(start, today, host, logType);
	}

	public void crawl(ZonedDateTime startDate, ZonedDateTime endDate, String url, String logType) {
		List<ZonedDateTime> dates = new ArrayList<>();
		for (ZonedDateTime date = startDate; date.isBefore(endDate); date = date.plusDays(1)) {
			String targetDate = cloudElasticDateFormat.format(date);
			LOG.debug("host is {}, log type is {}, date is {}", url, logType, targetDate);
			if(crawl(url, targetDate, logType) || OVERWRITE_ELASTIC) {
				dates.add(date);
			}
		}
		ElasticPersistence crawler = new ElasticPersistence(restTemplate, redisTemplate, cloudElasticDateFormat,
				localElasticDateFormat, dates, logType);
		crawler.store();

	}

	public boolean crawl(String url, String targetDate, String logType) {
		String redisKey = getRedisKey(targetDate, logType);
		boolean existing = redisTemplate.execute((RedisConnection connection) -> {
			return connection.exists(redisKey.getBytes(UTF_8_CHARSET));
		});
		if(existing){
			if(!OVERWRITE_REDIS){
				LOG.warn("target Date {} already exists in redis, skip overwrite", targetDate);
				return false;
			}else{
				redisTemplate.execute((RedisConnection connection) -> {
					return connection.del(redisKey.getBytes(UTF_8_CHARSET));
				});
			}
		}

		String body = "{ \"query\" : { \"match_all\" : {} } }";
//		String body = "{ \"query\" : { \"match\" : { \"message\" : \"No matched institution found for institution\" } } }";
		Map<String, Object> body_map = null;
		try{

			body_map = ObjectMapperUtil.getObjectMapper().readValue(body, HashMap.class);
		}catch (Throwable e){
			LOG.error(e);
		}
		HttpHeaders headers = new HttpHeaders();

		if (!url.endsWith("/")) {
			url += "/";
		}
		url += "_search?size={size}&from={from}";

		headers.set("Authorization", "Basic " + generateAuthEncoded());
		HttpEntity httpEntity = new HttpEntity(body_map, headers);

		ResponseEntity<ElasticSearchResponse> responseEntity = null;
		try {
			responseEntity = restTemplate.exchange(url, HttpMethod.POST, httpEntity,
					new ParameterizedTypeReference<ElasticSearchResponse>() {
					}, targetDate, logType, 0, 0);
		} catch(HttpClientErrorException e){
			LOG.error(e.getResponseBodyAsString());
		}
		Long totalCount;

		if (responseEntity != null && isSuccess(responseEntity)) {
			ObjectMapper objectMapper = ObjectMapperUtil.getObjectMapper();
			//TODO potential NPE
			totalCount = responseEntity.getBody().getHits().getTotal();
			LOG.debug("{} {} has {} log events", logType, targetDate, totalCount);

			long size = 300;
			long from = 0;

			while (from < totalCount) {
				responseEntity = restTemplate.exchange(url, HttpMethod.POST, httpEntity,
						new ParameterizedTypeReference<ElasticSearchResponse>() {
						}, targetDate, logType, Long.toString(size), Long.toString(from));
				from += responseEntity.getBody().getHits().getHits().size();

				responseEntity.getBody().getHits().getHits().parallelStream().forEach(hit -> {
					redisTemplate.execute((RedisConnection connection) -> {
						String value = null;
						try {
							value = objectMapper.writeValueAsString(hit);
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
				LOG.debug(from);
			}
		}
		return true;
	}

	public String generateAuthEncoded() {
		//TODO fill the real user password
		String user = "";
		String password = "";
		String authEncoded = new String(Base64Utils.encode((user + ":" + password).getBytes()));
		return authEncoded;
	}

	public boolean isSuccess(ResponseEntity<?> responseEntity) {
		int statusCode = responseEntity.getStatusCode().value();
		return statusCode >= 200 && statusCode < 300;
	}

	public static String getRedisKey(String dateString, String logType) {
		return logType + "_" + dateString;

	}

}
