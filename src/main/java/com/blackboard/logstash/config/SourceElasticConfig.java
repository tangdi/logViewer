/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.config;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.blackboard.logstash.job.ElasticCrawlJob;
import com.blackboard.logstash.util.ObjectMapperUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.util.Base64Utils;

/**
 * ClassName: SourceElasticConfig Function: TODO
 *
 * @Author: dtang
 * @Date: 12/18/15, 2:27 PM
 */
@Configuration
public class SourceElasticConfig {
	private static final Logger LOG = LogManager.getLogger(SourceElasticConfig.class);

	@Bean
	ElasticCrawlJob source0() {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
		DateTimeFormatter destFormtter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		ElasticCrawlJob elasticCrawlJob = new ElasticCrawlJob() {
			@Override
			public String getDestinationHost() {
				return "http://localhost:9200";
			}

			private String generateAuthEncoded() {
				//TODO fill the real user password
						String user = "";
						String password = "";
				String authEncoded = new String(Base64Utils.encode((user + ":" + password).getBytes()));
				return authEncoded;
			}

			@Override
			public String getSourceUrl(ZonedDateTime date, String type) {
				String urlTemplate = "https://telemetry-prod-bastion01.cloud.bb/%s/%s/";
				return String.format(urlTemplate, getSourceIndex(date), type);
			}

			@Override
			public String getDestinationIndex(ZonedDateTime date) {
				return "log." + destFormtter.format(date);
			}

			@Override
			public String getDestinationUrl(ZonedDateTime date, String type) {
				String urlTemplate = "%s/%s/%s/";
				return String.format(urlTemplate, getDestinationHost(), getDestinationIndex(date), type);
			}

			@Override
			public String getDestinationUrl(ZonedDateTime date) {
				String urlTemplate = "%s/%s/";
				return String.format(urlTemplate, getDestinationHost(), getDestinationIndex(date));
			}

			@Override
			public List<String> getSourceTypes() {
				List<String> types = new ArrayList<>();
				types.add("AccessLog");
				return types;
			}

			@Override
			public HttpEntity getSourceRequestEntity() {
				String queryBody = "{\"query\": { \"match_all\": {} }, \"fields\": [\"*\"], \"_source\": true }";
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

			@Override
			public String getSourceIndex(ZonedDateTime date) {
				return "event." + formatter.format(date);
			}

			@Override
			public String getSourceUniqueId() {
				return "ProdBeast";
			}
		};
		return elasticCrawlJob;
	}
}
