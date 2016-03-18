/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.config;

import java.io.File;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import com.blackboard.logstash.job.ElasticCrawlJob;
import com.blackboard.logstash.parser.Filter;
import com.blackboard.logstash.util.ObjectMapperUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.util.Base64Utils;

/**
 * ClassName: SourceElasticConfig Function: TODO
 *
 * @Author: dtang
 * @Date: 12/18/15, 2:27 PM
 */
public class SourceElasticConfig {
	private static final Logger LOG = LogManager.getLogger(SourceElasticConfig.class);
	private static final String JOB_CONFIG_FILE = "ElasticJobConfig.json";
	public static final List<ElasticCrawlJob> jobs;

	static {
		List<RawJob> rawjobs = Collections.emptyList();
		try {
			rawjobs = ObjectMapperUtil.getObjectMapper().readValue(new File(Thread.currentThread().getContextClassLoader().getResource(JOB_CONFIG_FILE).getFile()), new TypeReference<List<RawJob>>() {});
		} catch (Exception e) {
			LOG.error("cannot load config file", e);
		}
		if(Objects.nonNull(rawjobs) && rawjobs.size()>0){
			jobs = rawjobs.stream().filter(Objects::nonNull).map(rawJob -> {
				return new ElasticCrawlJob(){
					DateTimeFormatter formatter = DateTimeFormatter.ofPattern(rawJob.getSourceIndexDatePattern());
					DateTimeFormatter destFormtter = DateTimeFormatter.ofPattern(rawJob.getDestinationIndexDatePattern());

					@Override
					public String getSourceUrl(ZonedDateTime date, String type) {
						String urlTemplate = "%s/%s/%s/";
						return String.format(urlTemplate, rawJob.getSourceHost(), getSourceIndex(date), type);
					}

					@Override
					public List<String> getSourceTypes() {
						return rawJob.getSourceTypes();
					}

					@Override
					public HttpEntity getSourceRequestEntity() {
						Map<String, Object> bodyMap = rawJob.getSourceRequestBody();
						HttpHeaders headers = new HttpHeaders();
						headers.set("Authorization", "Basic " + generateAuthEncoded());
						HttpEntity httpEntity = new HttpEntity(bodyMap, headers);
						return httpEntity;
					}

					@Override
					public String getSourceIndex(ZonedDateTime date) {
						return rawJob.getSourceIndexPrefix() + formatter.format(date);
					}

					@Override
					public String getSourceUniqueId() {
						return rawJob.getSourceUniqueId();
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
					public String getDestinationIndex(ZonedDateTime date) {
						return rawJob.getDestinationIndexPrefix() + destFormtter.format(date);
					}

					@Override
					public String getDestinationHost() {
						return rawJob.getDestinationHost();
					}

					@Override
					public Optional<Filter> getFilter() {
						if(Objects.nonNull(rawJob.getFilters())){
							Filter filter = new Filter();
							for(RawJob.Filter filterConfig: rawJob.getFilters()){
								filter.add(filterConfig.getTargetField(), filterConfig.getPattern());
							}
							return Optional.of(filter);
						}
						return Optional.empty();
					}

					private String generateAuthEncoded() {
						String authEncoded = new String(Base64Utils.encode((rawJob.getSourceAuth().getUser() + ":" + rawJob.getSourceAuth().getPassword()).getBytes()));
						return authEncoded;
					}
				};
			}).collect(Collectors.toList());
		}else{
			jobs = Collections.emptyList();
		}
	}

	ElasticCrawlJob shanghaiELK() {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd");
		DateTimeFormatter destFormtter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		ElasticCrawlJob elasticCrawlJob = new ElasticCrawlJob() {
			@Override
			public String getDestinationHost() {
				return "http://localhost:9200";
			}

			@Override
			public Optional<Filter> getFilter() {
				return Optional.empty();
			}

			private String generateAuthEncoded() {
				//TODO fill the real user password
				String user = "kibanaadmin";
				String password = "Black@1";
				String authEncoded = new String(Base64Utils.encode((user + ":" + password).getBytes()));
				return authEncoded;
			}

			@Override
			public String getSourceUrl(ZonedDateTime date, String type) {
				String urlTemplate = "https://mbaas-prod-elk-664782521.us-east-1.elb.amazonaws.com/elasticsearch/%s/%s/";
				return String.format(urlTemplate, getSourceIndex(date), type);
			}

			@Override
			public String getDestinationIndex(ZonedDateTime date) {
				return "shanghai." + destFormtter.format(date);
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
				types.add("mbaas-rolling");
				types.add("mbaas-feedback");
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
				return "logstash-" + formatter.format(date);
			}

			@Override
			public String getSourceUniqueId() {
				return "SHANGHAI_DEVOPS_ELK";
			}
		};
		return elasticCrawlJob;
	}

	private static class RawJob{
		private static class Auth{
			private String user;
			private String password;

			public String getUser() {
				return user;
			}

			public void setUser(String user) {
				this.user = user;
			}

			public String getPassword() {
				return password;
			}

			public void setPassword(String password) {
				this.password = password;
			}
		}

		private static class Filter{
			private String targetField;
			private String pattern;

			public String getTargetField() {
				return targetField;
			}

			public void setTargetField(String targetField) {
				this.targetField = targetField;
			}

			public String getPattern() {
				return pattern;
			}

			public void setPattern(String pattern) {
				this.pattern = pattern;
			}
		}

		private String sourceUniqueId;
		private String sourceHost;
		private String sourceIndexDatePattern;
		private String sourceIndexPrefix;
		private List<String> sourceTypes;
		private Map<String, Object> sourceRequestBody;
		private Auth sourceAuth;
		private List<Filter> filters;
		private String destinationHost;
		private String destinationIndexDatePattern;
		private String destinationIndexPrefix;

		public String getSourceUniqueId() {
			return sourceUniqueId;
		}

		public void setSourceUniqueId(String sourceUniqueId) {
			this.sourceUniqueId = sourceUniqueId;
		}

		public String getSourceHost() {
			return sourceHost;
		}

		public void setSourceHost(String sourceHost) {
			this.sourceHost = sourceHost;
		}

		public String getSourceIndexDatePattern() {
			return sourceIndexDatePattern;
		}

		public void setSourceIndexDatePattern(String sourceIndexDatePattern) {
			this.sourceIndexDatePattern = sourceIndexDatePattern;
		}

		public String getSourceIndexPrefix() {
			return sourceIndexPrefix;
		}

		public void setSourceIndexPrefix(String sourceIndexPrefix) {
			this.sourceIndexPrefix = sourceIndexPrefix;
		}

		public List<String> getSourceTypes() {
			return sourceTypes;
		}

		public void setSourceTypes(List<String> sourceTypes) {
			this.sourceTypes = sourceTypes;
		}

		public Map<String, Object> getSourceRequestBody() {
			return sourceRequestBody;
		}

		public void setSourceRequestBody(Map<String, Object> sourceRequestBody) {
			this.sourceRequestBody = sourceRequestBody;
		}

		public Auth getSourceAuth() {
			return sourceAuth;
		}

		public void setSourceAuth(Auth sourceAuth) {
			this.sourceAuth = sourceAuth;
		}

		public List<Filter> getFilters() {
			return filters;
		}

		public void setFilters(List<Filter> filters) {
			this.filters = filters;
		}

		public String getDestinationHost() {
			return destinationHost;
		}

		public void setDestinationHost(String destinationHost) {
			this.destinationHost = destinationHost;
		}

		public String getDestinationIndexDatePattern() {
			return destinationIndexDatePattern;
		}

		public void setDestinationIndexDatePattern(String destinationIndexDatePattern) {
			this.destinationIndexDatePattern = destinationIndexDatePattern;
		}

		public String getDestinationIndexPrefix() {
			return destinationIndexPrefix;
		}

		public void setDestinationIndexPrefix(String destinationIndexPrefix) {
			this.destinationIndexPrefix = destinationIndexPrefix;
		}
	}
}
