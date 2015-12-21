package com.blackboard.logstash.job;

import java.time.ZonedDateTime;
import java.util.List;

import org.springframework.http.HttpEntity;

/**
 * Created by dtang on 12/18/15.
 */
public interface ElasticCrawlJob {

	String getSourceUrl(ZonedDateTime date, String type);

	List<String> getSourceTypes();

	HttpEntity getSourceRequestEntity();

	String getSourceIndex(ZonedDateTime date);

	String getSourceUniqueId();

	String getDestinationUrl(ZonedDateTime date, String type);

	String getDestinationUrl(ZonedDateTime date);

	String getDestinationIndex(ZonedDateTime date);

	String getDestinationHost();
}
