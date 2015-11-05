/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.job;

import java.text.SimpleDateFormat;
import java.util.*;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.blackboard.logstash.model.ElasticSearchResponse;
import com.blackboard.logstash.model.Hit;
import com.blackboard.logstash.parser.*;
import com.blackboard.logstash.parser.Scanner;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.Base64Utils;
import org.springframework.web.client.RestTemplate;

/**
 * ClassName: ElasticCrawler Function: TODO
 *
 * @Author: dtang
 * @Date: 11/3/15, 1:31 PM
 */
@Component
public class ElasticCrawler {
	@Autowired private RestTemplate restTemplate;

	private SimpleDateFormat cloudElasticDateFormat;

	private SimpleDateFormat localElasticDateFormat;

	public ElasticCrawler() {
		TimeZone gmt = TimeZone.getTimeZone("GMT");
		cloudElasticDateFormat = new SimpleDateFormat("yyyyMMdd");
		cloudElasticDateFormat.setTimeZone(gmt);

		localElasticDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		localElasticDateFormat.setTimeZone(gmt);

	}

	@Scheduled(fixedDelay = 24 * 3600 * 1000)
	public void crawl() {
		ActorSystem system = ActorSystem.create("logApp");
		Filter filter = new Filter();

		filter.add("request.headers.agent", "%{GREEDY}%{PLATFORM:platform}%{GREEDY}?", null, null);
		ActorRef storageMaster = system
				.actorOf(MasterHandler.createMasterProp(20, EventStorageHandler.class, restTemplate), "storage");
		ActorRef reaper = system.actorOf(Props.create(ActorReaper.class));
		ActorRef extractMaster = system.actorOf(MasterHandler.createMasterProp(10, EventExtractHandler.class, filter, storageMaster), "extract");
		reaper.tell(new ActorReaper.WatchMe(storageMaster), ActorRef.noSender());
		reaper.tell(new ActorReaper.WatchMe(extractMaster), ActorRef.noSender());

		//TODO fill the real elastic host
		String host = "XXXXXX";
		Date date = new Date(System.currentTimeMillis() - DateUtils.MILLIS_PER_DAY);
		String targetDate = cloudElasticDateFormat.format(date);
		String body = "{ \"query\" : { \"match_all\" : {} } }";
		HttpHeaders headers = new HttpHeaders();

		headers.set("Authorization", "Basic " + generateAuthEncoded());
		HttpEntity httpEntity = new HttpEntity(body, headers);

		ResponseEntity<ElasticSearchResponse> responseEntity = restTemplate
				.exchange(host, HttpMethod.GET, httpEntity, new ParameterizedTypeReference<ElasticSearchResponse>() {
				}, targetDate, 0, 0);
		Long totalCount;
		if (isSuccess(responseEntity)) {
			//TODO potential NPE
			totalCount = responseEntity.getBody().getHits().getTotal();
			System.out.println(totalCount);

			long size = 300;
			long from = 0;
//			Set<String> idSet = new HashSet<>();
			while (from < totalCount) {
				responseEntity = restTemplate.exchange(host, HttpMethod.GET, httpEntity,
						new ParameterizedTypeReference<ElasticSearchResponse>() {
						}, targetDate, Long.toString(size), Long.toString(from));
				from += responseEntity.getBody().getHits().getHits().size();
				responseEntity.getBody().getHits().getHits().stream().forEach(hit -> {
					Event event = generateEvent(hit, date);
					extractMaster.tell(event, ActorRef.noSender());
				});
//				responseEntity.getBody().getHits().getHits().stream().forEach(hit -> {
//					if (idSet.contains(hit.getId())) {
//						System.out.println("already has" + hit.getId());
//					}else{
//						idSet.add(hit.getId());
//					}
//				});
				System.out.println(from);
			}
			storageMaster.tell(Scanner.FINISH_WORK, ActorRef.noSender());
//			System.out.println("id set size is " + idSet.size());
			system.awaitTermination();
		}

	}

	public String generateAuthEncoded() {
		//TODO fill the real user password
		String user = "XXX";
		String password = "XXX";
		String authEncoded = new String(Base64Utils.encode((user + ":" + password).getBytes()));
		return authEncoded;
	}

	public boolean isSuccess(ResponseEntity<?> responseEntity) {
		int statusCode = responseEntity.getStatusCode().value();
		return statusCode >= 200 && statusCode < 300;
	}

	public void addMoreFields(Map<String, Object> fields) {
		if (fields != null) {
			fields.putIfAbsent("type", "AccessLog");
			fields.putIfAbsent("index", "");
		}
	}

	public Event generateEvent(Hit hit, Date date) {
		String index = "log." + localElasticDateFormat.format(date);
		Map<String, Object> source = hit.getSource();
		Event event = new Event(hit.getSource(), index, hit.getType(), null);
		return event;
	}

}
