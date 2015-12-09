/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.job;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.blackboard.logstash.model.Hit;
import com.blackboard.logstash.parser.*;
import com.blackboard.logstash.util.ObjectMapperUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.CollectionUtils;
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

	private final String logType;
	private static final String HOST = "http://localhost:9200/";

	public ElasticPersistence(RestTemplate restTemplate, RedisTemplate<String, Object> redisTemplate,
			DateTimeFormatter cloudElasticDateFormat, DateTimeFormatter localElasticDateFormat,
			List<ZonedDateTime> dates, String logType) {
		this.restTemplate = restTemplate;
		this.redisTemplate = redisTemplate;
		this.cloudElasticDateFormat = cloudElasticDateFormat;
		this.localElasticDateFormat = localElasticDateFormat;
		this.logType = logType;
		this.dates = dates;
	}

	public void store() {
		if(CollectionUtils.isEmpty(dates)){
			LOG.debug("no dates to process");
			return;
		}
		ActorSystem system = ActorSystem.create("logApp");
		Filter filter = new Filter();

		//filter to get device platform from user agent
		filter.add("request.headers.agent", "%{GREEDY}%{PLATFORM:platform}%{GREEDY}?", null, null);
		ActorRef storageMaster = system
				.actorOf(MasterHandler.createMasterProp(20, EventStorageHandler.class, restTemplate, HOST), "storage");
		ActorRef reaper = system.actorOf(Props.create(ActorReaper.class));
		ActorRef extractMaster = system
				.actorOf(MasterHandler.createMasterProp(10, EventExtractHandler.class, filter, storageMaster),
						"extract");

		reaper.tell(new ActorReaper.WatchMe(storageMaster), ActorRef.noSender());
		reaper.tell(new ActorReaper.WatchMe(extractMaster), ActorRef.noSender());


		for(ZonedDateTime date: dates) {
			String targetDate = cloudElasticDateFormat.format(date);
			String dateString = localElasticDateFormat.format(date);
			String index = generateIndex(dateString);
			String redisKey = ElasticToRedis.getRedisKey(targetDate, logType);

			Long totalCount = redisTemplate.execute((RedisConnection connection) -> {
				return connection.lLen(redisKey.getBytes(ElasticToRedis.UTF_8_CHARSET));
			});

			if(totalCount == 0L){
				LOG.debug("redis key {} contains no element", redisKey);
				continue;
			}
			try {
				LOG.debug("delete index {} if existing", index);
				restTemplate.delete("{host}/{index}", HOST, index);
			}catch (Throwable e){
				LOG.error(e.getMessage());
			}
			LOG.debug("send log {} {} from redis to dest, total log event count is {}", logType, targetDate, totalCount);


			long size = 600;
			long from = 0;


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
						LOG.info(hit);
						hitList.add(hit);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}

				hitList.stream().forEach(hit -> {
					Event event = generateEvent(hit, index);
					extractMaster.tell(event, ActorRef.noSender());
				});

			}
		}
		storageMaster.tell(Scanner.FINISH_WORK, ActorRef.noSender());
		// wait for actor system to terminate
		system.awaitTermination();

	}

	public Event generateEvent(Hit hit, String index){
		Event event = new Event(hit.getSource(), index, hit.getType(), null);
		return event;
	}

	public String generateIndex(String date){
		return "log." + date;
	}

}
