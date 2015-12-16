/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.job;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.blackboard.logstash.parser.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.util.Arrays;
import java.util.Date;

/**
 * ClassName: LogScanTask Function: TODO
 *
 * @Author: dtang
 * @Date: 9/29/15, 2:05 PM
 */
public class LogScanTask {
	RestTemplate restTemplate;

	public void runLogScan() {
		ActorSystem system = ActorSystem.create("logApp");
		//prepare
		String filePath = String.join(File.separator,
				Arrays.asList("/Users", "dtang", "Downloads", "apache-tomcat-8.0.24", "logs",
						"mbaas-rolling.log.2015-08-20"));
		String eventSeparator = "^(?:request_id|\\s+\\d{2})";

		Filter filter = new Filter();

		filter.add("path", "%{GREEDY}/%{NON_SPACE:type}\\.log\\.%{DATE:date}", null, fields -> {
			fields.put("date", Scanner.dateFormat.format(new Date()));
			fields.put("type", "mbaas-rolling");
		});
		filter.add("message",
				"(?:request_id%{NON_SPACE:requestId} user_id:%{USER_ID:userId})?%{SPACE}%{TIME:time} %{NON_SPACE} %{LOG_LEVEL:logLevel} %{GREEDY: detail}",
				fields -> {
					fields.put("@timestamp", fields.get("date") + "T" + fields.get("time") + "Z");
				}, null);

		ActorRef scanner = system.actorOf(Scanner.props(filePath, eventSeparator), "scanner");

		ActorRef storageMaster = system.actorOf(MasterHandler.createMasterProp(20, EventStorageHandler.class, restTemplate), "storage");
		ActorRef extractMaster = system.actorOf(MasterHandler.createMasterProp(10, EventExtractHandler.class, filter, storageMaster), "extract");
		//register reaper
		ActorRef reaper = system.actorOf(Props.create(ActorReaper.class));
		reaper.tell(new ActorReaper.WatchMe(scanner), ActorRef.noSender());
		reaper.tell(new ActorReaper.WatchMe(storageMaster), ActorRef.noSender());
		reaper.tell(new ActorReaper.WatchMe(extractMaster), ActorRef.noSender());
		//begin
		scanner.tell(Scanner.BEGIN_SCAN, extractMaster);
		// await system termination
		system.awaitTermination();

	}

//	@Scheduled(fixedDelay = 24 * 3600 * 1000)
	public void scan() {
		runLogScan();

	}
	public static void main(String[] args){
		LogScanTask t = new LogScanTask();
		t.runLogScan();
	}
}
