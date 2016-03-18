/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.config;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.blackboard.logstash.parser.EventExtractHandler;
import com.blackboard.logstash.parser.EventStorageHandler;
import com.blackboard.logstash.parser.MasterHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

/**
 * ClassName: ActorSystemConfig Function: TODO
 *
 * @Author: dtang
 * @Date: 12/18/15, 12:16 PM
 */
@Configuration
public class ActorSystemConfig {
	@Autowired RestTemplate restTemplate;
	private volatile ActorSystem actorSystem;

	@Bean
	ActorSystem actorSystem() {
		if (actorSystem == null) {
			synchronized (this) {
				actorSystem = ActorSystem.create("logApp");
			}
		}
		return actorSystem;
	}

	@Bean
	ActorRef logExtracter() {
		if (actorSystem == null) {
			synchronized (this) {
				actorSystem = ActorSystem.create("logApp");
			}
		}

		ActorRef storageMaster = actorSystem.actorOf(MasterHandler.createMasterProp(20, EventStorageHandler.class, restTemplate), "storager");
		ActorRef extractor = actorSystem.actorOf(MasterHandler.createMasterProp(10, EventExtractHandler.class, storageMaster), "extracter");
		return extractor;
	}
}
