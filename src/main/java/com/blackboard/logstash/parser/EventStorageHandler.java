/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.parser;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.UntypedActor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.client.RestTemplate;

/**
 * ClassName: EventStorageHandler Function: TODO
 *
 * @Author: dtang
 * @Date: 10/7/15, 2:08 PM
 */
public class EventStorageHandler extends UntypedActor {

	private final RestTemplate restTemplate;

	public EventStorageHandler(RestTemplate restTemplate) {
		this.restTemplate = restTemplate;
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof Event) {
			Event event = (Event) message;
			if (StringUtils.isNotBlank(event.id)) {
				restTemplate.put("{host}/{index}/{type}/{id}", event.fields, event.host, event.index, event.type, event.id);
			} else {
				restTemplate.postForLocation("{host}/{index}/{type}/", event.fields, event.host, event.index, event.type);

			}
		} else if (Scanner.FINISH_WORK.equals(message)) {
			getSelf().tell(PoisonPill.getInstance(), ActorRef.noSender());
		} else {
			unhandled(message);
		}
	}
}
