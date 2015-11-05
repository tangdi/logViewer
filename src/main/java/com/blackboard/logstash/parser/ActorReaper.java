/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.parser;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: ActorReaper Function: TODO
 *
 * @Author: dtang
 * @Date: 9/29/15, 4:48 PM
 */
public class ActorReaper extends UntypedActor {
	private List<ActorRef> watchList = new ArrayList<>();

	public static class WatchMe {
		protected final ActorRef ref;

		public WatchMe(ActorRef ref) {
			Assert.notNull(ref);
			this.ref = ref;

		}
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof WatchMe) {
			WatchMe watchMe = (WatchMe) message;
			context().watch(watchMe.ref);
			watchList.add(watchMe.ref);
			System.out.println("watch");
		} else if (message instanceof Terminated) {
			Terminated terminated = (Terminated) message;
			context().unwatch(terminated.getActor());
			watchList.remove(terminated.getActor());
			if (CollectionUtils.isEmpty(watchList)) {
				context().system().shutdown();
			}
		}

	}

}