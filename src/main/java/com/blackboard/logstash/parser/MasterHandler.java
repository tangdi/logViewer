/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.parser;

import akka.actor.*;
import akka.routing.Broadcast;
import akka.routing.RoundRobinPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.concurrent.duration.Duration;

/**
 * ClassName: MasterHandler Function: TODO
 *
 * @Author: dtang
 * @Date: 9/26/15, 4:18 PM
 */
public class MasterHandler extends UntypedActor {
	private static final Logger LOG = LogManager.getLogger(MasterHandler.class);
	ActorRef router;
	Props props;

	public MasterHandler(Props routeeProps, int count) {
		this.props = routeeProps;
		router = getContext().actorOf(new RoundRobinPool(count).withSupervisorStrategy(strategy).props(routeeProps));
		getContext().watch(router);

	}

	private static SupervisorStrategy strategy = new OneForOneStrategy(-1, Duration.Inf(), (Throwable e) -> {
		LOG.error(e);
		return SupervisorStrategy.resume();
	});

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof Event) {
			router.tell(message, getSender());
		} else if (Scanner.FINISH_WORK.equals(message)) {
			router.tell(new Broadcast(message), getSender());
		} else if (message instanceof Terminated) {
			Terminated terminated = (Terminated) message;
			if (terminated.getActor() == router) {
				getSelf().tell(PoisonPill.getInstance(), ActorRef.noSender());
				//				context().system().shutdown();
			} else {
				unhandled(message);
			}
		} else {
			unhandled(message);
		}

	}

	public static Props createMasterProp(int routeeCount, Class<?> clazz, Object... args) {
		return Props.create(MasterHandler.class, Props.create(clazz, args), routeeCount);

	}
}
