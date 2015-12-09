/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.parser;

import akka.actor.*;
import akka.japi.Function;
import akka.routing.Broadcast;
import akka.routing.RoundRobinPool;
import scala.concurrent.duration.Duration;

import java.io.IOException;

/**
 * ClassName: MasterHandler Function: TODO
 *
 * @Author: dtang
 * @Date: 9/26/15, 4:18 PM
 */
public class MasterHandler extends UntypedActor {
	ActorRef router;
	Props props;

	public MasterHandler(Props routeeProps, int count) {
		this.props = routeeProps;
		router = getContext()
				.actorOf(new RoundRobinPool(count).withSupervisorStrategy(strategy).props(routeeProps));
		getContext().watch(router);

	}

	private static SupervisorStrategy strategy = new OneForOneStrategy(-1, Duration.Inf(),
			new Function<Throwable, SupervisorStrategy.Directive>() {

				@Override
				public SupervisorStrategy.Directive apply(Throwable param) throws Exception {
					if (param instanceof IOException) {
						return SupervisorStrategy.resume();
					}
					return SupervisorStrategy.escalate();
				}
			});

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof Event) {
			router.tell(message, getSender());
		} else if (Scanner.FINISH_WORK.equals(message)) {
			System.out.println("receive finish call");
			router.tell(new Broadcast(message), getSender());
		} else if (message instanceof Terminated) {
			System.out.println("receive terminated");
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
