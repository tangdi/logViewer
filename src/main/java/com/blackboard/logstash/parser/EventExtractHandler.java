package com.blackboard.logstash.parser;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.UntypedActor;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class EventExtractHandler extends UntypedActor{

	private ActorRef nextHandler;
	public EventExtractHandler(ActorRef nextHandler) {
		this.nextHandler = nextHandler;
	}

	public void handle(Event event) {
		if(Objects.nonNull(event.filter)){
			Map<String, Object> fields = EventExtracter.extract(new HashMap<>(event.fields), event.filter);
			nextHandler.tell(new Event(fields, event.host, event.index, event.type, event.id, event.filter), getSender());
		}else{
			nextHandler.tell(event, getSender());
		}

	}

	@Override
	public void onReceive(Object message) throws Exception {
		if(message instanceof Event){
			handle((Event) message);
		}else if(Scanner.FINISH_WORK.equals(message)){
			nextHandler.tell(Scanner.FINISH_WORK, ActorRef.noSender());
			getSelf().tell(PoisonPill.getInstance(), ActorRef.noSender());
		}else{
			unhandled(message);
		}
	}
}
