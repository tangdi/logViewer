package com.blackboard.logstash.parser;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.UntypedActor;

import java.util.HashMap;
import java.util.Map;

public class EventExtractHandler extends UntypedActor{
	public Filter filter;
	public int count;

	private ActorRef nextHandler;
	public EventExtractHandler(Filter filter, ActorRef nextHandler) {
		this.filter = filter;
		this.nextHandler = nextHandler;
//		config().connectionConfig(connectionConfig().closeIdleConnectionsAfterEachResponse());
	}

	public void handle(Event event) {
		Map<String, Object> fields = EventExtracter.extract(new HashMap<>(event.fields), filter);
		nextHandler.tell(new Event(fields, event.host, event.index, event.type, event.id), getSender());
//				System.out.println("event field is " + event.fields);
//		baseURI = "http://localhost:9200";
//
//		Response response = given().body(event.fields)
//				.put("/{type}/log/{id}", event.fields.get("type") + "." + event.fields.get("date"), ++count)
//				.andReturn();
//		//		System.out.println(response.getBody().asString());
//
//		if (count % 10000 == 0) {
//			System.out.println(count);
//			try {
//				Thread.sleep(10000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
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
