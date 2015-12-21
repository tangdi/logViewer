package com.blackboard.logstash.parser;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Scanner extends UntypedActor {
	public static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-d");
	public static String BEGIN_SCAN = "begin";
	public static String FINISH_WORK = "finish";
	private final String DEFAULT_EVENT_SEPERATOR = "^";
	private File file;
	private Pattern eventSeparatorPattern;

	public Scanner(String filePath, String eventSeparator) {
		this(new File(filePath), eventSeparator);
	}

	public Scanner(File file, String eventSeparator) {
		this.file = file;
		if (eventSeparator != null) {
			this.eventSeparatorPattern = Pattern.compile(eventSeparator);
		} else {
			this.eventSeparatorPattern = Pattern.compile(DEFAULT_EVENT_SEPERATOR);
		}
	}

	public void scan(ActorRef sender) throws IOException {
		int i = 0;
		FileReader reader = new FileReader(file);
		StringBuilder eventMessage = new StringBuilder();
		try (BufferedReader bufReader = new BufferedReader(reader)) {
			for (String line; (line = bufReader.readLine()) != null; ) {
				System.out.println(i++);
				Matcher matcher = eventSeparatorPattern.matcher(line);
				if (matcher.find()) {
					if (eventMessage.length() == 0) {
						eventMessage.append(line);
					} else {
						String lastEventMessage = eventMessage.toString();
						sender.tell(populateEvent(lastEventMessage), getSelf());
						eventMessage.setLength(0);
						eventMessage.append(line);
					}
				} else {
					eventMessage.append("\n");
					eventMessage.append(line);
				}
			}
			String lastEventMessage = eventMessage.toString();
			sender.tell(populateEvent(lastEventMessage), getSelf());
		}
		sender.tell(FINISH_WORK, getSelf());
		getSelf().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}

	public Event populateEvent(String eventMessage) {
		Map<String, Object> fields = new HashMap<>();
		fields.put("message", eventMessage);
		fields.put("path", file.getAbsolutePath());
		Event event = new Event(fields, "http://", "log-" + fields.get("date"), fields.get("type").toString() );
		return event;

	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (BEGIN_SCAN.equals(message)) {
			scan(getSender());
		} else if (FINISH_WORK.equals(message)) {
			getSelf().tell(PoisonPill.getInstance(), ActorRef.noSender());
		} else {
			unhandled(message);
		}
	}

	public static Props props(String filePath, String eventSeparator) {
		return Props.create(new Creator<Scanner>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Scanner create() throws Exception {
				return new Scanner(filePath, eventSeparator);
			}
		});
	}
}
