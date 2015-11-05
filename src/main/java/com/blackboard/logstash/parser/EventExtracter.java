package com.blackboard.logstash.parser;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EventExtracter {
	public static final String VARIABLE_PATTERN = "\\%\\{(.*?)\\}";

	public static Map<String, Object> extract(Map<String, Object> fields, Filter filter) {
		for (Filter.FilterAction filterAction : filter.actions) {
			String targetField = filterAction.targetField;
			String originalPattern = filterAction.filterPattern;

			FieldPattern fieldPattern = EventExtracter.render(originalPattern);
			//			System.out.println(fieldPattern.pattern);
			Pattern pattern = Pattern.compile(fieldPattern.pattern, Pattern.DOTALL);

			Object object  = getValue(fields, targetField);
			if(! (object instanceof String)){
				continue;
			}
			String text = (String) object;
			Matcher matcher = pattern.matcher(text);
			if (matcher.matches()) {
				//				System.out.println("yes");
				fieldPattern.groupNames.stream().forEach(groupName -> {
					fields.putIfAbsent(groupName, matcher.group(groupName));
				});
				filterAction.actOnSuccess(fields);
			} else {
				filterAction.actOnFail(fields);
			}
		}
		return fields;
	}

	public static FieldPattern render(String template) {
		FieldPattern fieldPattern = new FieldPattern();
		Pattern pattern = Pattern.compile(VARIABLE_PATTERN);
		Matcher matcher = pattern.matcher(template);
		StringBuilder newString = new StringBuilder();
		int start_index = 0;
		while (matcher.find()) {
			String original = matcher.group(0);
			String[] toBeReplaced = matcher.group(1).split(":");
			newString.append(template.substring(start_index, matcher.start()));

			String realPattern = LogPattern.get(toBeReplaced[0].trim());
			if (realPattern == null) {
				throw new IllegalArgumentException("Predefine Pattern does not include " + toBeReplaced[0].trim());
			}
			// realPattern could also be %{NAME} format, so need to decode it first. Ignore namedGroup because it is not allowed as nested.
			FieldPattern decodedFieldPattern = render(realPattern);
			if (toBeReplaced.length == 2) {
				String nameGroup = "(?<" + toBeReplaced[1].trim() + ">" + decodedFieldPattern.pattern + ")";
				fieldPattern.groupNames.add(toBeReplaced[1].trim());
				newString.append(nameGroup);
			} else if (toBeReplaced.length == 1) {
				newString.append("(?:" + decodedFieldPattern.pattern + ")");
			}
			start_index = matcher.start() + original.length();

		}
		newString.append(template.substring(start_index));
		fieldPattern.pattern = newString.toString();
		return fieldPattern;
	}

	public static class FieldPattern {
		public Set<String> groupNames = new HashSet<>();
		public String pattern;

		public String toString() {
			return "groupNames = " + groupNames.toString() + ", Pattern = " + pattern;
		}
	}

	public static Object getValue(Map<String, ?> fields, String targetField){
		int dotIndex = targetField.indexOf(".");
		if(dotIndex == -1){
			//TODO NPE
			return fields.get(targetField);
		}
		String firstField = targetField.substring(0, dotIndex);
		Object object = fields.get(firstField);
		if(! (object instanceof HashMap)){
			throw new RuntimeException( firstField + " is not a map");
		}
		fields = (HashMap<String, ?>) object;
		String restFields = targetField.substring(dotIndex+1);
		return getValue(fields, restFields);


	}
}
