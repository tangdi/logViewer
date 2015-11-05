package com.blackboard.logstash.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Filter {
	public List<FilterAction> actions = new ArrayList<>();

	public void add(String targetField, String pattern, SuccessAction successAction, FailAction failAction) {
		actions.add(new FilterAction(targetField, pattern, successAction, failAction));
	}

	public void add(String targetField, String pattern) {
		actions.add(new FilterAction(targetField, pattern));
	}

	@FunctionalInterface
	public static interface FailAction {
		public void action(Map<String, Object> fields);
	}

	@FunctionalInterface
	public static interface SuccessAction {
		public void action(Map<String, Object> fields);
	}

	public static class FilterAction {
		public String targetField;
		public String filterPattern;
		public FailAction failAction;
		public SuccessAction successAction;

		public FilterAction(String targetField, String filterPattern, SuccessAction successAction,
				FailAction failAction) {
			this.targetField = targetField;
			this.filterPattern = filterPattern;
			this.successAction = successAction;
			this.failAction = failAction;
		}

		public FilterAction(String targetField, String filterPattern) {
			this.targetField = targetField;
			this.filterPattern = filterPattern;
		}

		public void actOnFail(Map<String, Object> fields) {
			if (failAction != null) {
				failAction.action(fields);
			}
		}

		public void actOnSuccess(Map<String, Object> fields) {
			if (successAction != null) {
				successAction.action(fields);
			}
		}

	}

}
