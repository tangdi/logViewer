package com.blackboard.logstash.parser;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class LogPatternTest {
	@Test
	public void testPattern() {
		String userIdPattern = LogPattern.get("USER_ID");
		Assert.assertTrue("-1".matches(userIdPattern));
		Assert.assertTrue("_34_1".matches(userIdPattern));

		String requestIdPattern = LogPattern.get("NON_SPACE");
		Assert.assertTrue("76e78524-08cd-4953-822a-bfe5993719d4".matches(requestIdPattern));
		Assert.assertFalse("76e78524-08cd-4953-822a-bfe5993719d4 ".matches(requestIdPattern));

		String logLevelPattern = LogPattern.get("LOG_LEVEL");
		Assert.assertTrue("ERROR".matches(logLevelPattern));
		Assert.assertFalse("error2:".matches(logLevelPattern));
	}

	@Test
	public void testExtracter() {
		Map<String, Object> fields = new HashMap<>();
		fields.put("message",
				"request_id:9361e6e8-a17e-4756-95c6-b28e73c5847e user_id:-1 10:55:34.700 [http-nio-8080-exec-2] DEBUG com.blackboard.mbaas.interceptor.MBaasContextInterceptor:148 - mbaas context ->MBaasContext [clientInfo=ClientInfo [carrierInfo=CarrierInfo [ca        rrierCode=null, carrierName=null], deviceInfo=DeviceInfo [deviceName=null, deviceId=null], language=null, platform=null, p        latformVersion=null, sdkVersion=null, appName=null, appVersion=null, clientId=null, timezone=null], institutionInfo=Instit        utionInfo [institutionId=null, institutionName=null, b2Url=null], cookies=null, needDeveloperMessage=false, ext=null]");
		Filter filter = new Filter();
		filter.add("message",
				"(?:request_id%{NON_SPACE:requestId}%{SPACE}user_id:%{USER_ID:userId})? %{NON_SPACE:timestamp} %{NON_SPACE} %{LOG_LEVEL:logLevel} %{GREEDY: detail}");
		EventExtracter.extract(fields, filter);
		System.out.println(fields);
		Assert.assertNotNull(fields.get("userId"));
		System.out.println(fields);
		//		Pattern.compile("request_id(?<requestId>\\S+) user_id:(?<userId>[\\d\\-_]+) (?:\\S+) (?:\\S+) (?<logLevel>[A-Z]+) (?<detail>.*)");

	}

	@Test
	public void testExtracterWithoutRequest_id() {
		Map<String, Object> fields = new HashMap<>();
		fields.put("message",
				"  10:55:34.700 [http-nio-8080-exec-2] DEBUG com.blackboard.mbaas.interceptor.MBaasContextInterceptor:148 - mbaas context ->MBaasContext [clientInfo=ClientInfo [carrierInfo=CarrierInfo [ca        rrierCode=null, carrierName=null], deviceInfo=DeviceInfo [deviceName=null, deviceId=null], language=null, platform=null, p        latformVersion=null, sdkVersion=null, appName=null, appVersion=null, clientId=null, timezone=null], institutionInfo=Instit        utionInfo [institutionId=null, institutionName=null, b2Url=null], cookies=null, needDeveloperMessage=false, ext=null]");
		Filter filter = new Filter();
		filter.add("message",
				"(?:request_id%{NON_SPACE:requestId} user_id:%{USER_ID:userId})?%{SPACE}%{NON_SPACE:timestamp} %{NON_SPACE} %{LOG_LEVEL:logLevel} %{GREEDY: detail}");
		EventExtracter.extract(fields, filter);
		Assert.assertNotNull(fields.get("timestamp"));
		System.out.println(fields);
		//		Pattern.compile("request_id(?<requestId>\\S+) user_id:(?<userId>[\\d\\-_]+) (?:\\S+) (?:\\S+) (?<logLevel>[A-Z]+) (?<detail>.*)");

	}

	@Test
	public void testExtracterWithMultiline() {
		Map<String, Object> fields = new HashMap<>();
		fields.put("message",
				"  10:55:34.700 [http-nio-8080-exec-2] DEBUG com.blackboard.mbaas.interceptor.MBaasContextInterceptor:148 - mbaas context ->MBaasContext\n [clientInfo=ClientInfo [carrierInfo=CarrierInfo [ca        \nrrierCode=null, carrierName=null], deviceInfo=DeviceInfo [deviceName=null, deviceId=null], language=null, platform=null, p        latformVersion=null, sdkVersion=null, appName=null, appVersion=null, clientId=null, timezone=null], institutionInfo=Instit        utionInfo [institutionId=null, institutionName=null, b2Url=null], cookies=null, needDeveloperMessage=false, ext=null]");
		Filter filter = new Filter();
		filter.add("message",
				"(?:request_id%{NON_SPACE:requestId} user_id:%{USER_ID:userId})?%{SPACE}%{NON_SPACE:timestamp} %{NON_SPACE} %{LOG_LEVEL:logLevel} %{GREEDY: detail}");
		EventExtracter.extract(fields, filter);
		Assert.assertNotNull(fields.get("detail"));
		System.out.println(fields.get("detail"));
		//		Pattern.compile("request_id(?<requestId>\\S+) user_id:(?<userId>[\\d\\-_]+) (?:\\S+) (?:\\S+) (?<logLevel>[A-Z]+) (?<detail>.*)");

	}

	@Test
	public void testExtracterWithEnclosingVariable() {
		Map<String, Object> fields = new HashMap<>();
		fields.put("message",
				"  10:55:34.700 [http-nio-8080-exec-2] DEBUG com.blackboard.mbaas.interceptor.MBaasContextInterceptor:148 - mbaas context ->MBaasContext\n [clientInfo=ClientInfo [carrierInfo=CarrierInfo [ca        \nrrierCode=null, carrierName=null], deviceInfo=DeviceInfo [deviceName=null, deviceId=null], language=null, platform=null, p        latformVersion=null, sdkVersion=null, appName=null, appVersion=null, clientId=null, timezone=null], institutionInfo=Instit        utionInfo [institutionId=null, institutionName=null, b2Url=null], cookies=null, needDeveloperMessage=false, ext=null]");
		Filter filter = new Filter();
		filter.add("message",
				"(?:request_id%{NON_SPACE:requestId} user_id:%{USER_ID:userId})?%{SPACE}%{TIME:timestamp} %{NON_SPACE} %{LOG_LEVEL:logLevel} %{GREEDY: detail}");
		EventExtracter.extract(fields, filter);
		Assert.assertNotNull(fields.get("timestamp"));
		System.out.println(fields.get("detail"));
		//		Pattern.compile("request_id(?<requestId>\\S+) user_id:(?<userId>[\\d\\-_]+) (?:\\S+) (?:\\S+) (?<logLevel>[A-Z]+) (?<detail>.*)");

	}

}
