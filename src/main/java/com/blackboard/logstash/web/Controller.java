/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.web;

import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.TimeZone;

import com.blackboard.logstash.job.ElasticToRedis;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.propertyeditors.CustomDateEditor;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * ClassName: Controller Function: TODO
 *
 * @Author: dtang
 * @Date: 9/29/15, 11:15 AM
 */
@RestController
public class Controller {
	@Autowired ElasticToRedis elasticToRedis;

	@InitBinder
	private void initBinder(WebDataBinder binder) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
		CustomDateEditor editor = new CustomDateEditor(sdf, false);
		binder.registerCustomEditor(Date.class, editor);
	}

	@RequestMapping("/")
	String home() {
		return "home";
	}

	@RequestMapping("/crawl")
	String crawl(@RequestParam Date fromDate, @RequestParam Date endDate, @RequestParam(required = false, defaultValue = "false") String overwriteRedis,
			@RequestParam(required = false, defaultValue = "false") String overwriteElastic) {
		long start = System.currentTimeMillis();
		try {
			elasticToRedis.crawl(ZonedDateTime.ofInstant(fromDate.toInstant(), ZoneOffset.UTC), ZonedDateTime.ofInstant(endDate.toInstant(), ZoneOffset.UTC), "AccessLog",
					Boolean.parseBoolean(overwriteRedis), Boolean.parseBoolean(overwriteElastic));
		} catch (Throwable e) {
			return "error: " + e.getMessage();

		}
		return "success, take " + (System.currentTimeMillis() - start) + "millis";
	}
}
