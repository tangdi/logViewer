/**
 * Copyright (c) 2015, Blackboard Inc. All Rights Reserved.
 */
package com.blackboard.logstash.web;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * ClassName: Controller Function: TODO
 *
 * @Author: dtang
 * @Date: 9/29/15, 11:15 AM
 */
@RestController
public class Controller {
	@RequestMapping("/")
	String home(){
		return "home";
	}
}
