/*
* Copyright (c) waylau.com, 2021. All rights reserved.
*/

package com.waylau.spark.java.samples.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Log4j Example
 * 
 * @author <a href="https://waylau.com">Way Lau</a> 
 * @since 2021-08-19
 */
public class Log4jExample {

	private static final Logger LOG = LoggerFactory
			.getLogger(Log4jExample.class);

	public static void main(String[] args) {
		LOG.error("This is a error log.");
		LOG.warn("This is a warn log.");
		LOG.info("This is a info log.");
		LOG.debug("This is a debug log.");
	}

}