/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.common;

/**
 * Java Document
 * 
 * @since 1.0.0 2021年8月21日
 * @author <a href="https://waylau.com">Way Lau</a>
 */
public class JavaDocument {
	private long id;

	private String text;

	public JavaDocument(long id, String text) {
		this.id = id;
		this.text = text;
	}

	public long getId() {
		return this.id;
	}

	public String getText() {
		return this.text;
	}
}
