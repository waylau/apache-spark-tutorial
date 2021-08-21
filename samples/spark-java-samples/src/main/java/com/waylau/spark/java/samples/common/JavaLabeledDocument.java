/**
 * Welcome to https://waylau.com
 */
package com.waylau.spark.java.samples.common;

/**
 * Java Labeled Document
 * @since 1.0.0 2021年8月21日
 * @author <a href="https://waylau.com">Way Lau</a>
 */
public class JavaLabeledDocument extends JavaDocument {

	private double label;

	public JavaLabeledDocument(long id, String text,
			double label) {
		super(id, text);
		this.label = label;
	}

	public double getLabel() {
		return this.label;
	}

}