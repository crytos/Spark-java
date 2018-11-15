package org.kratos.spark.applicationContext;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationContext {

	private ApplicationContext() {
	}

	/**
	 * Custom entry point to spark application
	 * 
	 * @return singleton object of the ApplicationContext
	 */
	static public ApplicationContext getInstance() {
		if (app == null) {
			app = new ApplicationContext();
		}
		return app;
	}

	/**
	 * 
	 * @return singleton spark session
	 */
	public SparkSession getSparkSession() {
		return SparkSession.builder().enableHiveSupport().getOrCreate();
	}

	/**
	 * 
	 * @param batchDuration
	 * @return
	 */
	public JavaStreamingContext getStreamingContext(Duration batchDuration) {
		if (jstreamingContext == null)
			jstreamingContext = new JavaStreamingContext(app.getSparkContext(), batchDuration);
		return jstreamingContext;
	}

	/**
	 * 
	 * @return JavaSparkContext given already existing spark session
	 */
	public JavaSparkContext getSparkContext() {
		if (jsparkContext == null)
			jsparkContext = new JavaSparkContext(app.getSparkSession().sparkContext());
		return jsparkContext;
	}

	/**
	 * 
	 * @param properties which are generated inside the main class
	 * @return singleton object of KafkaProducer
	 */
	public KafkaProducer<String, String> getKafkaProducer(Map<String, Object> properties) {
		if (producer == null)
			producer = new KafkaProducer<>(properties);
		return producer;
	}

	/**
	 * 
	 * @param propertiesFileName
	 * @return
	 * @throws IOException
	 */
	public Properties loadPropertiesFile(String propertiesFileName) throws IOException {

		properties = new Properties();
		InputStream inputStream = getClass().getResourceAsStream(propertiesFileName);

		if (inputStream != null) {
			properties.load(inputStream);
		} else {
			throw new FileNotFoundException("property file '" + propertiesFileName + "' not found in the classpath");
		}

		return properties;
	}

	public Logger getLogger() {
		return logger;
	}

	/*
	 * ================ DATA MEMBERS ================================
	 */

	static private ApplicationContext app = null;
	static private Properties properties = null;
	static private JavaSparkContext jsparkContext = null;
	static private JavaStreamingContext jstreamingContext = null;
	static private Logger logger = LoggerFactory.getLogger(ApplicationContext.class);
	static private KafkaProducer<String, String> producer = null;

}
