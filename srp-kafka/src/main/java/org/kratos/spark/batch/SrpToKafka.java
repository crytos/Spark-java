package org.kratos.spark.batch;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.JSONArray;
import org.json.XML;
import org.kratos.spark.applicationContext.ApplicationContext;
import org.kratos.srp.authentication.SrpAuthentication;
import org.slf4j.Logger;

public class SrpToKafka {

	/**
	 * Run the sample
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		ApplicationContext app = ApplicationContext.getInstance();
		Logger logger = app.getLogger();

		SparkSession session = app.getSparkSession();
		logger.info(session.version());

		JavaSparkContext jsc = app.getSparkContext();
		
//		SparkConf conf = jsc.getConf();
//		String topic = conf.get("spark.kafka.srp.topic");
//
//		Map<String, Object> properties = new HashMap<String, Object>();
//		properties.put("bootstrap.servers", conf.get("spark.kafka.bootstrap.server"));
//		properties.put("group.id", "srp-beta");
//		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//
//		KafkaProducer<String, String> producer = app.getKafkaProducer(properties);

		try {
			SrpAuthentication api = SrpAuthentication.getInstance();
			ArrayList<String> products = new ArrayList<String>();

			// 19061742
			int nbProducts = 1906;
			int current = 0;

			while (current <= nbProducts + 500) {
				String query = "/v1/products/products?count=500&from=" + current;

				logger.info("current query : " + query);
				String xmlResponse = api.fetch(query).substring(38);

				logger.info(xmlResponse);
				products.add(xmlResponse);

//				JSONArray products = XML.toJSONObject(xmlResponse).getJSONObject("products").getJSONObject("list").getJSONArray("product");

				// Deal with JSonArray !

//				ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, 0, null, "THE_KEY", products.toString());
//				producer.send(record);

				current += 500;
			}

			if (!products.isEmpty()) {
				jsc.parallelize(products).saveAsTextFile("srp_products_" + nbProducts);
			}

		} catch (Exception e) {
			System.out.println(e.getMessage());
		}

		session.close();
	}
}
