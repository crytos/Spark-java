package org.kratos.spark.batch;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;
import org.kratos.spark.applicationContext.ApplicationContext;
import org.kratos.utils.ConfigHandler;
import org.kratos.utils.Utils;
import org.slf4j.Logger;

import com.github.javafaker.Faker;

/**
 * 
 * @author kratos
 *
 */
public class FakeToKafka {

	public static void main(String[] args) {

		ApplicationContext app = ApplicationContext.getInstance();
		Logger logger = app.getLogger();

		SparkSession session = app.getSparkSession();
		logger.info(session.version());

		JavaSparkContext jsc = app.getSparkContext();

		SparkConf conf = jsc.getConf();
		String topic = conf.get(ConfigHandler.KAFKA_SRP_TOPIC);

		Map<String, Object> properties = new HashMap<String, Object>();

		properties.put("bootstrap.servers", conf.get(ConfigHandler.KAFKA_BOOTSTRAP_SERVER));
		properties.put("group.id", "srp-beta"); // What for ?
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		KafkaProducer<String, String> producer = app.getKafkaProducer(properties);

		Faker faker = Utils.getFaker();

		while (true) 
		{
			JSONObject json = Utils.fakeJsonData(faker);
			int partition = (json.getInt("transaction_id") % 2 == 0) ? 1 : 2;
			
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, partition, json.get("transaction_date").toString(), json.toString());
			// -- check order on transaction_date
			producer.send(record);
		}

	}

}
