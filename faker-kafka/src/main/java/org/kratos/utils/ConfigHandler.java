package org.kratos.utils;

/**
 * 
 * @author kratos
 *
 */
public interface ConfigHandler {
	
	String DOMAIN = "https://api.structuredretailproducts.com";

	String PUBLIC_API_KEY = "srp.api.public.key";

	String PRIVATE_API_KEY = "srp.api.private.key";
	
	String KAFKA_BOOTSTRAP_SERVER = "spark.kafka.bootstrap.server";
	
	String KAFKA_SRP_TOPIC = "spark.kafka.srp.topic";
}
