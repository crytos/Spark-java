package org.kratos.spark.applicationContext;

/**
 * 
 * @author kratos
 *
 */
public interface ConfigHandler {

	String KAFKA_BOOTSTRAP_SERVER = "spark.kafka.bootstrap.server";
	
	default void getConf() {
		ApplicationContext.getInstance().getLogger().info("Hello from default");
	}

}
