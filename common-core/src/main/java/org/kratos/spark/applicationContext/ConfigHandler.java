package org.kratos.spark.applicationContext;

/**
 * 
 * @author kratos
 *
 */
public interface ConfigHandler {

	/**
	 * coming soon
	 */
	default void getConf() {
		ApplicationContext.getInstance().getLogger().info("Hello from default");
	}

}
