package org.kratos.srp.authentication;

import org.kratos.spark.applicationContext.ApplicationContext;

/**
 * 
 * @author kratos
 *
 */
public interface ConfigHandler {
	
	/**
	 * Domain name
	 * 
	 */
	final String DOMAIN = "https://api.structuredretailproducts.com";

	/**
	 * Public API key
	 * 
	 */
	final String PUBLIC_API_KEY = "";

	/**
	 * Private API key
	 * 
	 */
	final String PRIVATE_API_KEY = "";
	
	
	default void getConf() {
		ApplicationContext.getInstance().getLogger().info("Hello from default");
	}

}
