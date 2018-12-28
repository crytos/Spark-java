package org.kratos.srp.authentication;

import org.kratos.spark.applicationContext.ApplicationContext;

/**
 * 
 * @author kratos
 *
 */
public interface ConfigHandler {
	
	default void getConf() {
		ApplicationContext.getInstance().getLogger().info("Hello from default");
	}

}
