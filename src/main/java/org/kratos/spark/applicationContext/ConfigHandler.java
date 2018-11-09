package org.kratos.spark.applicationContext;

/**
 * 
 * @author kratos
 *
 */
public interface ConfigHandler {

  String HIVE_DB_NAME = "foodmart";
  String HIVE_CUSTOMERS_TABLE_NAME = "customers";

  String HBASE_CUSTOMERS_TABLE_NAME = "customers";

  String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
  
  
  default void getConf() {
	  ApplicationContext.getInstance().getLogger().info("Hello from default");
  }
  
}
