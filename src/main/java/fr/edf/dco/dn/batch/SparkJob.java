package fr.edf.dco.dn.batch;

import java.io.IOException;
import java.util.Properties;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

import fr.edf.dco.dn.contextFactory.SQLContextFactory;
import fr.edf.dco.dn.contextFactory.SparkContextFactory;
import fr.edf.dco.dn.utils.Constants;
import fr.edf.dco.dn.utils.HiveUtils;
import fr.edf.dco.dn.utils.PropertiesLoader;


/**
 * Created by Anès Mahi on 12/05/2016.
 */
public class SparkJob {

		
	public static void main(String[] args) throws IOException {

		PropertiesLoader propLoader = new PropertiesLoader(dbPropertiesFileName);
		Properties dbProperties = propLoader.getDbProp();

		JavaSparkContext javaSparkContext = SparkContextFactory.getSparkContext();
		HiveContext hiveContext = SQLContextFactory.getHiveInstance(javaSparkContext.sc());

		//HiveUtils.cleanHDFSOutputDirectory();
		callHiveMethods(hiveContext, dbProperties);	

	}
	
	public static void callHiveMethods(HiveContext hiveContext, Properties dbProperties){
		
		DataFrame customersOrders = HiveUtils.getCustomersOrders(hiveContext);
		customersOrders.write().mode(SaveMode.Append).saveAsTable(dbProperties.getProperty(Constants.DB_NAME) + "."+ Constants.CUSTOMERS_ORDERS);

		HiveUtils.saveCustomersOrdersToHDFS(customersOrders, dbProperties, Constants.CUSTOMERS_ORDERS);

		DataFrame categories = HiveUtils.getTable(hiveContext, dbProperties, "categories");
		categories.groupBy(new Column("category_department_id")).count().show();

		DataFrame orders = HiveUtils.getTable(hiveContext, dbProperties, "orders");
		orders.groupBy(new Column("order_status")).count().show();
	}
	
	
	private static String dbPropertiesFileName = Constants.DB_PROPERTIES_FILE_NAME;

}
