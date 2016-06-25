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
import fr.edf.dco.dn.utils.DataTools;
import fr.edf.dco.dn.utils.PropertiesLoader;

/**
 * Created by Anès Mahi on 12/05/2016.
 */
public class RunSparkSubmit {

	public static void main(String[] args) throws IOException {

		String dbPropertiesFileName = "/properties/database.properties";

		/*
		 * Use the PropertiesLoader class to retrieve database and images
		 * properties
		 */
		PropertiesLoader propLoader = new PropertiesLoader(dbPropertiesFileName);
		Properties dbProperties = propLoader.getDbProp();

		/* Create the application configuration and the JavaSparkContext */
		JavaSparkContext javaSparkContext = SparkContextFactory
				.getSparkContext();

		// DataTools.cleanHDFSOutputDirectory();

		HiveContext hive_context = SQLContextFactory
				.getHiveInstance(javaSparkContext.sc());

		DataFrame customersOrders = DataTools.getCustomersOrders(hive_context);
		customersOrders
				.write()
				.mode(SaveMode.Append)
				.saveAsTable(
						dbProperties.getProperty(Constants.DB_NAME) + "."
								+ Constants.CUSTOMERS_ORDERS);

		DataTools.saveCustomersOrdersToHDFS(customersOrders, dbProperties,
				Constants.CUSTOMERS_ORDERS);

		DataFrame categories = DataTools.getTable(hive_context, dbProperties,
				"categories");
		categories.groupBy(new Column("category_department_id")).count().show();

		DataFrame orders = DataTools.getTable(hive_context, dbProperties,
				"orders");
		orders.groupBy(new Column("order_status")).count().show();

	}
}
