package fr.edf.dco.dn.utils;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by Anès Mahi on 13/05/2016.
 */
public class HiveUtils {

	static Path getHDFSDirectory() throws IOException {

		FileSystem hadoopFS = FileSystem.get(new Configuration());
		System.out.println(hadoopFS.getWorkingDirectory());
		System.out.println(hadoopFS.getHomeDirectory());
		return hadoopFS.getHomeDirectory();
	}

	/**
	 * @param hive_context
	 * @param dbProperties
	 * @param tableName
	 * @return
	 */

	static public DataFrame getTable(HiveContext hive_context, Properties dbProperties, String tableName) {
		return hive_context.table(dbProperties.getProperty(Constants.DB_NAME)+ "." + tableName);
	}

	static public DataFrame getCustomersOrders(HiveContext hive_context) {

		DataFrame orders = hive_context.table("sqoop_import.orders");
		DataFrame customers = hive_context.table("sqoop_import.customers").drop("customer_email").drop("customer_password");

		return orders.join(customers,customers.col("customer_id").equalTo(orders.col("order_customer_id"))).drop("customer_id");
	}

	static public void saveCustomersOrdersToHDFS(DataFrame dataFrame, Properties dbProperties, String outputFile) {

		JavaRDD<Row> customers_orderRdd = dataFrame.toJavaRDD();
		customers_orderRdd.saveAsTextFile(dbProperties.getProperty(Constants.HDFS_OUTPUT_DIRECTORY) + outputFile);
	}

	static public void cleanHDFSOutputDirectory() throws IOException {

		FileSystem hdfs = FileSystem.get(new Configuration());
		Path p = new Path(Constants.HDFS_OUTPUT_DIRECTORY + "*");
		// @TODO implement *
		if (hdfs.exists(p)) {
			hdfs.delete(p, true);
		} else
			System.out.println(p + " not find");

	}

}
