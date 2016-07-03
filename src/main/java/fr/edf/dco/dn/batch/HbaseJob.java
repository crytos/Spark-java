package fr.edf.dco.dn.batch;

import java.io.IOException;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;

import fr.edf.dco.dn.contextFactory.SQLContextFactory;
import fr.edf.dco.dn.contextFactory.SparkContextFactory;
import fr.edf.dco.dn.utils.HBaseUtils;

public class HbaseJob {

	public static void main(String[] args) throws IOException {
		
		JavaSparkContext javaSparkContext = SparkContextFactory.getSparkContext();
		System.out.println("RUNNING : "+javaSparkContext.appName());
		
		HiveContext hiveContext = SQLContextFactory.getHiveInstance(javaSparkContext.sc());
		hiveContext.sql("SELECT COUNT (*) FROM SQOOP_IMPORT.CUSTOMERS_ORDERS").show();
		
		HBaseUtils.scanHbaseTable();

	}

}
