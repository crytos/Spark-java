package fr.edf.dco.dn.contextFactory;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by Anès Mahi on 12/05/2016.
 */

public class SQLContextFactory {

    static private SQLContext sql_instance = null;

    static public SQLContext getInstance(JavaSparkContext java_spark_context) {

        if (sql_instance == null) {
            sql_instance = new SQLContext(java_spark_context);
        }
        return sql_instance;
    }

    static private HiveContext hive_instance = null;

    static public HiveContext getHiveInstance(SparkContext spark_context) {
        if (hive_instance == null) {
            hive_instance = new HiveContext(spark_context);
        }
        return hive_instance;

    }

}
