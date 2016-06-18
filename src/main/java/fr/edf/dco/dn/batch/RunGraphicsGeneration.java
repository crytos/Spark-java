package fr.edf.dco.dn.batch;

import fr.edf.dco.dn.contextFactory.SQLContextFactory;
import fr.edf.dco.dn.contextFactory.SparkContextFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import fr.edf.dco.dn.utils.DataTools;
import fr.edf.dco.dn.utils.PropertiesLoader;

import java.io.IOException;
import java.security.PrivilegedAction;

import static jodd.util.ClassLoaderUtil.getResourceAsStream;


/**
 * Created by Anès Mahi on 12/05/2016.
 */
public class RunGraphicsGeneration {


    public static void main(String[] args) throws IOException {

        String dbPropertiesFileName = "/properties/database.properties";

        /* Use the PropertiesLoader class to retrieve database and images properties*/
        PropertiesLoader propLoader = new PropertiesLoader(dbPropertiesFileName);

        String path = "hdfs://quickstart.cloudera:8020/user/cloudera/spark/java/";

        /*Create the application configuration and the JavaSparkContext*/
        SparkContextFactory scf = new SparkContextFactory();
        JavaSparkContext javaSparkContext = scf.getSparkContext();

        DataTools.cleanHDFSOutputDirectory();

        /*
        HiveContext hive_context = SQLContextFactory.getHiveInstance(javaSparkContext.sc());

        DataFrame customersOrders = DataTools.getCustomersOrders(hive_context);
        DataTools.saveCustomersOrdersToHDFS(customersOrders);

        DataFrame categories = DataTools.getTable(hive_context, "categories");
        categories.groupBy("category_department_id").count().javaRDD().saveAsTextFile(path + "categories_by_department");

        DataFrame orders = DataTools.getTable(hive_context, "orders");
        orders.groupBy("order_status").count().javaRDD().saveAsTextFile(path + "orders_by_status");

        DataTools.someFunctionsOnRDD(categories);*/

    }
}
