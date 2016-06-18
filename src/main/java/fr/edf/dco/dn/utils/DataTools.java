package fr.edf.dco.dn.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.codehaus.groovy.runtime.powerassert.SourceText;
import scala.Tuple2;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;


/**
 * Created by Anès Mahi on 13/05/2016.
 */
public class DataTools {

    static Path getHDFSDirectory() throws IOException {

        FileSystem hadoopFS = FileSystem.get(new Configuration());
        System.out.println(hadoopFS.getWorkingDirectory());
        System.out.println(hadoopFS.getHomeDirectory());
        return hadoopFS.getHomeDirectory();
    }

    /**
     * @param hive_context
     * @param tableName
     * @return
     */

    static public DataFrame getTable(HiveContext hive_context, String tableName) {
        return hive_context.table(Constants.DB_NAME + "." + tableName);
    }

    static public DataFrame getCustomersOrders(HiveContext hive_context) {
        DataFrame orders = hive_context.table("sqoop_import.orders");
        DataFrame customers = hive_context.table("sqoop_import.customers");

        return orders.join(customers, customers.col("customer_id").equalTo(orders.col("order_customer_id"))).drop("customer_id");
    }

    static public void saveCustomersOrdersToHDFS(DataFrame dataFrame) {

        JavaRDD<Row> customers_orderRdd = dataFrame.toJavaRDD();

        List<String> customers_order = customers_orderRdd.map(new Function<Row, String>() {
            public String call(Row row) {
                return row.toString();
            }
        }).collect();

        customers_orderRdd.saveAsTextFile(Constants.HDFS_OUTPUT_DIRECTORY + "customers_orders");
    }

    static public void someFunctionsOnRDD(DataFrame dataFrame) {

        JavaRDD<Row> rdd = dataFrame.toJavaRDD();
        rdd.saveAsTextFile(Constants.HDFS_OUTPUT_DIRECTORY + "result_rdd");

        JavaRDD<Row> filteredCategories = dataFrame.javaRDD().filter(new Function<Row, Boolean>() {
            public Boolean call(Row row) throws Exception {
                return ((Integer) row.get(0) > 10) ? true : false;
            }
        });

        filteredCategories.saveAsTextFile(Constants.HDFS_OUTPUT_DIRECTORY + "filtered_categories_rdd");

        JavaRDD<Tuple2<Integer, String>> rddTuple = dataFrame.javaRDD().map(new Function<Row, Tuple2<Integer, String>>() {
            public Tuple2<Integer, String> call(Row row) throws Exception {
                return new Tuple2<Integer, String>((Integer) row.get(0), (String) row.get(2));
            }
        });

        rddTuple.saveAsTextFile(Constants.HDFS_OUTPUT_DIRECTORY + "result_rddTuple");

        /*JavaPairRDD can be obtained from JavaRDD of Tuple2 : JavaRDD<Tuple2<k, v>> */
        //JavaPairRDD<Integer, String> pairRDD = JavaPairRDD.fromJavaRDD(rddTuple);
    }


    static public void cleanHDFSOutputDirectory() throws IOException {

        FileSystem  hdfs = FileSystem.get(new Configuration());
        // @TODO implement *
        hdfs.delete(new Path(Constants.HDFS_OUTPUT_DIRECTORY+"*"), true);

    }

}

