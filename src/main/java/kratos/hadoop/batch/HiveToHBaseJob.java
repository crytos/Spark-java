package kratos.hadoop.batch;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.slf4j.Logger;

import com.google.protobuf.ServiceException;

import kratos.hadoop.ApplicationContext.ApplicationContext;
import kratos.hadoop.ApplicationContext.Utils;

public class HiveToHBaseJob {

  public static void main(String[] args) throws IOException, Exception {

    ApplicationContext app = ApplicationContext.getInstance();
    Logger logger = app.getLogger();

    JavaSparkContext javaSparkContext = app.getSparkContext();

    String query = "SELECT account_num, lname, fname, gender, occupation, city, state_province, postal_code FROM foodmart.customers LIMIT 100";

    DataFrame df = Utils.getHiveData(query);

    Configuration configuration = app.getHBaseNewAPIConfiguration();

    try {
      HBaseAdmin.checkHBaseAvailable(configuration);
      logger.info("HBase is running!");
    } catch (ServiceException e) {
      logger.error("HBase connection problem " + e.getMessage());
    }

    Utils.storeCustomersDataToHbase(df);

    javaSparkContext.close();

  }

}
