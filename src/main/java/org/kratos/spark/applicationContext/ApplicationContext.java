package kratos.hadoop.ApplicationContext;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ServiceException;

public class ApplicationContext {

  private ApplicationContext() {
  }

  /**
   * 
   * @return
   */
  static public ApplicationContext getInstance() {
    if (app == null) {
      app = new ApplicationContext();
    }
    return app;
  }

  /**
   * 
   * @return
   */
  public JavaSparkContext getSparkContext() {
    if (sparkContext == null) {
      sparkContext = new JavaSparkContext(new SparkConf());
    }
    return sparkContext;
  }

  /**
   * 
   * @param javaSparkContext
   * @return
   */
  public SQLContext getInstance(JavaSparkContext javaSparkContext) {

    if (sqlInstance == null) {
      sqlInstance = new SQLContext(javaSparkContext);
    }
    return sqlInstance;
  }

  /**
   * 
   * @param sparkContext
   * @return
   */
  public HiveContext getHiveInstance(SparkContext sparkContext) {
    if (hiveInstance == null) {
      hiveInstance = new HiveContext(sparkContext);
    }
    return hiveInstance;
  }

  /**
   * 
   * @param hBaseTableName
   * @return
   * @throws ServiceException
   * @throws ZooKeeperConnectionException
   * @throws MasterNotRunningException
   * @throws IOException
   */
  public Configuration getHBaseNewAPIConfiguration()
      throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException {

    if (conf == null)
      conf = HBaseConfiguration.create();

    conf.set(Constants.HBASE_ZOOKEEPER_QUORUM, "");

    HBaseAdmin.checkHBaseAvailable(conf);
    logger.info("HBase is running !");

    Job newAPIJob = Job.getInstance(conf);
    newAPIJob.setOutputFormatClass(TableOutputFormat.class);

    return newAPIJob.getConfiguration();
  }

  /**
   * 
   * @param propertiesFileName
   * @return
   * @throws IOException
   */
  public Properties loadPropertiesFile(String propertiesFileName) throws IOException {

    properties = new Properties();
    InputStream inputStream = getClass().getResourceAsStream(propertiesFileName);

    if (inputStream != null) {
      properties.load(inputStream);
    } else {
      throw new FileNotFoundException("property file '" + propertiesFileName + "' not found in the classpath");
    }
    
    return properties;
  }
  
  public Logger getLogger(){
    return logger;
  }

  /*
   * ================ DATA MEMBERS ================================
   */

  static private ApplicationContext app = null;
  static private Configuration conf = null;
  static private Properties properties = null;
  static private JavaSparkContext sparkContext = null;
  static private SQLContext sqlInstance = null;
  static private HiveContext hiveInstance = null;
  static private Logger logger = LoggerFactory.getLogger(ApplicationContext.class);

}
