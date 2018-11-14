package org.kratos.spark.applicationContext;

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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ServiceException;

public class ApplicationContext {

  private ApplicationContext() {
  }

  /**
   * Custom entry point to spark application
   * @return app
   */
  static public ApplicationContext getInstance() {
    if (app == null) {
      app = new ApplicationContext();
    }
    return app;
  }
  
  /**
   * 
   * @return singleton spark session
   */
  public SparkSession getSparkSession() {
	  return SparkSession.builder().enableHiveSupport().getOrCreate();
  }
  
  /**
   * 
   * @param batchDuration
   * @return
   */
  public JavaStreamingContext getStreamingContext(Duration batchDuration) {
	  if(jstreamingContext == null)
		  jstreamingContext = new JavaStreamingContext(app.getSparkContext(), batchDuration);
	  return jstreamingContext;
  }

  /**
   * 
   * @return JavaSparkContext given already existing spark session
   */
  public JavaSparkContext getSparkContext() {
    if (jsparkContext == null) 
      jsparkContext = new JavaSparkContext(app.getSparkSession().sparkContext());
    return jsparkContext;
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

    conf.set(ConfigHandler.HBASE_ZOOKEEPER_QUORUM, "");

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
  static private JavaSparkContext jsparkContext = null;
  static private JavaStreamingContext jstreamingContext = null;
  static private Logger logger = LoggerFactory.getLogger(ApplicationContext.class);
  
}
