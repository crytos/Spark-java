package crytos.hadoop.ApplicationContext;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

public class ApplicationContext {
	
	
	private ApplicationContext(){}
	
	/**
	 * 
	 * @return
	 */
	static public ApplicationContext getInstance(){
		if(app == null){
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
     * @throws IOException
     */
	public Configuration getHBaseNewAPIConfiguration() throws IOException {
    	
    	Job newAPIJob = Job.getInstance(HBaseConfiguration.create());
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
    
    
    //====================================================
    //	DATA MEMBERS
    //====================================================
    
    static public ApplicationContext 							app = null;
    static private Properties 									properties = null;
    static private JavaSparkContext 							sparkContext = null;
    static private SQLContext 									sqlInstance = null;
    static private HiveContext 									hiveInstance = null;
    
    
}
