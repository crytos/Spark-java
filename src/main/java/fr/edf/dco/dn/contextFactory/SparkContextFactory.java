package fr.edf.dco.dn.contextFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by Anès Mahi on 12/05/2016.
 */
public class SparkContextFactory {

    static public SparkConf sparkConf;


    public SparkContextFactory(String appName) {
        this.sparkConf = new SparkConf().setAppName(appName);//.setMaster("local[*]");
    }

    static private JavaSparkContext sparkContext = null;

    static public JavaSparkContext getSparkContext() {
        if (sparkContext == null) {
            sparkContext = new JavaSparkContext(sparkConf);
        }
        return sparkContext;
    }

}
