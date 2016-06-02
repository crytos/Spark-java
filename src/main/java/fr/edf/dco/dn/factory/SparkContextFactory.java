package fr.edf.dco.dn.factory;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by Anès Mahi on 12/05/2016.
 */
public class SparkContextFactory {

    public SparkConf sparkConf;


    public SparkContextFactory(String appName) {
        this.sparkConf = this.createSparkConf(appName);
    }

    private SparkConf createSparkConf(String appName) {
        return new SparkConf().setAppName(appName);
                //.setMaster("local[*]");
    }

    private SparkContext createSparkContext(String master, String appName) {
        return new SparkContext(this.sparkConf);
    }

    private JavaSparkContext createJavaSparkContext() {
        return new JavaSparkContext(this.sparkConf);
    }

    public JavaSparkContext create() {
        return createJavaSparkContext();
    }

    static private transient JavaSparkContext sparkContext = null;

    static public JavaSparkContext getSparkContext() {
        if (sparkContext == null) {
            sparkContext = new JavaSparkContext();
        }
        return sparkContext;
    }

}
