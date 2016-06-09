package fr.edf.dco.dn.batch;

import fr.edf.dco.dn.contextFactory.SQLContextFactory;
import fr.edf.dco.dn.contextFactory.SparkContextFactory;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import fr.edf.dco.dn.utils.DataTools;
import fr.edf.dco.dn.utils.PropertiesLoader;

import java.io.IOException;

import static jodd.util.ClassLoaderUtil.getResourceAsStream;


/**
 * Created by Anès Mahi on 12/05/2016.
 */
public class RunGraphicsGeneration {


    public static void main(String[] args) throws IOException {

        String dbPropertiesFileName = "/properties/database.properties";
        String imagesPropertiesFileName = "/properties/images.properties";

        /* Use the PropertiesLoader class to retrieve database and images properties*/
        PropertiesLoader propLoader = new PropertiesLoader(dbPropertiesFileName, imagesPropertiesFileName);

        String dbName = propLoader.getDbProp().getProperty("db.name");
        String dbTable = propLoader.getDbProp().getProperty("db.table.name");

        /*Creation the application configuration and the JavaSparkContext*/
        SparkContextFactory scf = new SparkContextFactory("Poc13"); //add appName in properties
        JavaSparkContext javaSparkContext = scf.getSparkContext();

        /* Create the HiveContext and load data to be used in images generation.*/
        HiveContext hive_context = SQLContextFactory.getHiveInstance(JavaSparkContext.toSparkContext(javaSparkContext));
        DataFrame dataSource = DataTools.retrieveDataFromHiveTable(hive_context, dbName, dbTable);

        DataTools.doingThingsWithDataFrames(dataSource);

        /* Use the JavaRDD struct to create images*/
        //Properties imageProperties = propLoader.getImagesProp();
        // Try the coalesce method applied to the JavaRDD
        //DataTools.generateImages(dataSource.repartition(Integer.valueOf(args[1])).toJavaRDD(), imageProperties);

    }
}
