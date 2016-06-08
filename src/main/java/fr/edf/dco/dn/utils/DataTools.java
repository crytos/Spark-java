package fr.edf.dco.dn.utils;

import fr.edf.dco.dn.graphics.ImageFactory;
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
import scala.Tuple2;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;


/**
 * Created by Anès Mahi on 13/05/2016.
 */
public class DataTools {

    /**
     * @param sqlContext
     * @param dataSourceFile
     * @return
     */
    static public DataFrame csvToDf(SQLContext sqlContext, String dataSourceFile) {
        DataFrame dataSourceDF = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("header", "false").option("delimiter", ";")
                //.option("mode", "PERMISSIVE").option("charset", "UTF-8")
                //.option("quote","£")
                .schema(DataTools.createCustomSchema())
                .load(dataSourceFile);

        return dataSourceDF;
    }

    static public StructType createCustomSchema() {
        StructType customSchema = new StructType(new StructField[]{
                new StructField("Id_customer", DataTypes.StringType, true, Metadata.empty()),
                new StructField("home", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("home_similar", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("home_similar_low", DataTypes.IntegerType, true, Metadata.empty())
        });

        return customSchema;
    }

    /**
     * @param hive_context
     * @param db_name
     * @param db_table
     * @return
     */
    static public DataFrame retrieveDataFromHiveTable(HiveContext hive_context, String db_name, String db_table) {
        DataFrame result = hive_context.table("sqoop_import.categories");
        return result;
    }

    static public DataFrame retrieveDataForTest(HiveContext hive_context, String db_name, String db_table, int limit) {
        String query = "SELECT * FROM " + db_name + "." + db_table + " LIMIT " + limit;
        DataFrame result = hive_context.sql(query);
        return result;
    }


    static public void generateImages(JavaRDD<Row> dataSource, final Properties imageProperties) {

        dataSource.foreachPartition(new VoidFunction<Iterator<Row>>() {
            /* For each row in the each partition of the Java RDD  we create the corresponding image
            *  and save it in an hash map <K=BP, value=buffered Image>*/
            HashMap<String, BufferedImage> hashMapOfImages = new HashMap<String, BufferedImage>();

            public void call(Iterator<Row> rowIterator) throws Exception {
                ImageFactory imageFactory = new ImageFactory();
                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    BufferedImage img = imageFactory.createImage(row, imageProperties);
                    hashMapOfImages.put(row.get(0).toString(), img);
                }
                /*
                * Use the hash map to store images against HDFS*/
                DataTools.writeToHDFS(hashMapOfImages, imageProperties);
                //DataTools.writeToLocal(hashMapOfImages, imageProperties);
            }
        });
    }

    static public void writeToHDFS(HashMap<String, BufferedImage> hashMapOfImages, Properties imageProperties) throws IOException {

        String HDFSOutputDirectory = imageProperties.getProperty("img.output_directory");
        String fileType = imageProperties.getProperty("img.type");
        String archivePrefix = imageProperties.getProperty("img.archives.prefix");
        String archiveType = imageProperties.getProperty("img.archive.type");

        FileSystem HDFS = FileSystem.get(new Configuration());

        // A way (that can be changed) to generate unique names for the generated zip
        String suffix = hashMapOfImages.entrySet().iterator().next().getKey();
        System.out.println(suffix);

        // Create the ZIP file
        String zipName = archivePrefix + "-" + suffix.substring(0, 5) + "." + archiveType;

        String zipHDFSPath = HDFS.getHomeDirectory() + HDFSOutputDirectory + zipName;
        Path path = new Path(zipHDFSPath);
        FSDataOutputStream out = HDFS.create(path);

        // Create the zip output stream that will use the HDFS output stream
        ZipOutputStream zipOut = new ZipOutputStream(out);

        /* Iterate over an hash map having the customer ID (BP) as key and the buffered Image as value
        *  Each entry will be stored against HDFS as an image file.*/
        for (Map.Entry<String, BufferedImage> entry : hashMapOfImages.entrySet()) {

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ImageIO.write(entry.getValue(), fileType, outputStream);
            byte[] bytes = outputStream.toByteArray();

            String fileName = entry.getKey() + "-" + new Date().getTime() + "." + fileType;
            ZipEntry e = new ZipEntry(fileName);
            zipOut.putNextEntry(e);

            // Transfer bytes from the file to the ZIP file
            zipOut.write(bytes);
            zipOut.closeEntry();
        }
        zipOut.close();
        out.close();
    }

    static public void writeToLocal(HashMap<String, BufferedImage> hashMapOfImages, Properties ImageProperties) throws IOException {

        // make sur output directory exists.
        String target_directory = "output/";
        String format_sortie = "png";
        String suffix = hashMapOfImages.entrySet().iterator().next().getKey().substring(0, 5);

        String path = target_directory + "Archive-" + suffix + ".zip";

        ZipOutputStream zipOut = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(new File(path))));

        for (Map.Entry<String, BufferedImage> entry : hashMapOfImages.entrySet()) {

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ImageIO.write(entry.getValue(), format_sortie, outputStream);
            byte[] bytes = outputStream.toByteArray();

            String fileName = entry.getKey() + "-" + new Date().getTime() + "." + format_sortie;
            ZipEntry e = new ZipEntry(fileName);
            zipOut.putNextEntry(e);

            // Transfer bytes from the file to the ZIP file
            zipOut.write(bytes);
            zipOut.closeEntry();

            //BufferedImage new_img = entry.getValue();
            //File output_file = new File(target_directory + entry.getKey() + "." + format_sortie);
            /*
            try {
                if (new_img != null)
                    ImageIO.write(new_img, format_sortie, out);
            } catch (IOException e) {
                e.printStackTrace();
            }*/
        }
        zipOut.close();
    }

    static public void writeToLocal(BufferedImage new_img, String file_name) {

        String target_directory = "output/";
        String format_sortie = "png";
        File output_file = new File(target_directory + file_name + "." + format_sortie);
        try {
            if (new_img != null)
                ImageIO.write(new_img, format_sortie, output_file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static public void doingThingsWithDataFrames(DataFrame dataFrame) {

        JavaRDD<Row> rdd = dataFrame.toJavaRDD();

        List<String> categories = dataFrame.javaRDD().map(new Function<Row, String>() {
            public String call(Row row) {
                return "Categorie : " + row.getString(2);
            }
        }).collect();

        JavaRDD<Row> filteredCategories = dataFrame.javaRDD().filter(new Function<Row, Boolean>() {
            public Boolean call(Row row) throws Exception {
                return ((Integer) row.get(0) > 10) ? true : false;
            }
        });

        filteredCategories.saveAsTextFile("hdfs://quickstart.cloudera:8020/user/cloudera/output/filtered_categories_rdd.txt");


        rdd.saveAsTextFile("hdfs://quickstart.cloudera:8020/user/cloudera/output/result_rdd.txt");


        JavaRDD<Tuple2<Integer, String>> rddTuple = dataFrame.javaRDD().map(new Function<Row, Tuple2<Integer, String>>() {
            public Tuple2<Integer, String> call(Row row) throws Exception {
                return new Tuple2<Integer, String>((Integer) row.get(0), (String) row.get(2));
            }
        });

        rddTuple.saveAsTextFile("hdfs://quickstart.cloudera:8020/user/cloudera/output/result_rddTuple.txt");

        /*JavaPairRDD can be obtained from JavaRDD of Tuple2 : JavaRDD<Tuple2<k, v>> */
        JavaPairRDD<Integer, String> pairRDD = JavaPairRDD.fromJavaRDD(rddTuple);

        //pairRDD.saveAsHadoopFile("hdfs://quickstart.cloudera:8020/user/cloudera/output/result_rddTuple.txt", );
    }
}

