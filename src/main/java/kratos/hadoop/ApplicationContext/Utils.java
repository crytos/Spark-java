package kratos.hadoop.ApplicationContext;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import com.google.protobuf.ServiceException;

import scala.Tuple2;

public class Utils {
	
  public static ApplicationContext app = ApplicationContext.getInstance();
	
	static public DataFrame getHiveData(ApplicationContext app, String query){
		return app.getHiveInstance(app.getSparkContext().sc()).sql(query);
	}
	
	
	@SuppressWarnings("serial")
	static public void storeCustomersDataToHbase(DataFrame df) throws IOException, Exception{
		
		Configuration conf = app.getHBaseNewAPIConfiguration();
		conf.set(TableOutputFormat.OUTPUT_TABLE, Constants.HBASE_CUSTOMERS_TABLE_NAME);
		
		JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = df.toJavaRDD().mapToPair(new PairFunction<Row, ImmutableBytesWritable, Put>() {
			
				public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {
			         
					Put put = new Put(Bytes.toBytes(row.get(0).toString()));
					put.addColumn(Bytes.toBytes("personal_data"), Bytes.toBytes("lname"),Bytes.toBytes(row.get(1).toString()));
					put.addColumn(Bytes.toBytes("personal_data"), Bytes.toBytes("fname"),Bytes.toBytes(row.get(2).toString()));
					put.addColumn(Bytes.toBytes("personal_data"), Bytes.toBytes("gender"),Bytes.toBytes(row.get(3).toString()));
					put.addColumn(Bytes.toBytes("personal_data"), Bytes.toBytes("occupation"),Bytes.toBytes(row.get(4).toString()));
					put.addColumn(Bytes.toBytes("personal_data"), Bytes.toBytes("city"),Bytes.toBytes(row.get(5).toString()));
					put.addColumn(Bytes.toBytes("personal_data"), Bytes.toBytes("state_province"),Bytes.toBytes(row.get(6).toString()));
					put.addColumn(Bytes.toBytes("personal_data"), Bytes.toBytes("postal_code"),Bytes.toBytes(row.get(7).toString()));
			     
			        return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);     
			    }
		});

		hbasePuts.saveAsNewAPIHadoopDataset(conf);
	}
	
}
