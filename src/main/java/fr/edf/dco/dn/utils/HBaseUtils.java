package fr.edf.dco.dn.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by cloudera on 7/1/16.
 */
public class HBaseUtils {

	public static void scanHbaseTable() throws IOException {

		Configuration configuration = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(configuration);

		Table table = connection.getTable(TableName.valueOf("hbase_tutorial"));
		Get get = new Get("1".getBytes());
		Result result = table.get(get);

		String name = Bytes.toString(result.getValue(Bytes.toBytes("personal_data"), Bytes.toBytes("family_name")));
		String weight = Bytes.toString(result.getValue(Bytes.toBytes("personal_data"), Bytes.toBytes("weight")));

		System.out.println("HBASE RESULT : " + name + " ===> " + weight);

	}

}
