import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

public class TableModel {

    private static Configuration config;

    public TableModel(Configuration conf) {
        config = conf;
    }

    private static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            System.out.println("Table exists. Deleting.");
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    public void createSchemaTables(String tableName, String... columnFamilyNames) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {

            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf : columnFamilyNames) {
                table.addFamily(new HColumnDescriptor(cf).setCompressionType(Algorithm.NONE));
            }

            System.out.println("Creating table " + tableName);
            createOrOverwrite(admin, table);
            System.out.println("Done.");
        }
    }

    public void insertData(String tableName, String key, String cf, String attr, String value) throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf(tableName))) {

            Put p = new Put(Bytes.toBytes(key));
            p.addColumn(Bytes.toBytes(cf), Bytes.toBytes(attr), Bytes.toBytes(value));
            table.put(p);

            System.out.println("data inserted");
        }
    }
}
