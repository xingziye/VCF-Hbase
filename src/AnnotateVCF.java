import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class AnnotateVCF extends Configured implements Tool {

    public static class AnnotateMapper extends Mapper<Object, Text, Text, Text> {
        private static final String TABLE_NAME = "Annotation";
        private static final String CF_DEFAULT = "content";
        private static final String ATTR = "cosmic";
        private Table table;
        private Text mutation = new Text();
        private Text annotation = new Text();

        @Override
        public void setup(Context context) throws IOException {
            Configuration config = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(TABLE_NAME));
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] attrs = value.toString().split(" ");
            String rowKey = String.join(".", attrs[0], attrs[1], attrs[2], attrs[4]);
            mutation.set(rowKey);
            Get g = new Get(Bytes.toBytes(rowKey));
            g.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(ATTR));
            Result result = table.get(g);

            if (!result.isEmpty()) {
                byte [] cell = result.getValue(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(ATTR));
                annotation.set(Bytes.toString(cell));
                context.write(mutation, annotation);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException {
            table.close();
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new AnnotateVCF(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        // Add any necessary configuration files (hbase-site.xml, core-site.xml)
        // conf.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        // conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        Job job = Job.getInstance(conf, "AnnotateVCF");
        job.setJarByClass(AnnotateVCF.class);
        job.setMapperClass(AnnotateMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
