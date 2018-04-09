import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;

public class JoinVCF extends Configured implements Tool {

    public static class TableJoinMapper extends TableMapper<Text, Text> {
        private static byte[] VCFTable = Bytes.toBytes("VCF");
        private static byte[] AnnotationTable = Bytes.toBytes("Annotation");
        private static final String CF_DEFAULT = "content";
        private Text mapperKey;
        private Text mapperValue;

        @Override
        public void map(ImmutableBytesWritable rowKey, Result columns, Context context) throws IOException, InterruptedException {
            TableSplit currentSplit = (TableSplit)context.getInputSplit();
            byte[] tableName = currentSplit.getTableName();
            
            mapperKey = new Text(rowKey.get());
            if (Arrays.equals(tableName, AnnotationTable)) {
                mapperValue = new Text(columns.getValue(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("cosmic")));
            } else if (Arrays.equals(tableName, VCFTable)) {
                mapperValue = new Text();
            }
            context.write(mapperKey, mapperValue);
        }
    }
    
    public static class TableJoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            String annotation = "";
            for (Text value : values) {
                annotation += value.toString();
                count += 1;
            }
            
            if (count == 2) {
                context.write(key, new Text(annotation));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JoinVCF(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        List<Scan> scans = new ArrayList<>();
        
        Scan VCFScan = new Scan();
        VCFScan.setAttribute("scan.attributes.table.name", Bytes.toBytes("VCF"));
        scans.add(VCFScan);
        
        Scan AnnotationScan = new Scan();
        AnnotationScan.setAttribute("scan.attributes.table.name", Bytes.toBytes("Annotation"));
        scans.add(AnnotationScan);
        
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "JoinVCF");
        job.setJarByClass(JoinVCF.class);
        
        TableMapReduceUtil.initTableMapperJob(scans, TableJoinMapper.class, Text.class, Text.class, job);
        
        job.setReducerClass(TableJoinReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
