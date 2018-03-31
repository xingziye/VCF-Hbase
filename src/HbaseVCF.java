import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.fs.Path;

public class HbaseVCF {

    private static final String TABLE_NAME = "Annotation";
    private static final String CF_DEFAULT = "cosmic";
    private static final String ATTR = "content";

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();

        // Add any necessary configuration files (hbase-site.xml, core-site.xml)
        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));

        TableModel db = new TableModel(config);
        db.createSchemaTables(TABLE_NAME, CF_DEFAULT);

        db.insertData(TABLE_NAME, "9.95121497.95121498.GA", CF_DEFAULT, ATTR, "ID=COSN212065;OCCURENCE=1(breast)");
        db.insertData(TABLE_NAME, "X.79951432.79951433.AA", CF_DEFAULT, ATTR, "ID=COSM1558810;OCCURENCE=1(lung)");
        db.insertData(TABLE_NAME, "X.107554022.107554023.AA", CF_DEFAULT, ATTR, "ID=COSM1650981,COSM1145489;OCCURENCE=1(lung)");
        db.insertData(TABLE_NAME, "X.110495625.110495626.AG", CF_DEFAULT, ATTR, "ID=COSM363499;OCCURENCE=1(lung)");
        db.insertData(TABLE_NAME, "9.120476556.120476557.AT", CF_DEFAULT, ATTR, "ID=COSM1145195;OCCURENCE=1(lung)");
        db.insertData(TABLE_NAME, "18.61305197.61305198.AA", CF_DEFAULT, ATTR, "ID=COSM400195;OCCURENCE=1(lung)");
        db.insertData(TABLE_NAME, "5.112174845.112174846.-", CF_DEFAULT, ATTR, "ID=COSM18943;OCCURENCE=1(large_intestine)");
        db.insertData(TABLE_NAME, "11.33358691.33358692.TT", CF_DEFAULT, ATTR, "ID=COSM133430;OCCURENCE=1(ovary)");
    }
}
