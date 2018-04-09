import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.fs.Path;

public class HbaseVCF {

    private static final String CF_DEFAULT = "content";

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();

        // Add any necessary configuration files (hbase-site.xml, core-site.xml)
        // config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        // config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));

        TableModel db = new TableModel(config);
        db.createSchemaTables("Annotation", CF_DEFAULT);

        db.insertData("Annotation", "9.95121497.95121498.GA", CF_DEFAULT, "cosmic", "ID=COSN212065;OCCURENCE=1(breast)");
        db.insertData("Annotation", "X.79951432.79951433.AA", CF_DEFAULT, "cosmic", "ID=COSM1558810;OCCURENCE=1(lung)");
        db.insertData("Annotation", "X.107554022.107554023.AA", CF_DEFAULT, "cosmic", "ID=COSM1650981,COSM1145489;OCCURENCE=1(lung)");
        db.insertData("Annotation", "X.110495625.110495626.AG", CF_DEFAULT, "cosmic", "ID=COSM363499;OCCURENCE=1(lung)");
        db.insertData("Annotation", "9.120476556.120476557.AT", CF_DEFAULT, "cosmic", "ID=COSM1145195;OCCURENCE=1(lung)");
        db.insertData("Annotation", "18.61305197.61305198.AA", CF_DEFAULT, "cosmic", "ID=COSM400195;OCCURENCE=1(lung)");
        db.insertData("Annotation", "5.112174845.112174846.-", CF_DEFAULT, "cosmic", "ID=COSM18943;OCCURENCE=1(large_intestine)");
        db.insertData("Annotation", "11.33358691.33358692.TT", CF_DEFAULT, "cosmic", "ID=COSM133430;OCCURENCE=1(ovary)");

        TableModel vcf = new TableModel(config);
        vcf.createSchemaTables("VCF", CF_DEFAULT);

        vcf.insertData("VCF", "3.51892983.51892983.A", CF_DEFAULT, "ref_base", "G");
        vcf.insertData("VCF", "8.82831052.82831052.A", CF_DEFAULT, "ref_base", "G");
        vcf.insertData("VCF", "9.95121497.95121498.GA", CF_DEFAULT, "ref_base", "TT");
        vcf.insertData("VCF", "11.33358691.33358692.TT", CF_DEFAULT, "ref_base", "GA");
        vcf.insertData("VCF", "19.55522302.55522302.A", CF_DEFAULT, "ref_base", "G");
        vcf.insertData("VCF", "X.79951432.79951433.AA", CF_DEFAULT, "ref_base", "TT");
        vcf.insertData("VCF", "X.107554022.107554023.AA", CF_DEFAULT, "ref_base", "CC");
        vcf.insertData("VCF", "X.110495625.110495626.AG", CF_DEFAULT, "ref_base", "TC");
    }
}
