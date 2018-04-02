# VCF-Hbase

Annotate VCF file using Hbase framework.

## Environment

The code has been tested on a virtual machine with the following configurations:

* Ubuntu 16.04.4 LTS
* Java openjdk version "1.8.0_151"
* Hadoop 2.7.5
* Hbase 1.3.2

Both Hadoop and Hbase are configured in Pseudo-Distributed mode.

## Developing

We will take the assumption that Annotation Table resides in Hbase database and can be continually updated. User will use VCF file as input, and the output will be the annotated VCF.

### Table Schema

For the Annotation Table, we will design its schema and load some sample data for testing. This is done by `HbaseVCF.java` and `TableModel.java`.

The row key for Annotation Table has the design like this:

```
chrm.start.end.alt_base
```

And there will only be one column family `cosmic` and a single column qualifier `content` to store the annotation for this variant.

The benefit to have this schema design is that, first of all, it will have quick read access for the query in our problem. Ref base is not included as a part of row key, because it is already certain if we have specified both start and end position in a chromosome. With the query has the same structure as row key, we will be able to take the full advantage of NoSQL database system to fetch the content efficiently. Secondly, since everything in Hbase is sorted in order of row key, our data will be naturally grouped by chromosome and in its order of location.

Our data, for example, will have format like this:

Rowkey | ColumnFamly
------------ | :-----------:
| | Column
9.95121497.95121498.GA | ID=COSN212065;OCCURENCE=1(breast)
X.79951432.79951433.AA | ID=COSM1558810;OCCURENCE=1(lung)
X.107554022.107554023.AA | ID=COSM1650981,COSM1145489;OCCURENCE=1(lung)
... | ...

### Task Mapping

Since each VCF Table to be annotated is generally huge, it is necessary to annotate them in parallel. We can take the advantage of Hadoop MapReduce framework to speed up this process. `AnnotateVCF.java` will implement this task.

User can first put the input file into HDFS. In this sample implementation, the `AnnotateMapper` processes one line from input file at a time. The mapper will construct a query in the format mentioned above, send a query to Hbase, and get a result if the corresponding annotation exists. This operation can be further optimized in batch operation to process multiple lines at a time in future.

```java
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    ...
    Get g = new Get(Bytes.toBytes(rowKey));
    g.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes(ATTR));
    Result result = table.get(g);

    ...
    context.write(mutation, annotation);
}
```

The KeyValue pair emits is simply the row key and its cell in Hbase, and they are the combination we desired for annotation. There is no need to furtherly process this KeyValue, we can omit the reducer and directly set the pair as the final output.

```java
job.setNumReduceTasks(0);
```

## Building

First build `HbaseVCF` to load the sample Annotation Table data:

```
$ javac HbaseVCF.java TableModel.java
$ java HbaseVCF
```

Then build Hadoop job to map input data to annotate:
```
$ hadoop com.sun.tools.javac.Main AnnotateVCF.java
$ jar cf av.jar AnnotateVCF*.class

$ hadoop jar av.jar AnnotateVCF /input /output
```

## Reference

* [Apache HBase ™ Reference Guide](http://hbase.apache.org/book.html)
* [Apache Hadoop 2.7.5](http://hadoop.apache.org/docs/r2.7.5/)
* Google [Cloud Bigtable Documentation](https://cloud.google.com/bigtable/docs/)
* [HBase Tutorial](https://www.tutorialspoint.com/hbase/index.htm)
