export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre
export PATH=$PATH:$JAVA_HOME/bin

export HADOOP_HOME=/usr/local/hadoop
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
export HADOOP_INSTALL=$HADOOP_HOME

export HBASE_HOME=/usr/local/hbase
export CLASSPATH=$CLASSPATH:$HBASE_HOME/lib

export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HBASE_CONF_DIR=$HBASE_HOME/conf

export HADOOP_CLASSPATH=`${HBASE_HOME}/bin/hbase classpath`:/usr/lib/jvm/java-8-openjdk-amd64/lib/tools.jar
