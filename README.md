## Download instructions

```
Make sure these components are installed:
- JDK 1.8 - brew
- Scala 2.11.12 - https://downloads.lightbend.com/scala/2.11.12/scala-2.11.12.tgz
- Hadoop 2.9.1 - https://archive.apache.org/dist/hadoop/core/hadoop-2.9.1/
- Spark 2.3.3 (without bundled Hadoop) - https://www.apache.org/dyn/closer.lua/spark/spark-2.3.3/spark-2.3.3-bin-without-hadoop.tgz
- Maven - brew
- AWS CLI (for EMR execution) - brew install awscli
```

Environment setup

```
1) Example ~/.bash_profile:
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export HADOOP_HOME=/home/joe/tools/hadoop/hadoop-2.9.1
export SCALA_HOME=/home/joe/tools/scala/scala-2.11.12
export SPARK_HOME=/home/joe/tools/spark/spark-2.3.1-bin-without-hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SCALA_HOME/bin:$SPARK_HOME/bin
export SPARK_DIST_CLASSPATH=$(hadoop classpath)

2) Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh:
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
```

