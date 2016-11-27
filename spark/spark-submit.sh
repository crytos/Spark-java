#!/bin/bash -e

SPARK_SUBMIT=/usr/bin/spark-submit

OPTS="--name APP-SPARK --class crytos.hadoop.batch.$1 --master yarn --deploy-mode cluster "

OPTS+="--files /etc/hive/conf/hive-site.xml "

#JARS="--jars /usr/jars/datanucleus-core-3.2.10.jar,"
#JARS+="/usr/jars/datanucleus-api-jdo-3.2.6.jar,/usr/jars/datanucleus-rdbms-3.2.9.jar"

echo $SPARK_SUBMIT $OPTS  target/spark.practice-0.1-jar-with-dependencies.jar

exec $SPARK_SUBMIT $OPTS  target/spark.practice-0.1-jar-with-dependencies.jar
