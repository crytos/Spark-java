#!/bin/bash -e

SPARK_SUBMIT=/usr/bin/spark-submit

OPTS="--name APP-SPARK --class fr.edf.dco.dn.batch.RunSparkSubmit --master yarn --deploy-mode cluster "

OPTS+="--files /etc/hive/conf/hive-site.xml "

JARS="--jars /usr/jars/datanucleus-core-3.2.10.jar,"
JARS+="/usr/jars/datanucleus-api-jdo-3.2.6.jar,/usr/jars/datanucleus-rdbms-3.2.9.jar"

echo $SPARK_SUBMIT $OPTS $JARS target/POC13-1.0-SNAPSHOT-jar-with-dependencies.jar

exec $SPARK_SUBMIT $OPTS $JARS target/POC13-1.0-SNAPSHOT-jar-with-dependencies.jar