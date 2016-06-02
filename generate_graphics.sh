#!/bin/bash -e

SPARK_SUBMIT=/usr/bin/spark-submit

OPTS="--class fr.edf.dco.dn.graphics.GraphicsGen --master yarn --deploy-mode cluster --num-executors 5 --executor-memory 10G"

#OPTS+=" --files /etc/hive/conf/hive-site.xml"

## This following line is used instead of the above one only in th DN cluster due to syntax errors in hive-site.xml. 
## Otherwise, please use the one above

OPTS+=" --files hive-site.xml"

OPTS+=" --jars /usr/hdp/current/hive-client/lib/datanucleus-core-3.2.10.jar,/usr/hdp/current/hive-client/lib/datanucleus-api-jdo-3.2.6.jar,/usr/hdp/current/hive-client/lib/datanucleus-rdbms-3.2.9.jar"

NB_RECORDS=$1
NB_ARCHIVES=$2

echo $SPARK_SUBMIT $OPTS POC13-1.0-SNAPSHOT-jar-with-dependencies.jar $NB_RECORDS $NB_ARCHIVES

exec $SPARK_SUBMIT $OPTS POC13-1.0-SNAPSHOT-jar-with-dependencies.jar $NB_RECORDS $NB_ARCHIVES
