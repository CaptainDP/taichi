#!/usr/bin/env bash
##-*-coding: utf-8; mode: shell-script;-*-##
set -e
umask 0000
APP_HOME="$(cd $(dirname $0)/..; pwd -P)"

set -a
source "$APP_HOME/etc/run.env"
set +a

if [[ $# -eq 2 ]]; then
    dt=$(date -d "$1" +%Y%m%d)
    conffile="$2"
else
    echo "$0 date conffile"
    exit 1
fi

export LOG_FILE=$conffile
export LOG_DATE=$dt

logging "base date $dt to process: $conffile"

filelist="$APP_HOME/conf/taichi.json,$APP_HOME/conf/log4j.properties,$APP_HOME/conf/$conffile"

CLASSPATH="$APP_HOME/conf:$APP_HOME/etc:$APP_HOME/libs/*"

cls="$APP_CLS"
jar="$APP_HOME/libs/$APP_JAR"

cmd="$SPARK_HOME/bin/spark-submit"
cmd="$cmd --conf spark.driver.extraClassPath=$CLASSPATH"
cmd="$cmd --conf spark.sql.warehouse.dir=file://$(pwd)/sparksql/warehouse"
cmd="$cmd --conf spark.app.name=taichi-$conffile-$dt"
cmd="$cmd --conf spark.yarn.submit.waitAppCompletion=true"
cmd="$cmd --files ${filelist}"
#cmd="$cmd --queue bigdata"
cmd="$cmd --num-executors 2 --executor-cores 2 --executor-memory 1GB"

cmd="$cmd --master local[*]"
#cmd="$cmd --master yarn"
#cmd="$cmd --master spark://10.174.101.32:7077"

#cmd="$cmd --deploy-mode client"
#cmd="$cmd --deploy-mode cluster"

#cmd="$cmd --principal $KEYTAB_USER --keytab $KEYTAB_FILE"
cmd="$cmd --conf spark.port.maxRetries=30"
cmd="$cmd --conf spark.speculation=true"
cmd="$cmd --conf spark.authenticate=true"
#cmd="$cmd --conf spark.authenticate.secret=passwd"
cmd="$cmd --name taichi-$conffile-$dt"
cmd="$cmd --class $cls $jar"
cmd="$cmd  $dt"
cmd="$cmd  $conffile"

logging "command to execute: $cmd"
LOG_DIR=$LOG_HOME/$dt
mkdir -p $LOG_DIR
$cmd &>> $LOG_DIR/$dt.log
#$APP_HOME/bin/runner "$cmd"

logging 'all OK'
