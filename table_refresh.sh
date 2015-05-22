#!/bin/sh -ex

function join { local IFS="$1"; shift; echo "$*"; }

function new_table {
    maprcli table delete -path "$TABLE_PATH"
    maprcli table create -path "$TABLE_PATH"
    maprcli table cf create -path "$TABLE_PATH" -cfname cf1
}

TABLE_PATH="/tmp/ticks"
# INPUT_FILE="src/test/resources/data.1M.csv"
SYNC_OR_ASYNC="async"
INPUT_FILE="/mapr/vgonzalez.spark/tmp/s20150518_1M/0_0_0.csv"
CP=`join : $(ls target/jackson*.jar target/HBaseExample*.jar target/async-*jar target/asynchbase*.jar)`

for sync_flag in async sync; do
    echo "100K rows: $sync_flag"
    new_table
    java -cp $CP:`hbase classpath` com.mapr.hadoop.HBaseExample "$TABLE_PATH" "/mapr/vgonzalez.spark/tmp/s20150518_100K/0_0_0.csv" "$SYNC_OR_ASYNC"

    echo "1 million rows: $sync_flag"
    new_table
    java -cp $CP:`hbase classpath` com.mapr.hadoop.HBaseExample "$TABLE_PATH" "/mapr/vgonzalez.spark/tmp/s20150518_1M/0_0_0.csv" "$SYNC_OR_ASYNC"

    echo "10 million rows: $sync_flag"
    new_table
    java -cp $CP:`hbase classpath` com.mapr.hadoop.HBaseExample "$TABLE_PATH" "/mapr/vgonzalez.spark/tmp/s20150518_10M/0_0_0.csv" "$SYNC_OR_ASYNC"

    #echo "100 million rows: $sync_flag"
    #new_table
    #java -cp $CP:`hbase classpath` com.mapr.hadoop.HBaseExample "$TABLE_PATH" "/mapr/vgonzalez.spark/tmp/s20150518/0_0_0.csv" "$SYNC_OR_ASYNC"
done
