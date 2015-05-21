#!/bin/sh -ex

TABLE_PATH="/user/vgonzalez/ticks"
INPUT_FILE="/mapr/se1/user/vgonzalez/s20150518/0_0_0.csv"

mvn package

maprcli table delete -path "$TABLE_PATH"
maprcli table create -path "$TABLE_PATH"
maprcli table cf create -path "$TABLE_PATH" -cfname cf1

#maprcli table cf create -path "$TABLE_PATH" -cfname opens
#maprcli table cf create -path "$TABLE_PATH" -cfname highs
#maprcli table cf create -path "$TABLE_PATH" -cfname lows
#maprcli table cf create -path "$TABLE_PATH" -cfname closes
#maprcli table cf create -path "$TABLE_PATH" -cfname volumes

time java -cp target/HBaseExample-0.0.1-SNAPSHOT.jar:target/jackson-core-2.4.5.jar:target/jackson-annotations-2.4.5.jar:target/jackson-databind-2.4.5.jar:`hbase classpath` com.mapr.hadoop.HBaseExample "$TABLE_PATH" "$INPUT_FILE"
