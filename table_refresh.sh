#!/bin/sh -ex

TABLE_PATH="/user/vgonzalez/ticks"

mvn package

maprcli table delete -path "$TABLE_PATH"
maprcli table create -path "$TABLE_PATH"
maprcli table cf create -path "$TABLE_PATH" -cfname opens
maprcli table cf create -path "$TABLE_PATH" -cfname highs
maprcli table cf create -path "$TABLE_PATH" -cfname lows
maprcli table cf create -path "$TABLE_PATH" -cfname closes
maprcli table cf create -path "$TABLE_PATH" -cfname volumes

time java -cp target/HBaseExample-0.0.1-SNAPSHOT.jar:`hbase classpath` com.mapr.hadoop.HBaseExample "$TABLE_PATH" ~/NASDAQ_20150518.csv