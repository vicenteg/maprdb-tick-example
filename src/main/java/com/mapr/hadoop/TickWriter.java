package com.mapr.hadoop;

import org.apache.hadoop.hbase.util.Bytes;
import org.hbase.async.KeyValue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by vince on 5/26/15.
 */
public class TickWriter implements Runnable {
    TickDataClient client;
    Map<String,DataReader.TransactionList> mp;
    String tableName;
    String cfName;
    Set<String> keySet;

    public TickWriter(TickDataClient c, Map<String, DataReader.TransactionList> m, String t, String cf, String key) {
        client = c;
        mp = m;
        cfName = cf;
        keySet = new HashSet<String>();

        keySet.add(key);
    }

    public TickWriter(TickDataClient c, Map<String, DataReader.TransactionList> m, String t, String cf) {
        client = c;
        mp = m;
        cfName = cf;
        tableName = t;
        keySet = m.keySet();
    }

    public void persistMapAsync() throws java.io.IOException {
        byte[] cfNameBytes = Bytes.toBytes(this.cfName);
        byte[] columnNameBytes = Bytes.toBytes("data");

        double pt0 = System.nanoTime() * 1e-9;
        for (String s : keySet) {
            KeyValue kv = new KeyValue(Bytes.toBytes(s), cfNameBytes, columnNameBytes, Bytes.toBytes(mp.get(s).asJsonMaps()));
            this.client.performPut(kv);
        }
        double pt1 = System.nanoTime() * 1e-9;
        System.out.printf("Wrote %d equities in %.3f seconds\n", mp.size(), pt1 - pt0);
    }

    @Override
    public void run() {
        try {
            this.persistMapAsync();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
