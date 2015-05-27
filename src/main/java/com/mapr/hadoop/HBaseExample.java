package com.mapr.hadoop;

import com.google.common.base.Splitter;
import org.joda.time.*;

import java.util.Map;
import java.io.*;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
// Input file is in CSV format:
// Symbol,Date,Open,High,Low,Close,Volume
// AAIT,18-May-2015 11:29,36.58,36.58,36.58,36.58,375

public class HBaseExample {	
	private static String generateKeyString(String symbol, DateTime dateTime) {
        return String.format("%s_%tY-%<tm-%<td-%<tH", symbol ,dateTime.toDate());
		// return symbol + "_" + dateTime.getYear() + dateTime.getMonthOfYear() + dateTime.getDayOfMonth() + dateTime.getHourOfDay();
	}

    /*
    public static void persistMapAsync(Map<String, DataReader.TransactionList> mp, String tableName, String cfName) throws java.io.IOException {
        TickDataClient tdc = new TickDataClient("", cfName, tableName);
        tdc.init();
        byte[] cfNameBytes = Bytes.toBytes(cfName);
        byte[] columnNameBytes = Bytes.toBytes("data");
        double pt0 = System.nanoTime() * 1e-9;
        for (String s : mp.keySet()) {
            KeyValue kv = new KeyValue(Bytes.toBytes(s), cfNameBytes, columnNameBytes, Bytes.toBytes(mp.get(s).asJsonMaps()));
            tdc.performPut(kv);
        }
        double pt1 = System.nanoTime() * 1e-9;
        System.out.printf("Wrote %d equities in %.3f seconds\n", mp.size(), pt1 - pt0);
        tdc.term();
    }
*/
	public static void main(String[] args) throws IOException {
        String cfName = args[0];
        String tableName = args[1];
        String inputFilePath = args[2];
        int nThreads = 5;

        ExecutorService es = Executors.newFixedThreadPool(nThreads);
        TickDataClient tdc = new TickDataClient("", cfName, tableName);
        tdc.init();

        DataReader rd = new DataReader();
        double t0 = System.nanoTime() * 1e-9;
        Map<String, DataReader.TransactionList> m = rd.read(Files.newReaderSupplier(Paths.get(inputFilePath).toFile(), Charsets.UTF_8));
        double t1 = System.nanoTime() * 1e-9;
        System.out.printf("Read %d equities in %.3f seconds\n", m.size(), t1 - t0);

        TickWriter tw = new TickWriter(tdc, m, tableName, cfName);
        es.execute(tw);
        //persistMapAsync(m, tableName, "cf1");
	}
}
