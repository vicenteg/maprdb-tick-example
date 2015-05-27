package com.mapr.hadoop;

import com.google.common.collect.Lists;
import org.joda.time.*;

import java.util.List;
import java.util.Map;
import java.io.*;
import java.nio.file.Paths;
import java.util.Set;
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
	}

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

        Set<String> keys = m.keySet();
        final List<TickWriter> tasks = Lists.newArrayList();

        for (String k: keys) {
            TickWriter t = new TickWriter(tdc, m, tableName, cfName, k);
            System.out.println("Submitted " + k);
            es.submit(t);
        }

        es.shutdown();
        tdc.term();
	}
}
