package com.mapr.hadoop;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

public class HBaseExampleTest {
    @org.junit.Test
    public void testFormat() {
        String symbol = "AADT";
        DateTime dateTime = new DateTime();
        System.out.println(String.format("%s_%tY-%<tm-%<td-%<tH", symbol, dateTime.toDate()));
    }

    @Test
    public void testReadSpeed() throws IOException, InterruptedException {
        DataReader rd = new DataReader();
        rd.useCache(false);
        for (int i = 0; i < 4; i++) {
            double t0 = System.nanoTime() * 1e-9;
            Map<String, DataReader.TransactionList> m = rd.read(Resources.newReaderSupplier(Resources.getResource("data.1M.csv"), Charsets.UTF_8));
            double t1 = System.nanoTime() * 1e-9;
            System.out.printf("%d equities in %.3f seconds\n", m.size(), t1 - t0);
        }
        rd.useCache(true);
        for (int i = 0; i < 4; i++) {
            double t0 = System.nanoTime() * 1e-9;
            Map<String, DataReader.TransactionList> m = rd.read(Resources.newReaderSupplier(Resources.getResource("data.1M.csv"), Charsets.UTF_8));
            double t1 = System.nanoTime() * 1e-9;
            System.out.printf("%d equities in %.3f seconds\n", m.size(), t1 - t0);
        }
        rd.useCache(false);
        for (int i = 0; i < 4; i++) {
            double t0 = System.nanoTime() * 1e-9;
            Map<String, DataReader.TransactionList> m = rd.read(Resources.newReaderSupplier(Resources.getResource("data.1M.csv"), Charsets.UTF_8));
            double t1 = System.nanoTime() * 1e-9;
            System.out.printf("%d equities in %.3f seconds\n", m.size(), t1 - t0);
        }
    }

    @Test
    public void testJsonMapsAreValid() throws IOException {
        DataReader rd = new DataReader();
        ObjectMapper mapper = new ObjectMapper();
        for (int i = 0; i < 4; i++) {
            double t0 = System.nanoTime() * 1e-9;
            Map<String, DataReader.TransactionList> m = rd.read(Resources.newReaderSupplier(Resources.getResource("data.1M.csv"), Charsets.UTF_8));
            double t1 = System.nanoTime() * 1e-9;
            try (BufferedWriter out = new BufferedWriter(new FileWriter("test.out"))) {
                PrintWriter pw = new PrintWriter(out);
                for (String s : m.keySet()) {
                    mapper.writeValueAsString(m.get(s).asJsonMaps(pw));
                }
            }
            double t2 = System.nanoTime() * 1e-9;
            System.out.printf("%.3fs %.3fs\n", t1 - t0, t2 - t1);
        }
    }

    @Test
    public void testJsonArraysAreValid() throws IOException {
        DataReader rd = new DataReader();
        ObjectMapper mapper = new ObjectMapper();
        for (int i = 0; i < 4; i++) {
            double t0 = System.nanoTime() * 1e-9;
            Map<String, DataReader.TransactionList> m = rd.read(Resources.newReaderSupplier(Resources.getResource("data.1M.csv"), Charsets.UTF_8));
            double t1 = System.nanoTime() * 1e-9;
            try (BufferedWriter out = new BufferedWriter(new FileWriter("test.out"))) {
                PrintWriter pw = new PrintWriter(out);
                for (String s : m.keySet()) {
                    mapper.writeValueAsString(m.get(s).asJsonArrays(pw));
                }
            }
            double t2 = System.nanoTime() * 1e-9;
            System.out.printf("%.3fs %.3fs\n", t1 - t0, t2 - t1);
        }
    }
    @Test
    public void testJson() throws IOException {
        DataReader rd = new DataReader();
        for (int i = 0; i < 4; i++) {
            double t0 = System.nanoTime() * 1e-9;
            Map<String, DataReader.TransactionList> m = rd.read(Resources.newReaderSupplier(Resources.getResource("data.1M.csv"), Charsets.UTF_8));
            double t1 = System.nanoTime() * 1e-9;
            try (BufferedWriter out = new BufferedWriter(new FileWriter("test.out"))) {
                PrintWriter pw = new PrintWriter(out);
                for (String s : m.keySet()) {
                    m.get(s).asJsonMaps(pw);
                }
            }
            double t2 = System.nanoTime() * 1e-9;
            System.out.printf("%.3fs %.3fs\n", t1 - t0, t2 - t1);
        }
    }
}