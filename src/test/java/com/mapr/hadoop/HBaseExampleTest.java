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

public class HBaseExampleTest {
    @org.junit.Test
    public void testFormat() {
        String symbol = "AADT";
        DateTime dateTime = new DateTime();
        System.out.println(String.format("%s_%tY-%<tm-%<td-%<tH", symbol, dateTime.toDate()));
    }

    @Test
    public void testReadSpeed() throws IOException, InterruptedException {
        System.out.printf("starting\n");
        Thread.sleep(10000);
        System.out.printf("running\n");
        DataReader rd = new DataReader();
        for (int i = 0; i < 20; i++) {
            double t0 = System.nanoTime() * 1e-9;
            Map<String, DataReader.TransactionList> m = rd.read(Resources.newReaderSupplier(Resources.getResource("data.1M.csv"), Charsets.UTF_8));
            double t1 = System.nanoTime() * 1e-9;
            System.out.printf("%d equities in %.3f seconds\n", m.size(), t1 - t0);
        }
    }

    @Test
    public void testJson() throws IOException {
        DataReader rd = new DataReader();
        for (int i = 0; i < 20; i++) {
            double t0 = System.nanoTime() * 1e-9;
            Map<String, DataReader.TransactionList> m = rd.read(Resources.newReaderSupplier(Resources.getResource("data.1M.csv"), Charsets.UTF_8));
            double t1 = System.nanoTime() * 1e-9;
            try (BufferedWriter out = new BufferedWriter(new FileWriter("test.out"))) {
                PrintWriter pw = new PrintWriter(out);
                for (String s : m.keySet()) {
                    m.get(s).asJson(pw);
                }
            }
            double t2 = System.nanoTime() * 1e-9;
            System.out.printf("%.3fs %.3fs\n", t1 - t0, t2 - t1);
        }
    }
}