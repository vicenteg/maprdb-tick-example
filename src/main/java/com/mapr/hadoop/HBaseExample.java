package com.mapr.hadoop;

import org.joda.time.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TimeZone;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapr.hadoop.Tick;

// Input file is in CSV format:
// Symbol,Date,Open,High,Low,Close,Volume
// AAIT,18-May-2015 11:29,36.58,36.58,36.58,36.58,375

public class HBaseExample {	
	private static BufferedReader reader;

	private static String generateKeyString(String symbol, DateTime dateTime) {
		return symbol + "_" + dateTime.getYear() + dateTime.getMonthOfYear() + dateTime.getDayOfMonth() + dateTime.getHourOfDay();
	}
	
	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		HTable table = new HTable(config, args[0]);
		ObjectMapper mapper = new ObjectMapper();

		reader = new BufferedReader(new FileReader(args[1]));
		String delimiter = "[,]";

		// throw away the header line.
		String headerLine = reader.readLine();
		// throw away the header line.
		
		HashMap<String, DateTime> currentDateTime = new HashMap<String, DateTime>();
		HashMap<String, ArrayList<Tick>> ticks = new HashMap<String, ArrayList<Tick>>();
		
		int totalRows = 0;
		SimpleDateFormat format = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss.SSS");
		format.setTimeZone(TimeZone.getTimeZone("America/New_York"));
		DateTime jt = null;
		
		long start = System.currentTimeMillis();

		while (true) {
			String line = reader.readLine();
			if (line == null) {
				reader.close();
				break;
			}
			totalRows+=1;

			String[] fields = line.split(delimiter);


			String symbol = fields[0];
			
			try {
				jt = new DateTime(format.parse(fields[1]));
			} catch (ParseException e) {
				e.printStackTrace();
			}
			Double open = new Double(fields[2]);
			Double high = new Double(fields[3]);
			Double low = new Double(fields[4]);
			Double close = new Double(fields[5]);
			Double volume = new Double(fields[6]);
			
			Tick t = new Tick();
			t.setTime(jt);
			t.setTick("open", open);
			t.setTick("close", close);
			t.setTick("volume", volume);
			t.setTick("high", high);
			t.setTick("low", low);
			
			
			String keyString = generateKeyString(symbol, jt);

			if (ticks.get(keyString) != null) {
				ArrayList<Tick> a = ticks.get(keyString);
				a.add(t);
			}
			else {
				ticks.put(keyString, new ArrayList<Tick>());
			}
			
			if (currentDateTime.get(symbol) != null) {
				DateTime lastDateTime = currentDateTime.get(symbol);
				int lastHourSeen = lastDateTime.getHourOfDay();
				
				if (jt.getHourOfDay() > lastHourSeen) {
					String lastTickKeyString = generateKeyString(symbol, lastDateTime);
					byte[] keyBytes = Bytes.toBytes(lastTickKeyString);
					byte[] cfName = Bytes.toBytes("cf1");
					byte[] column = Bytes.toBytes("data");
					
					ArrayList<Tick> lastTicks = ticks.get(lastTickKeyString);
					Put p = new Put(keyBytes);
					p.add(cfName, column, Bytes.toBytes(mapper.writeValueAsString(lastTicks)));
					table.put(p);
					ticks.remove(lastTickKeyString);
				}
			}
			
			currentDateTime.put(symbol, jt);
		}
		long end = System.currentTimeMillis();
		System.err.println("Ingested " + totalRows + " rows in " +  (end - start)/1000.0 + " seconds.");

		// Let's get AAPL's "open" at 14:50
		/*
		Get g = new Get(Bytes.toBytes("AAPL_201551814"));
		Result r = table.get(g);
		byte[] value = r.getValue(Bytes.toBytes("opens"), Bytes.toBytes(50));
		Double opens = Bytes.toDouble(value);
		System.err.println("GET: " + opens);
		*/
		table.close();
		
	}
}
