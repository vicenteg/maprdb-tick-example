package com.mapr.hadoop;

import org.joda.time.*;

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

		reader = new BufferedReader(new FileReader(args[1]));
		String delimiter = "[,]";

		// throw away the header line.
		String headerLine = reader.readLine();
		// throw away the header line.
		
		HashMap<String, Put> accumulator = new HashMap<String, Put>();
		HashMap<String, DateTime> currentDateTime = new HashMap<String, DateTime>();
		
		int totalRows = 0;
		SimpleDateFormat format = new SimpleDateFormat("dd-MMM-yyyy HH:mm");
		format.setTimeZone(TimeZone.getTimeZone("America/New_York"));
		DateTime jt = null;
		
		long start = System.currentTimeMillis();
		while (true) {
			String line = reader.readLine();
			if (line == null) {
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
			Integer volume = new Integer(fields[6]);
			
			String keyString = generateKeyString(symbol, jt);
			byte[] keyBytes = Bytes.toBytes(keyString);
			
			if (currentDateTime.get(symbol) != null) {
				DateTime lastDateTime = currentDateTime.get(symbol);
				int lastHourSeen = lastDateTime.getHourOfDay();
				
				if (jt.getHourOfDay() > lastHourSeen) {
					Put next = accumulator.get(generateKeyString(symbol, lastDateTime));
					table.put(next);
				}
			}
			
			currentDateTime.put(symbol, jt);
			
			if (accumulator.containsKey(keyString)) {
				Put p = accumulator.get(keyString);
				byte[] opensCF = Bytes.toBytes("opens");
				byte[] closesCF = Bytes.toBytes("closes");
				byte[] volumesCF = Bytes.toBytes("volumes");
				byte[] highsCF = Bytes.toBytes("highs");
				byte[] lowsCF = Bytes.toBytes("lows");

				byte[] column = Bytes.toBytes(jt.getMinuteOfHour());
				
				p.add(opensCF, column, Bytes.toBytes(open));
				p.add(closesCF, column, Bytes.toBytes(close));
				p.add(highsCF, column, Bytes.toBytes(high));
				p.add(lowsCF, column, Bytes.toBytes(low));
				p.add(volumesCF, column, Bytes.toBytes(volume));
			}
			else {
				Put p = new Put(keyBytes);
				accumulator.put(keyString, p);
			}
		}
		long end = System.currentTimeMillis();
		System.err.println("Ingested " + totalRows + " rows in " +  (end - start)/1000.0 + " seconds.");

		Get g = new Get(Bytes.toBytes("AAPL_201551814"));
		Result r = table.get(g);
		byte[] value = r.getValue(Bytes.toBytes("opens"), Bytes.toBytes(50));
		Double opens = Bytes.toDouble(value);
		System.err.println("GET: " + opens);
		table.close();
	}
}
