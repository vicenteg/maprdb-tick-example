package com.mapr.hadoop;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import com.google.common.io.InputSupplier;
import com.google.common.io.LineProcessor;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Formatter;
import java.util.Iterator;
import java.util.Map;

/**
 * Reads CSV data and parses time and price info.  The prices for different equities are kept in a
 * special data structure that keeps data in arrays instead of in generic data structures.
 */
public class DataReader {
    public Map<String, TransactionList> read(InputSupplier<InputStreamReader> input) throws IOException {
        return CharStreams.readLines(
                input,
                new LineProcessor<Map<String, TransactionList>>() {
                    private String price;
                    private String date;
                    private String symbol;
                    DateTimeFormatter fmt = DateTimeFormat.forPattern("dd-MMM-yyyy HH:mm:ss.SSS");

                    Map<String, TransactionList> data = Maps.newHashMap();
                    Splitter onComma = Splitter.on(",").trimResults();
                    boolean header = true;

                    @Override
                    public boolean processLine(String s) throws IOException {
                        if (header) {
                            // ignore header
                            header = false;
                        } else {
                            Iterator<String> pieces = onComma.split(s).iterator();

                            symbol = pieces.next();
                            date = pieces.next();
                            price = pieces.next();

                            long timeStamp = fmt.parseMillis(date);
                            double open = Double.parseDouble(price);

                            TransactionList trans = data.get(symbol);
                            if (trans == null) {
                                trans = new TransactionList();
                                data.put(symbol, trans);
                            }
                            trans.add(timeStamp, open);
                        }
                        return true;
                    }

                    @Override
                    public Map<String, TransactionList> getResult() {
                        return data;
                    }
                });
    }


    /**
     * Keeps a bunch of time-stamped prices in arrays.  Presumably these are for a single equity.
     */
    public static class TransactionList {
        long[] times = new long[10];
        double[] prices = new double[10];
        int insert = 0;

        public void add(long timeStamp, double open) {
            // need more data?
            if (insert >= times.length) {
                // reallocate a bigger array and copy our data to the new array
                int newSize = times.length * 2;
                long[] newTimes = new long[newSize];
                System.arraycopy(times, 0, newTimes, 0, insert);
                times = newTimes;

                double[] newPrices = new double[newSize];
                System.arraycopy(prices, 0, newPrices, 0, insert);
                prices = newPrices;
            }
            // add to the current array
            times[insert] = timeStamp;
            prices[insert] = open;
            insert++;
        }

        public int size() {
            return insert;
        }

        public String asJsonMaps() {
            Formatter out = new Formatter();
            out.format("[");

            for (int i = 0; i < insert; i++) {
                if (i > 0) {
                    out.format(",");
                }
                out.format("{ \"time\": %d, \"open\": %.3f }", times[i], prices[i]);
            }
            out.format("]\n");
            return out.toString();
        }

        public String asJsonMaps(PrintWriter out) {
            out.format("[");

            for (int i = 0; i < insert; i++) {
                if (i > 0) {
                    out.format(",");
                }
                out.format("{ \"time\": %d, \"open\": %.3f }", times[i], prices[i]);
            }
            out.format("]\n");
            return out.toString();
        }
        /**
         * Formats our data as a single JSON object with arrays of values in it.
         */
        public String asJsonArrays() {
            Formatter out = new Formatter();
            out.format("{\"times\":[");
            String separator = "";
            for (int i = 0; i < insert; i++) {
                out.format("%s%d", separator, times[i]);
                separator = ",";
            }
            out.format("],\"open\":[");
            separator = "";
            for (int i = 0; i < insert; i++) {
                out.format("%s%.3f", separator, prices[i]);
                separator = ",";
            }
            out.format("]}\n");
            return out.toString();
        }

        /**
         * Formats our data to a stream without building a string.
         * @param out The stream to write to.
         */
        public String asJsonArrays(PrintWriter out) {
            out.format("{\"times\":[");
            String separator = "";
            for (int i = 0; i < insert; i++) {
                out.format("%s%d", separator, times[i]);
                separator = ",";
            }
            out.format("],\"open\":[");
            separator = "";
            for (int i = 0; i < insert; i++) {
                out.format("%s%.3f", separator, prices[i]);
                separator = ",";
            }
            out.format("]}\n");
            return out.toString();
        }
    }

}
