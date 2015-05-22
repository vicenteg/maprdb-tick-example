package com.mapr.hadoop;

import com.mine.hbase.GenericHBaseClient;

/**
 * Created by vince on 5/21/15.
 */
public class TickDataClient extends GenericHBaseClient {
    public TickDataClient(String quorumSpecification, String columnFamily, String tableName) {
        super(quorumSpecification,  columnFamily,  tableName);
    }
}
