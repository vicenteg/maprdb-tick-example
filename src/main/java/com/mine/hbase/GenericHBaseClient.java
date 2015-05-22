package com.mine.hbase;

import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.hbase.async.NoSuchColumnFamilyException;
import org.hbase.async.TableNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import static com.google.common.base.Preconditions.*;
import com.stumbleupon.async.TimeoutException;

import java.util.*;

import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;

/**
 * After this class is constructed and all values are set appropriately the init() method must be
 * called to connect and verify the settings to the HBase server.
 *
 * This class will wrap the HBaseClient to be specific for a table / column family.
 *
 * Batching of requests automatically occur based on the batching time of the client.
 *
 * @author jscott
 */
public abstract class GenericHBaseClient {

	private static Logger log = LoggerFactory.getLogger(GenericHBaseClient.class);
	public static final String DEFAULT_COLUMN_FAMILY = "a";
	public static final int WAIT_UNTIL_COMPLETE = -100;
	private String quorumSpecification = null;
	private String tableName = null;
	private byte[] tableNameBytes = null;
	private String columnFamily = null;
	private byte[] columnFamilyBytes = null;
	private HBaseClient client = null;
	private boolean isInitialized = false;

	// Logs to enable retries of failed puts.
//	private HashMap<Deferred<Object>, KeyValue> submitLog = new HashMap<Deferred<Object>, KeyValue>();
//	private HashMap<Deferred<Object>, KeyValue> retryLog = new HashMap<Deferred<Object>, KeyValue>();
	public static final int MAX_RETRIES_CAP = 5;

	/**
	 * Calling this constructor requires the setters to be called for the quorumSpecification,
	 * tableName, and clientType prior to calling init.
	 */
	public GenericHBaseClient() {
	}

	/**
	 *
	 * @param quorumSpecification
	 * @param columnFamily
	 * @param tableName
	 * @throws IllegalArgumentException if clientType, quorumSpecification, columnFamily and
	 * tableName have not been specified.
	 */
	public GenericHBaseClient(String quorumSpecification, String columnFamily, String tableName) {
		checkArgument(quorumSpecification != null, "quorumSpecification must be specified");
		checkArgument(columnFamily != null, "columnFamily must be specified");
		checkArgument(tableName != null, "tableName must be specified");
		this.quorumSpecification = quorumSpecification;
		this.columnFamily = columnFamily;
		this.columnFamilyBytes = columnFamily.getBytes();
		this.tableName = tableName;
		this.tableNameBytes = tableName.getBytes();
	}

	/**
	 * Ensure that all values (quorumSpecification, tableName, and clientType) to initialize are set
	 * before we connect to HBase and confirm that the required table is available.
	 *
	 * If testing is being performed on this class, do NOT call init if you have overridden the
	 * client to test with.
	 *
	 * Upon successful initialization a log entry will be written to the logs.
	 *
	 * @throws IllegalStateException if clientType, quorumSpecification, columnFamily and tableName
	 * have not already been set.
	 * @throws IllegalStateException if this client has already been initialized
	 * @throws TableNotFoundException If table does not exist on the hbase server
	 * @throws NoSuchColumnFamilyException If the column family for the specified table does not
	 * exist on the hbase server
	 * @throws RuntimeException An unknown error occurred
	 */
	public void init() throws TableNotFoundException, NoSuchColumnFamilyException {
		checkState(!isInitialized, "Client has already been initialized");
		checkState(quorumSpecification != null, "QuorumSpecification must be set before initialization can occur");
		checkState(columnFamily != null, "ColumnFamily must be set before initialization can occur");
		checkState(tableName != null, "TableName must be set before initialization can occur");
		log.debug("Initialization of quorum '{}' for table '{}' and column family '{}' started",
				new Object[]{quorumSpecification, tableName, columnFamily});
		client = HBaseClientManager.getClient(quorumSpecification);

		Object result = null;
		try {
			Deferred<Object> ensureTableFamilyExists = client.ensureTableFamilyExists(tableNameBytes, columnFamilyBytes);
			ensureTableFamilyExists.addErrback(new Callback<Exception, Exception>() {

				/**
				 * Pass on the error to the next error callback (if any);
				 */
				@Override
				public Exception call(Exception arg) {
					log.error("TableException occurred during a database integrity/initializiation request for quorum '{}'", quorumSpecification);
					return arg;
				}
			});
			result = ensureTableFamilyExists.joinUninterruptibly(6000);
		}
		catch (Exception e) {
			log.error("Caught exception when trying to validate hbase connection: {}", e);
			result = e;
		}

		/**
		 * A null result is a successful check, anything else and we need to figure out what the
		 * problem is.
		 */
		if (result == null) {
			log.info("Initialization of quorum '{}' for table '{}' and column family '{}' complete",
					new Object[]{quorumSpecification, tableName, columnFamily});
			isInitialized = true;
		}
		else if (result instanceof TableNotFoundException) {
			throw (TableNotFoundException) result;
		}
		else if (result instanceof NoSuchColumnFamilyException) {
			throw (NoSuchColumnFamilyException) result;
		}
		else if (result instanceof Exception) {
			throw new RuntimeException("Unexpected type of exception occurred during initialization of quorum " + quorumSpecification + " for table " + tableName, (Exception) result);
		}
		else {
			log.info("Successfully connected to quorum: {}", quorumSpecification);
			isInitialized = true;
		}
	}

	public void term() throws HBaseException {
		term(0);
	}

	public void term(int maxWaitTime) throws HBaseException {
		if (isInitialized() && client != null) {
			Object result = null;
			Deferred<Object> shutdown = client.shutdown();
			try {
				if (maxWaitTime > 0) {
					result = shutdown.joinUninterruptibly(maxWaitTime);
				}
				else {
					result = shutdown.joinUninterruptibly();
				}
			}
			catch (Exception e) {
				log.error("Caught exception when trying to shutdown hbase connection: {}", e);
				result = e;
			}
			if (result == null) {
				log.info("Shutdown of quorum '{}' for table '{}' and column family '{}' complete",
						new Object[]{quorumSpecification, tableName, columnFamily});
				isInitialized = false;
			}
			else if (result instanceof Exception) {
				log.error("Unable to shutdown cleanly the HBase connection. result={}, quorum={}, table={}, columnFamily={}", new Object[]{result, quorumSpecification, tableName, columnFamily});
				throw new RuntimeException((Exception)result);
			}
		}
	}

	public boolean isInitialized() {
		return isInitialized;
	}

	/**
	 * @return the quorumSpecification
	 */
	public String getQuorumSpecification() {
		return quorumSpecification;
	}

	/**
	 * @param quorumSpecification the quorumSpecification to set
	 * @exception IllegalStateException If quorumSpecification has already been set
	 */
	public void setQuorumSpecification(String quorumSpecification) {
		if (this.quorumSpecification != null) {
			throw new IllegalStateException("QuorumSpecification cannot be changed after being set");
		}
		this.quorumSpecification = quorumSpecification;
	}

	/**
	 * @return the columnFamily
	 */
	public String getColumnFamily() {
		return columnFamily;
	}

	/**
	 * @return the columnFamilyBytes
	 */
	public byte[] getColumnFamilyBytes() {
		return columnFamilyBytes;
	}

	/**
	 * @param columnFamily the columnFamily to set
	 * @exception IllegalStateException If columnFamily has already been set
	 */
	public void setColumnFamily(String columnFamily) {
		if (this.columnFamily != null) {
			throw new IllegalStateException("ColumnFamily cannot be changed after being set");
		}
		this.columnFamily = columnFamily;
		this.columnFamilyBytes = columnFamily.getBytes();
	}

	/**
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @return the tableNameBytes
	 */
	public byte[] getTableNameBytes() {
		return tableNameBytes;
	}

	/**
	 * @param tableName the tableName to set
	 * @exception IllegalStateException If tableName has already been set
	 */
	public void setTableName(String tableName) {
		if (this.tableName != null) {
			throw new IllegalStateException("TableName cannot be changed after being set");
		}
		this.tableName = tableName;
		this.tableNameBytes = tableName.getBytes();
	}

	/**
	 * This method is meant to allow overriding the client to be used for testing, e.g. set a mock
	 * object for testing the actions of the class.
	 *
	 * KEEP this package private!!!
	 *
	 * @param client
	 */
	void setClient(HBaseClient client) {
		this.client = client;
	}

	/**
	 * Scanners are NOT thread safe
	 *
	 * @return New Scanner
	 */
	protected Scanner getScanner() {
		Scanner scanner = client.newScanner(tableNameBytes);
		scanner.setFamily(columnFamilyBytes);
		return scanner;
	}

	/**
	 *
	 * @param key
	 * @param maxWaitTime When set to less than zero, waits for completion.
	 * @return
	 * <code>ArrayList<KeyValue></code> each key value will represent a column from the row of the
	 * key.
	 * @throws TimeExceededException if max wait time has been exceeded.
	 * @throws HBaseRequestException if an exception was thrown by HBase.
	 */
	protected ArrayList<KeyValue> performGet(byte[] key, long maxWaitTime) throws TimeExceededException, HBaseRequestException {
		Deferred<ArrayList<KeyValue>> result = performGet(key);

		ArrayList<KeyValue> value;
		try {
			if (maxWaitTime > 0) {
				value = result.joinUninterruptibly(maxWaitTime);
			}
			else {
				value = result.joinUninterruptibly();
			}
		}
		catch (InterruptedException e) {
			throw new TimeExceededException("Max wait time was exceeded for get request: " + maxWaitTime + "ms", e);
		}
		catch (TimeoutException e) {
			throw new TimeExceededException("Max wait time was exceeded for put request: " + maxWaitTime + "ms", e);
		}
		catch (Exception e) {
			throw new HBaseRequestException("Result returned from HBase was an exception for get request", e);
		}

		return value;
	}

	/**
	 * Gets a single row from HBase
	 *
	 * @param key
	 * @return A single row in the Deferred object for the specified key
	 */
	protected Deferred<ArrayList<KeyValue>> performGet(byte[] key) {
		GetRequest request = new GetRequest(tableNameBytes, key);
		return client.get(request);
	}

	/**
	 * Gets a single cell from HBase
	 *
	 * @param key
	 * @param qualifier - column name
	 * @return A single cell in the Deferred object for the specified key
	 */
	protected Deferred<ArrayList<KeyValue>> performGet(byte[] key, byte[] qualifier) {
		GetRequest request = new GetRequest(tableNameBytes, key);
		request.family(columnFamilyBytes);
		request.qualifier(qualifier);
		return client.get(request);
	}

	/**
	 * Puts
	 *
	 * @param keys
	 * @param maxWaitTime When set to less than zero, waits for completion
	 * @return
	 * <code>true</code> if there were no problems performing the operation
	 * @throws TimeExceededException if max wait time has been exceeded.
	 * @throws HBaseRequestException if the transaction to put failed. The failure of a single key
	 * value will yield a failure result.
	 */
	protected void performPut(Collection<KeyValue> keyValues, long maxWaitTime) throws TimeExceededException, HBaseRequestException {
		Deferred<ArrayList<Object>> result = performPut(keyValues);
		try {
			if (maxWaitTime > 0) {
				result.joinUninterruptibly(maxWaitTime);
			}
			else {
				result.joinUninterruptibly();
			}
		}
		catch (InterruptedException e) {
			throw new TimeExceededException("Max wait time was exceeded for put request: " + maxWaitTime + "ms", e);
		}
		catch (TimeoutException e) {
			throw new TimeExceededException("Max wait time was exceeded for put request: " + maxWaitTime + "ms", e);
		}
		catch (Exception e) {
			throw new HBaseRequestException("Result returned from HBase was an exception for put request", e);
		}
	}

	/**
	 * Creating a group of deferred objects which allows waiting on a group of put operations
	 *
	 * @param keyValues Given values are written to HBase
	 * @return Effectively a list of deferred voids, could still have errors attached.
	 */
	protected Deferred<ArrayList<Object>> performPut(Collection<KeyValue> keyValues) {
		Collection<Deferred<Object>> results = new ArrayList(keyValues.size());
		for (KeyValue keyValue : keyValues) {
			results.add(performPut(keyValue));
		}
		return Deferred.group(results);
	}

	/**
	 * Puts a single cell into HBase
	 * rick: I had to make this public so it can be used in the submitters outside this package.
	 * (I prefer a composition/delegate model and having every gateway extend this class doesn't seem right.)
	 *
	 * @param keyValue Given value is written to HBase
	 * @return Effectively a list of deferred voids, could still have errors attached.
	 */
	public Deferred<Object> performPut(KeyValue keyValue) {
		PutRequest request = new PutRequest(tableNameBytes, keyValue);
		return client.put(request);
	}

	/**
	 *
	 * @param keys
	 * @param maxWaitTime When set to less than zero, waits for completion
	 * @return
	 * <code>true</code> if there were no problems performing the operation
	 * @throws TimeExceededException if max wait time has been exceeded.
	 * @throws HBaseRequestException if an exception was thrown by HBase.
	 */
	protected boolean performDelete(Collection<byte[]> keys, long maxWaitTime) throws TimeExceededException, HBaseRequestException {
		boolean success = false;

		Deferred<ArrayList<Object>> result = performDelete(keys);
		try {
			if (maxWaitTime > 0) {
				result.joinUninterruptibly(maxWaitTime);
				success = true;
			}
			else {
				result.joinUninterruptibly();
				success = true;
			}
		}
		catch (InterruptedException e) {
			throw new TimeExceededException("Max wait time was exceeded for delete request: " + maxWaitTime + "ms", e);
		}
		catch (TimeoutException e) {
			throw new TimeExceededException("Max wait time was exceeded for put request: " + maxWaitTime + "ms", e);
		}
		catch (Exception e) {
			throw new HBaseRequestException("Result returned from HBase was an exception for delete request", e);
		}

		return success;
	}

	/**
	 * Creating a group of deferred objects allows waiting on a group of delete operations
	 *
	 * @param key
	 * @return Group of Deferred objects that error callback should be attached to
	 */
	protected Deferred<ArrayList<Object>> performDelete(Collection<byte[]> keys) {
		Collection<Deferred<Object>> results = new ArrayList(keys.size());
		for (byte[] key : keys) {
			results.add(performDelete(key));
		}
		return Deferred.group(results);
	}

	/**
	 *
	 * @param key
	 * @return Single Deferred object that error callback should be attached to
	 */
	protected Deferred<Object> performDelete(byte[] key) {
		DeleteRequest request = new DeleteRequest(getTableNameBytes(), key);
		return client.delete(request);
	}

	/**
	 * Deletes only one cell, specified by key and column name (qualifier).
	 *
	 * @param kv
	 * @return Single Deferred object that error callback should be attached to
	 */
	protected Deferred<Object> performDeleteCell(KeyValue kv) {
		DeleteRequest request = new DeleteRequest(getTableNameBytes(), kv.key(), getColumnFamilyBytes(), kv.qualifier());
		return client.delete(request);
	}

	protected KeyValue keyValue(byte[] key, byte[] column, byte[] value) {
		return new KeyValue(key, columnFamilyBytes, column, value);
	}

	protected KeyValue keyValue(byte[] key, byte[] column, byte[] value, long timestamp) {
		return new KeyValue(key, columnFamilyBytes, column, timestamp, value);
	}

	public void checkPutStatus(Deferred<Object> result, long maxWaitTime) throws HBaseRequestException, TimeExceededException {

		try {
			result.joinUninterruptibly(maxWaitTime);
		}
		catch (InterruptedException e) {
			throw new TimeExceededException("Max wait time (" + maxWaitTime + " ms) exceeded.", e);
		}
		catch (TimeoutException e) {
			throw new TimeExceededException("Max wait time (" + maxWaitTime + " ms) exceeded.", e);
		}
		catch (Exception e) {
			throw new HBaseRequestException("Unexpected error in put request.", e);
		}
	}



	public void checkAndRetryPut(Deferred<Object> handle, KeyValue kv, long maxWaitTime, int maxRetries) throws HBaseRequestException, TimeExceededException {

		if (maxRetries > MAX_RETRIES_CAP) {
			log.warn("Too many retries ({})requested. Number of retries capped at " + MAX_RETRIES_CAP, maxRetries);
			maxRetries = MAX_RETRIES_CAP;
		}

		try {
			if (maxWaitTime > 0 ) {
				handle.joinUninterruptibly(maxWaitTime);
			}
			else {
				handle.joinUninterruptibly();
			}
		}
		catch (Exception e) {
			if (maxRetries > 0) {
				log.debug("Put failed. Issuing retry. Key={}", new String(kv.key()));
				Deferred<Object> handle2 = performPut(kv);
				checkAndRetryPut(handle2, kv, maxWaitTime, maxRetries-1);
			}
			else {
				log.warn("Put failed. Key=" + new String(kv.key()), e);
				if (e instanceof TimeoutException || e instanceof InterruptedException) {
					throw new TimeExceededException("Put timed out. max timeout=" + maxWaitTime, e);
				}
				throw new HBaseRequestException("Put failed for unexpected reason.", e);
			}
		}
	}

	public int checkPutStatuses(String id, List<Deferred<Object>> results, long maxWaitTime) {

		int successCount = 0;

		if (results != null) {

			int totalCount = results.size();

			for (Deferred<Object> result: results) {
				try {
					result.joinUninterruptibly(maxWaitTime);
					successCount++;
				}
				catch (Exception e) {
					log.warn("Error while saving user profile. id=" + id, e);
				}
			}

			if (log.isDebugEnabled()) {
				log.debug(successCount + " out of " + totalCount + " puts succeeded.");
			}
		}
		return successCount;
	}

	public int checkAndRetryPuts(String id, Map<Deferred<Object>, KeyValue> submitLog, long maxWaitTime, int maxRetries) throws HBaseRequestException {

		int successCount = 0;
		HashMap<Deferred<Object>, KeyValue> retryLog = new HashMap<Deferred<Object>, KeyValue>();
		HashMap<Deferred<Object>, KeyValue> resubmitLog = new HashMap<Deferred<Object>, KeyValue>();

		if (maxRetries > MAX_RETRIES_CAP) {
			log.warn("Too many retries ({})requested. Number of retries capped at " + MAX_RETRIES_CAP, maxRetries);
			maxRetries = MAX_RETRIES_CAP;
		}

		if (submitLog != null) {

			int totalCount = submitLog.size();

			retryLog.clear();

			for (Deferred<Object> result: submitLog.keySet()) {
				try {
					if (maxWaitTime > 0 ) {
						result.joinUninterruptibly(maxWaitTime);
					}
					else {
						result.joinUninterruptibly();
					}
					successCount++;

				}
				catch (Exception e) {
					KeyValue kv = submitLog.get(result);
					if (kv != null) {
						log.debug("Put check failed. Issuing retry. Key={}", new String(kv.key()));
						retryLog.put(result, kv);
					}
					else {
						log.warn("No key value found in submit log for failed put. Cannot retry.");
					}
				}
			}

			if (maxRetries > 0 && retryLog.size() > 0) {

				// Throw exception if there are too many failures.
				// This is added to make sure any retry is a responsible retry.
				if (submitLog.size() > 10 && retryLog.size() > (submitLog.size()/3)) {
					log.warn("Too many failed puts (" + retryLog.size() + " out of " + submitLog.size() + "). Retry attempt canceled.");
				}
				else {
					log.debug("Retrying {} puts", retryLog.size());
					// Resubmit failed puts
					ArrayList<Deferred<Object>> retryResults = new ArrayList<Deferred<Object>>();
					for (KeyValue kv: retryLog.values()) {
						Deferred<Object> result = performPut(kv);
						resubmitLog.put(result, kv);
					}
					successCount += checkAndRetryPuts(id, resubmitLog, maxWaitTime, maxRetries-1);
				}
			}

			if (log.isDebugEnabled()) {
				log.debug(successCount + " out of " + totalCount + " puts succeeded.");
			}
		}
		return successCount;
	}

	public int checkAndRetryDeleteCells(String id, Map<Deferred<Object>, KeyValue> submitLog, long maxWaitTime, int maxRetries) throws HBaseRequestException {

		int successCount = 0;
		HashMap<Deferred<Object>, KeyValue> retryLog = new HashMap<Deferred<Object>, KeyValue>();
		HashMap<Deferred<Object>, KeyValue> resubmitLog = new HashMap<Deferred<Object>, KeyValue>();

		if (maxRetries > MAX_RETRIES_CAP) {
			log.warn("Too many retries ({})requested. Number of retries capped at " + MAX_RETRIES_CAP, maxRetries);
			maxRetries = MAX_RETRIES_CAP;
		}

		if (submitLog != null) {

			int totalCount = submitLog.size();

			retryLog.clear();

			for (Deferred<Object> result: submitLog.keySet()) {
				try {
					if (maxWaitTime > 0 ) {
						result.joinUninterruptibly(maxWaitTime);
					}
					else {
						result.joinUninterruptibly();
					}
					successCount++;

				}
				catch (Exception e) {
					KeyValue kv = submitLog.get(result);
					if (kv != null) {
						log.debug("DeleteCell check failed. Issuing retry. Key={}", new String(kv.key()));
						retryLog.put(result, kv);
					}
					else {
						log.warn("No key value found in submit log for failed deleteCell. Cannot retry.");
					}
				}
			}

			if (maxRetries > 0 && retryLog.size() > 0) {

				// Throw exception if there are too many failures.
				// This is added to make sure any retry is a responsible retry.
				if (submitLog.size() > 10 && retryLog.size() > (submitLog.size()/3)) {
					log.warn("Too many failed deletes (" + retryLog.size() + " out of " + submitLog.size() + "). Retry attempt canceled.");
				}
				else {
					log.debug("Retrying {} deletes", retryLog.size());
					// Resubmit failed puts
					for (KeyValue kv: retryLog.values()) {
						Deferred<Object> result = performDeleteCell(kv);
						resubmitLog.put(result, kv);
					}
					successCount += checkAndRetryDeleteCells(id, resubmitLog, maxWaitTime, maxRetries-1);
				}
			}

			if (log.isDebugEnabled()) {
				log.debug(successCount + " out of " + totalCount + " deletes succeeded.");
			}
		}
		return successCount;
	}
}
