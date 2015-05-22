package com.mine.hbase;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.hbase.async.HBaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is not intended to be extended. It is NOT package private because it has globally
 * accessible accessor methods for modifying the internally managed client instances.
 *
 * @author jscott
 */
public final class HBaseClientManager {

	private static Logger log = LoggerFactory.getLogger(HBaseClientManager.class);
	private static final Map<String, HBaseClient> HBASE_CLIENTS = new ConcurrentHashMap<String, HBaseClient>();
	private static final Map<String, Short> MAX_BATCH_TIMES = new ConcurrentHashMap<String, Short>();
	public static final short DEFAULT_MAX_BATCH_TIME = 1000;

	/**
	 * When the VM shuts down we want this to always run. This will prevent data loss by allowing
	 * the HBaseClient to shutdown cleanly.
	 */
	static {
		Runtime.getRuntime().addShutdownHook(new Thread("HBaseClientManagerShutdown") {

			@Override
			public void run() {
				log.info("Shutdown of {} has started...", getClass().getName());
				for (Map.Entry<String, HBaseClient> entry : HBASE_CLIENTS.entrySet()) {
					Deferred<Object> shutdownRequest = entry.getValue().shutdown();
					final String quorum = entry.getKey();
					shutdownRequest.addErrback(
							new Callback<Exception, Exception>() {

								/**
								 * Pass on the error to the next error callback (if any);
								 */
								@Override
								public Exception call(Exception arg) {
									log.error("Exception occurred during a shutdown request for quorum '{}'", quorum);
									return arg;
								}
							});
				}
				log.info("Shutdown of {} has completed...", getClass().getName());
			}
		});
	}

	private HBaseClientManager() {
	}

	/**
	 * 
	 * @param quorumSpecification comma separated list of servers in quorum
	 * @return HBaseClient
	 */
	public static HBaseClient getClient(String quorumSpecification) {
		String clientKey = clientKey(quorumSpecification);
		HBaseClient client = HBASE_CLIENTS.get(clientKey);

		if (client == null) {
			log.info("Creating HBaseClient for performing {}s, against quorum {}", quorumSpecification);
			client = new HBaseClient(quorumSpecification);
			Short waitTime = MAX_BATCH_TIMES.get(clientKey);
			if (waitTime != null) {
				client.setFlushInterval(waitTime);
			}
			HBASE_CLIENTS.put(clientKey, client);
		}

		return client;
	}

	private static String clientKey(String quorumSpecification) {
		return quorumSpecification;
	}

	/**
	 * @param quorumSpecification comma separated list of servers in quorum
	 * @return the maxClientBatchTime
	 */
	public static Short getMaxClientBatchTime(String quorumSpecification) {
		String clientKey = clientKey(quorumSpecification);
		HBaseClient client = HBASE_CLIENTS.get(clientKey);
		Short value = null;
		if (client != null) {
			value = client.getFlushInterval();
		}
		return value;
	}

	/**
	 * If the client is active this change will happen immediately (within two batching cycles).
	 *
	 * @param Enum (read, write, etc..)
	 * @param quorumSpecification comma separated list of servers in quorum
	 * @param maxClientBatchTime If set to zero, batching is disabled and all requests are sent
	 * immediately. This value is in millis
	 */
	public void setMaxClientBatchTime(String quorumSpecification, short maxClientBatchTime) {
		String clientKey = clientKey(quorumSpecification);
		MAX_BATCH_TIMES.put(clientKey, maxClientBatchTime);
		HBaseClient client = HBASE_CLIENTS.get(clientKey);
		if (client != null) {
			client.setFlushInterval(maxClientBatchTime);
		}
	}
}
