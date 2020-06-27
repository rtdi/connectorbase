package io.rtdi.bigdata.connector.connectorframework;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema.Field;

import io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.controller.ThreadBasedController;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorTemporaryException;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RowType;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

public abstract class ProducerQueuing<S extends ConnectionProperties, P extends ProducerProperties> extends Producer<S,P> {
	protected ArrayBlockingQueue<Data> pollqueue = new ArrayBlockingQueue<>(10000);
	protected Data commit = new Data();
	protected Data rollback = new Data();
	private ThreadBasedController<?> executor;
	private Map<String, Object> transactions = new HashMap<>();

	public ProducerQueuing(ProducerInstanceController instance) throws PropertiesException {
		super(instance);
		if (instance.getChildController("QueueProducer") == null) {
			ThreadBasedController<?> task = getNewQueueProducer();
			instance.addChild("QueueProducer", task);
			this.executor = task;
		} else {
			executor = (ThreadBasedController<?>) instance.getChildController("QueueProducer");
		}
	}

	/**
	 * This implementation is using a BlockingQueue.<br>
	 * One thread does produce data constantly and is blocked when adding records to the queue. The poll method reads
	 * the queue for up to one second or when enough records have been received.
	 * 
	 * @param aftersleep indicates if the previous poll call returned zero records and hence the sleep for poll interval did happen 
	 * @return Number of records produced in this cycle
	 * @throws IOException if error
	 */
	@Override
	public final int poll(boolean aftersleep) throws IOException {
		int rows = 0;
		Data data;
		try {
			while (executor != null && executor.isRunning() && (data = pollqueue.poll(1, TimeUnit.SECONDS)) != null && rows < 10000) {
				if (producersession.getTransactionID() == null) {
					logger.info("poll starts transaction");
					producersession.beginTransaction(data.sourcetransactionid);
				}
				if (data == commit) {
					String t = producersession.getTransactionID();
					producersession.commitTransaction();
					logger.info("poll received commit");
					commit(t, transactions.get(t));
					transactions.remove(t);
				} else if (data == rollback) {
					String t = producersession.getTransactionID();
					producersession.abortTransaction();
					logger.info("poll received rollback");
					transactions.remove(t);
				} else {
					logger.debug("poll received record {}", data.valuerecord.toString());
					
					JexlRecord targetvaluerecord;
					if (data.schemahandler.getMapping() != null) {
						targetvaluerecord = data.schemahandler.getMapping().apply(data.valuerecord);
					} else {
						targetvaluerecord = data.valuerecord;
					}
					JexlRecord keyrecord = new JexlRecord(data.schemahandler.getKeySchema());
					for (Field f : keyrecord.getSchema().getFields()) {
						keyrecord.put(f.name(), targetvaluerecord.get(f.name()));
					}

					producersession.addRow(data.topichandler, data.partition, data.schemahandler,
							keyrecord, targetvaluerecord, data.changetype, data.sourcerowid, data.sourcesystemid);
					rows++;
				}
			}
			if (executor == null || !executor.isRunning()) {
				throw new ConnectorTemporaryException("Queue Producer terminated, nobody producing rows for the internal queue any longer", null,
						"For whatever reason the Executor is no longer active", instance.getName());
			}
		} catch (InterruptedException e) {
			logger.info("Polling the source got interrupted");
			executor.interrupt();
		}
		return rows;
	}

	public void queueBeginTransaction(String transactionid, Object payload) {
		transactions.put(transactionid, payload);
	}
	
	public void queueCommitRecord() {
		try {
			pollqueue.put(commit);
		} catch (InterruptedException e) {
			logger.debug("Adding a record to the queue got interrupted");
			executor.interrupt();
		}
	}
	
	public void queueRollbackRecord() {
		try {
			pollqueue.put(rollback);
		} catch (InterruptedException e) {
			logger.debug("Adding a record to the queue got interrupted");
			executor.interrupt();
		}
	}
	
	public void waitTransactionsCompleted() {
		while (executor != null && executor.isRunning() && transactions.size() != 0) {
			logger.debug("Waiting for {} transactions to complete", transactions.size());
			executor.sleep(500);
		}
	}

	public void queueRecord(TopicHandler topichandler, Integer partition, SchemaHandler schemahandler,
			JexlRecord valuerecord, RowType changetype, 
			String sourcerowid, String sourcesystemid, String sourcetransactionid) {
		Data data = new Data(topichandler, partition, schemahandler, valuerecord, 
				changetype, sourcerowid, sourcesystemid, sourcetransactionid);
		try {
			pollqueue.put(data);
		} catch (InterruptedException e) {
			logger.info("Adding a record to the queue got interrupted");
			executor.interrupt();
		}
	}
	
	/**
	 * In order to simplify the implementation, producers can use this method to produce data for the poll method.
	 * The idea is that any potentially long running procedure passes a Runnable into this method at the start and the runnable
	 * is in an endless loop reading data by constantly scanning the source. If the producer is shutdown, this thread will get an interrupt signal
	 * and should terminate asap.
	 * 
	 * @throws IOException if error
	 */
	@Override
	public final void startProducerCapture() throws IOException {
		executor.startController();
	}
	
	@Override
	public void close() {
		super.close();
	}

	
	public abstract ThreadBasedController<?> getNewQueueProducer();
	
	private static class Data {
		private TopicHandler topichandler;
		private Integer partition;
		private SchemaHandler schemahandler;
		private JexlRecord valuerecord;
		private RowType changetype;
		private String sourcerowid;
		private String sourcesystemid;
		private String sourcetransactionid;

		private Data() {	
		}
		
		private Data(TopicHandler topichandler, Integer partition, SchemaHandler schemahandler,
				JexlRecord valuerecord, RowType changetype, 
				String sourcerowid, String sourcesystemid, String sourcetransactionid) {
			this.topichandler = topichandler;
			this.partition = partition;
			this.schemahandler = schemahandler;
			this.valuerecord = valuerecord;
			this.changetype = changetype;
			this.sourcerowid = sourcerowid;
			this.sourcesystemid = sourcesystemid;
			this.sourcetransactionid = sourcetransactionid;
		}

	}

}
