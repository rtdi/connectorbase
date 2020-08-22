package io.rtdi.bigdata.pipelinehttp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;

import io.rtdi.bigdata.connector.pipeline.foundation.AvroDeserialize;
import io.rtdi.bigdata.connector.pipeline.foundation.ConsumerSession;
import io.rtdi.bigdata.connector.pipeline.foundation.IProcessFetchedRow;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.OperationState;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineTemporaryException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.HttpUtil;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.IOUtils;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;

public class ConsumerSessionHttp extends ConsumerSession<TopicHandlerHttp> {
	private PipelineHttp api;
	private URL url;
	private IOUtils io = new IOUtils();
	private HttpUtil http;

	protected ConsumerSessionHttp(ConsumerProperties properties, PipelineHttp api) throws PropertiesException {
		super(properties, api);
		this.api = api;
		try {
			url = api.getTransactionEndpointForFetch().toURL();
			String username = api.getAPIProperties().getUser();
			String password = api.getAPIProperties().getPassword();
			http = new HttpUtil(username, password);
		} catch (MalformedURLException e) {
			throw new PipelineCallerException("The generated URL for fetching the data via http is not a proper url", e, null, api.getTransactionEndpointForFetch().toString());
		}
	}

	@Override
	public int fetchBatch(IProcessFetchedRow processor) throws IOException {
		http.getHttpConnection(url, "GET");
		int rowsfetched = 0;
		try (InputStream in = http.getConnection().getInputStream();) {
			state = OperationState.FETCHWAITINGFORDATA;
			while (io.readNextIntValue(in) && isRunning()) {
				switch (io.getNextIntValue()) {
				case 1:
					state = OperationState.FETCHGETTINGROW;
					String s = io.readString(in);
					lasttopic = TopicName.create(s); // lasttopic is a monitoring variable, needs to be set anyhow, hence using it
					lastoffset = io.readLong(in);
					int partition = io.readInt(in);
					TopicHandlerHttp topic = getTopic(lasttopic); 
					if (topic == null) {
						topic = api.getTopic(lasttopic);
						addTopic(topic);
					}
					byte[] key = io.readBytes(in);
					byte[] value = io.readBytes(in);
					JexlRecord keyrecord = null;
					try {
						keyrecord = AvroDeserialize.deserialize(key, this, null);
					} catch (IOException e) {
						logger.error("Cannot deserialize data Key with offset {}, row not processed", String.valueOf(lastoffset));
					}
					JexlRecord valuerecord = null;
					try {
						valuerecord = AvroDeserialize.deserialize(value, this, null);
					} catch (IOException e) {
						logger.error("Cannot deserialize data Value with offset {}, row not processed", String.valueOf(lastoffset));
					}

					processor.process(lasttopic, lastoffset, partition, keyrecord, valuerecord);
					rowsfetched++;
					break;
				case 0: 
					// keep alive packet
					break;
				}
				state = OperationState.FETCHWAITINGFORDATA;
			}
		}
		return rowsfetched;
	}

	@Override
	public void setTopics() throws PropertiesException {
	}

	@Override
	public void open() throws IOException {
		state = OperationState.OPEN;
		http.getHttpConnection(url, "POST");
		try (OutputStream out = http.getConnection().getOutputStream();) {
			io.sendString(out, getProperties().getName());
			io.sendString(out, getProperties().getTopicPattern());
			io.sendInt(out, getProperties().getFlushMaxRecords());
			io.sendLong(out, getProperties().getFlushMaxTime());
		}
		state = OperationState.DONEOPEN;
	}

	@Override
	public void close() {
		try {
			state = OperationState.CLOSE;
			http.getHttpConnection(url, "DELETE");
			int status = http.getConnection().getResponseCode();
			if (status < 200 || status >= 300) {
				logger.info("Close failed on server", http.getConnection().getResponseMessage());
			}
		} catch (IOException e) {
			logger.info("Close failed with exception", e);
		}
		state = OperationState.DONECLOSE;
	}

	@Override
	public void commit() throws IOException {
		state = OperationState.DOEXPLICITCOMMIT;
		http.getHttpConnection(url, "PUT");
		int status = http.getConnection().getResponseCode();
		if (status < 200 || status >= 300) {
			throw new PipelineTemporaryException(http.getConnection().getResponseMessage());
		}
		state = OperationState.DONEEXPLICITCOMMIT;
	}

}
