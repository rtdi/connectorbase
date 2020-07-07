package io.rtdi.bigdata.pipelinehttp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import javax.ws.rs.core.MediaType;

import org.apache.avro.generic.GenericRecord;

import io.rtdi.bigdata.connector.pipeline.foundation.AvroSerializer;
import io.rtdi.bigdata.connector.pipeline.foundation.ProducerSession;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineTemporaryException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.HttpUtil;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.IOUtils;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

public class ProducerSessionHttp extends ProducerSession<TopicHandlerHttp> {
	private PipelineHttp api;
	private OutputStream out = null;
	private IOUtils io = new IOUtils();
	private URL url = null;
	private HttpUtil http;

	public ProducerSessionHttp(ProducerProperties properties, PipelineHttp api) throws PropertiesException {
		super(properties, api);
		this.api = api;
	}

	@Override
	public void beginImpl() throws PipelineRuntimeException {
		try {
			http.getHttpConnection(url, "POST");
			http.getConnection().setRequestProperty("Content-Type", MediaType.APPLICATION_OCTET_STREAM);
			http.getConnection().setChunkedStreamingMode(1024*1024*2);
			http.getConnection().setUseCaches(false);
			
			out = http.getConnection().getOutputStream();
		} catch (IOException e) {
			throw new PipelineRuntimeException("open of the http connection to the connection server failed", e, (url != null?url.toString():null));
		}
	}

	@Override
	public void commitImpl() throws IOException {
		io.sendInt(out, 2); // send commit message
		out.close();
		@SuppressWarnings("unused")
		int responsecode = http.getConnection().getResponseCode();
		http.updateCookies();
		try (InputStream in = http.getConnection().getInputStream();) {
			int len = in.read();
			int firstbyte = len;
			while (len != -1) {
				len = in.read();
			}
			switch (firstbyte) {
			case -1: 
				throw new IOException("No response from remote");
			case 2:
				// commit successful
				break;
			case 1:
				throw new PipelineTemporaryException("Commit did not get through");
			}
		}
	}

	@Override
	protected void abort() throws PipelineRuntimeException {
		close();
	}

	@Override
	public void open() throws PipelineRuntimeException {
		try {
			url = api.getTransactionEndpointForSend().toURL();
			String username = api.getAPIProperties().getUser();
			String password = api.getAPIProperties().getPassword();
			http = new HttpUtil(username, password);
		} catch (IOException e) {
			throw new PipelineRuntimeException("creating the url failed", e, (url != null?url.toString():null));
		}
	}

	@Override
	public void close() {
		if (out != null) {
			try {
				out.close();
				http.updateCookies();
				out = null;
			} catch (IOException e) {
			}
		}
	}

	@Override
	protected void addRowImpl(TopicHandlerHttp topic, Integer partition, SchemaHandler handler, GenericRecord keyrecord, GenericRecord valuerecord) throws IOException {
		byte[] key = AvroSerializer.serialize(handler.getDetails().getKeySchemaID(), keyrecord);
		byte[] value = AvroSerializer.serialize(handler.getDetails().getValueSchemaID(), valuerecord);
		addRowBinary(topic, partition, key, value);
	}

	@Override
	public void addRowBinary(TopicHandler topic, Integer partition, byte[] keyrecord, byte[] valuerecord) throws IOException {
		io.sendInt(out, 1);
		io.sendString(out, topic.getTopicName().getName());
		
		if (partition == null) {
			io.sendInt(out, -1);
		} else {
			io.sendInt(out, partition.intValue());
		}
		io.sendBytes(out, keyrecord);
		io.sendBytes(out, valuerecord);
	}

}
