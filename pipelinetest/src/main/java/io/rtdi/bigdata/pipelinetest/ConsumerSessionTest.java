package io.rtdi.bigdata.pipelinetest;

import java.io.IOException;
import java.util.List;

import io.rtdi.bigdata.connector.pipeline.foundation.ConsumerSession;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineBase;
import io.rtdi.bigdata.connector.pipeline.foundation.IProcessFetchedRow;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicPayload;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.OperationState;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;

public class ConsumerSessionTest extends ConsumerSession<TopicHandlerTest> {

	protected ConsumerSessionTest(ConsumerProperties properties, IPipelineBase<?, TopicHandlerTest> api, String tenantid) throws PropertiesException {
		super(properties, tenantid, api);
	}

	@Override
	public int fetchBatch(IProcessFetchedRow processor) throws IOException {
		state = OperationState.FETCH;
		TopicHandlerTest topic = getTopic(getProperties().getTopicPattern());
		int rowsfetched = 0;
		if (topic != null) {
			state = OperationState.FETCHWAITINGFORDATA;
			List<TopicPayload> data = topic.getData();
			for (TopicPayload p : data) {
				if (lastoffset < p.getOffset()) {
					state = OperationState.FETCHGETTINGROW;
					processor.process(topic.getTopicName().getName(), p.getOffset(), 1, p.getKeyRecord(), p.getValueRecord(), p.getKeySchemaId(), p.getValueSchemaId());
					lastoffset = p.getOffset();
					rowsfetched++;
				}
			}
			if (rowsfetched == 0) {
				try {
					Thread.sleep(getProperties().getFlushMaxTime()); // throttle the CPU consumption a bit as this is a non-blocking implementation
				} catch (InterruptedException e) {
				}
			}
		}
		state = OperationState.DONEFETCH;
		return rowsfetched;
	}

	@Override
	public void setTopics() throws PropertiesException {
		TopicHandlerTest topic = getPipelineAPI().getTopic(new TopicName(getTenantId(), getProperties().getTopicPattern()));
		addTopic(topic);
	}

	@Override
	public void open() throws PropertiesException {
		state = OperationState.DONEOPEN;
	}

	@Override
	public void close() {
		state = OperationState.DONECLOSE;
	}

	@Override
	public void commit() throws PipelineRuntimeException {
		state = OperationState.DONEEXPLICITCOMMIT;
	}

}
