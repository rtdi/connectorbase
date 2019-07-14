package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ConsumerMetadataEntity {
	private List<ConsumerEntity> consumerlist;

	public ConsumerMetadataEntity() {
	}

	public List<ConsumerEntity> getConsumerList() {
		return consumerlist;
	}

	public void setConsumerList(List<ConsumerEntity> consumerlist) {
		this.consumerlist = consumerlist;
	}
	
	public void addAll(Collection<ConsumerEntity> values) {
		consumerlist = new ArrayList<>();
		consumerlist.addAll(values);
	}

	public void remove(String consumername) {
		if (consumerlist != null) {
			for (int i = 0; i<consumerlist.size(); i++) {
				ConsumerEntity c = consumerlist.get(i);
				if (c.getConsumerName().equals(consumername)) {
					consumerlist.remove(i);
					break;
				}
			}
		}		
	}

	public void update(ConsumerEntity consumer) {
		if (consumerlist == null) {
			consumerlist = new ArrayList<>();
		} else {
			remove(consumer.getConsumerName());
		}
		consumerlist.add(consumer);
	}
	
}
