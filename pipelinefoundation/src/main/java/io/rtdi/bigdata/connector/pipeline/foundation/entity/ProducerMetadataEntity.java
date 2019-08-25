package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import java.util.ArrayList;
import java.util.List;

public class ProducerMetadataEntity {
	private List<ProducerEntity> producerlist;

	public ProducerMetadataEntity() {
	}

	public ProducerMetadataEntity(List<ProducerEntity> producerlist) {
		this();
		setProducerList(producerlist);
	}

	public List<ProducerEntity> getProducerList() {
		return producerlist;
	}

	public void setProducerList(List<ProducerEntity> producerlist) {
		this.producerlist = producerlist;
	}
	
	public void remove(String producername) {
		if (producerlist != null) {
			for (int i = 0; i<producerlist.size(); i++) {
				ProducerEntity p = producerlist.get(i);
				if (p.getProducerName().equals(producername)) {
					producerlist.remove(i);
					break;
				}
			}
		}		
	}

	public void update(ProducerEntity producer) {
		if (producerlist == null) {
			producerlist = new ArrayList<>();
		} else {
			remove(producer.getProducerName());
		}
		producerlist.add(producer);
	}

}
