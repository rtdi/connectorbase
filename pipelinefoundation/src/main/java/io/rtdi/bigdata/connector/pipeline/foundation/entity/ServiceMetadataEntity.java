package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import java.util.ArrayList;
import java.util.List;

public class ServiceMetadataEntity {
	private List<ServiceEntity> servicelist;

	public ServiceMetadataEntity() {
	}

	public ServiceMetadataEntity(List<ServiceEntity> producerlist) {
		this();
		setServiceList(producerlist);
	}

	public List<ServiceEntity> getServiceList() {
		return servicelist;
	}

	public void setServiceList(List<ServiceEntity> producerlist) {
		this.servicelist = producerlist;
	}
	
	public void remove(String servicename) {
		if (servicelist != null) {
			for (int i = 0; i<servicelist.size(); i++) {
				ServiceEntity p = servicelist.get(i);
				if (p.getServiceName().equals(servicename)) {
					servicelist.remove(i);
					break;
				}
			}
		}		
	}

	public void update(ServiceEntity service) {
		if (servicelist == null) {
			servicelist = new ArrayList<>();
		} else {
			remove(service.getServiceName());
		}
		servicelist.add(service);
	}

}
