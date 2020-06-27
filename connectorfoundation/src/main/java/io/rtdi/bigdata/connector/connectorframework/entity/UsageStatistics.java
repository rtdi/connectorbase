package io.rtdi.bigdata.connector.connectorframework.entity;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConsumerController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConsumerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerController;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ErrorEntity;

public class UsageStatistics {

	private String connectorname;
	private String connectorjar;
	private String apijar;
	private List<Connection> connections;
	private Map<String, Integer> connectionindex;
	private long starttime;
	private long endtime;
	private String companyname;

	public UsageStatistics() {
	}

	public UsageStatistics(ConnectorController connector, UsageStatistics prev) {
		this.connectorname = connector.getName();
		this.starttime = (prev != null? prev.endtime : System.currentTimeMillis());
		this.endtime = System.currentTimeMillis();
		this.companyname = connector.getGlobalSettings().getCompanyName();
		File file = new File(connector.getConnectorFactory().getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
		connectorjar = file.getName();
		file = new File(connector.getPipelineAPI().getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
		apijar = file.getName();
		Map<String, ConnectionController> connections = connector.getConnections();
		if (connections != null) {
			this.connections = new ArrayList<>();
			this.connectionindex = new HashMap<>();
			for (ConnectionController connection : connections.values()) {
				String name = connection.getName();
				this.connectionindex.put(name, this.connections.size());
				this.connections.add(new Connection(connection, getConnectionByNameFrom(name, prev)));
			}
		}
	}
	
	private Connection getConnectionByName(String name) {
		Integer index = connectionindex.get(name);
		if (index != null) {
			return connections.get(index);
		} else {
			return null;
		}
	}
	
	private Connection getConnectionByNameFrom(String name, UsageStatistics prev) {
		if (prev != null) {
			return prev.getConnectionByName(name);
		} else {
			return null;
		}
	}

	public String getConnectorname() {
		return connectorname;
	}

	public void setConnectorname(String connectorname) {
		this.connectorname = connectorname;
	}

	public List<Connection> getConnections() {
		return connections;
	}

	public void setConnections(List<Connection> connections) {
		this.connections = connections;
		if (connectionindex == null) {
			connectionindex = new HashMap<>();
		}
		for (int i=0; i<connections.size(); i++) {
			Connection c = connections.get(i);
			connectionindex.put(c.getConnectionname(), i);
		}
	}

	public long getStarttime() {
		return starttime;
	}

	public void setStarttime(long starttime) {
		this.starttime = starttime;
	}

	public long getEndtime() {
		return endtime;
	}

	public void setEndtime(long endtime) {
		this.endtime = endtime;
	}


	public String getConnectorjar() {
		return connectorjar;
	}

	public void setConnectorjar(String connectorjar) {
		this.connectorjar = connectorjar;
	}


	public String getPipelineAPIjar() {
		return apijar;
	}

	public void setPipelineAPIjar(String apijar) {
		this.apijar = apijar;
	}


	public String getCompanyname() {
		return companyname;
	}

	public void setCompanyname(String companyname) {
		this.companyname = companyname;
	}


	public static class Connection {

		private String connectionname;
		private List<Producer> producers;
		private Map<String, Integer> producerindex;
		private List<Consumer> consumers;
		private Map<String, Integer> consumerindex;
		private List<ErrorEntity> errors;

		public Connection() {
		}
		
		public Connection(ConnectionController connection, Connection prev) {
			this.connectionname = connection.getName();
			HashMap<String, ProducerController> producers = connection.getProducers();
			errors = connection.getErrorListRecursive();
			if (producers != null) {
				this.producers = new ArrayList<>();
				this.producerindex = new HashMap<>();
				for (ProducerController producercontroller : producers.values()) {
					String name = producercontroller.getName();
					this.producerindex.put(name, this.producers.size());
					this.producers.add(new Producer(producercontroller, getProducerByNameFrom(name, prev)));
				}
			}
			HashMap<String, ConsumerController> consumers = connection.getConsumers();
			if (consumers != null) {
				this.consumers = new ArrayList<>();
				this.consumerindex = new HashMap<>();
				for (ConsumerController consumercontroller : consumers.values()) {
					String name = consumercontroller.getName();
					this.consumerindex.put(name, this.consumers.size());
					this.consumers.add(new Consumer(consumercontroller, getConsumerByNameFrom(name, prev)));
				}
			}
		}

		private Producer getProducerByName(String name) {
			if (producers != null && producerindex != null) {
				int index = producerindex.get(name);
				return producers.get(index);
			} else {
				return null;
			}
		}
		
		private Producer getProducerByNameFrom(String name, Connection prev) {
			if (prev != null) {
				return prev.getProducerByName(name);
			} else {
				return null;
			}
		}
		
		private Consumer getConsumerByName(String name) {
			if (consumerindex != null && consumerindex != null) {
				int index = consumerindex.get(name);
				return consumers.get(index);
			} else {
				return null;
			}
		}
		
		private Consumer getConsumerByNameFrom(String name, Connection prev) {
			if (prev != null) {
				return prev.getConsumerByName(name);
			} else {
				return null;
			}
		}

		public String getConnectionname() {
			return connectionname;
		}

		public void setConnectionname(String connectionname) {
			this.connectionname = connectionname;
		}

		public List<Producer> getProducers() {
			return producers;
		}

		public void setProducers(List<Producer> producers) {
			this.producers = producers;
		}

		public List<Consumer> getConsumers() {
			return consumers;
		}

		public void setConsumers(List<Consumer> consumers) {
			this.consumers = consumers;
		}

		public List<ErrorEntity> getErrors() {
			return errors;
		}

		public void setErrors(List<ErrorEntity> errors) {
			this.errors = errors;
		}

	}
	
	public static class Producer {

		private String producername;
		private List<ProducerInstance> instances;

		public Producer() {
		}

		public Producer(ProducerController producer, Producer prev) {
			this.producername = producer.getName();
			HashMap<String, ProducerInstanceController> instances = producer.getInstances();
			if (instances != null) {
				this.instances = new ArrayList<>();
				int i=0;
				for (ProducerInstanceController instance : instances.values()) {
					this.instances.add(new ProducerInstance(instance, (prev != null? prev.instances.get(i): null)));
					i++;
				}
			}
		}

		public String getProducername() {
			return producername;
		}

		public void setProducername(String producername) {
			this.producername = producername;
		}

		public List<ProducerInstance> getInstances() {
			return instances;
		}

		public void setInstances(List<ProducerInstance> instances) {
			this.instances = instances;
		}

		public int getInstancecount() {
			if (instances != null) {
				return instances.size();
			} else {
				return 0;
			}
		}
	}
	
	public static class ProducerInstance {

		private Long lastdatatimestamp;
		private int pollcalls;
		private String state;
		private long rowsproduced;
		private List<ErrorEntity> errors;

		public ProducerInstance() {
		}

		public ProducerInstance(ProducerInstanceController instance, ProducerInstance prev) {
			this.lastdatatimestamp = instance.getLastProcessed();
			this.state = instance.getState().name();
			errors = instance.getErrorListRecursive();
			if (prev == null) {
				this.pollcalls = instance.getPollCalls();
				this.rowsproduced = instance.getRowsProduced();
			} else {
				this.pollcalls = instance.getPollCalls() - prev.pollcalls;
				this.rowsproduced = instance.getRowsProduced() - prev.rowsproduced;
			}
		}

		public Long getLastdatatimestamp() {
			return lastdatatimestamp;
		}

		public void setLastdatatimestamp(Long lastdatatimestamp) {
			this.lastdatatimestamp = lastdatatimestamp;
		}

		public int getPollcalls() {
			return pollcalls;
		}

		public void setPollcalls(int pollcalls) {
			this.pollcalls = pollcalls;
		}

		public String getState() {
			return state;
		}

		public void setState(String state) {
			this.state = state;
		}

		public long getRowsproduced() {
			return rowsproduced;
		}

		public void setRowsproduced(long rowsproduced) {
			this.rowsproduced = rowsproduced;
		}

		public List<ErrorEntity> getErrors() {
			return errors;
		}

		public void setErrors(List<ErrorEntity> errors) {
			this.errors = errors;
		}
	}
	
	public static class Consumer {

		private String consumername;
		private List<ConsumerInstance> instances;

		public Consumer() {
		}

		public Consumer(ConsumerController consumer, Consumer prev) {
			this.consumername = consumer.getName();
			HashMap<String, ConsumerInstanceController> instances = consumer.getInstances();
			if (instances != null) {
				this.instances = new ArrayList<>();
				int i=0;
				for (ConsumerInstanceController instance : instances.values()) {
					this.instances.add(new ConsumerInstance(instance, (prev != null? prev.instances.get(i): null)));
					i++;
				}
			}
		}

		public String getConsumername() {
			return consumername;
		}

		public void setConsumername(String consumername) {
			this.consumername = consumername;
		}

		public List<ConsumerInstance> getInstances() {
			return instances;
		}

		public void setInstances(List<ConsumerInstance> instances) {
			this.instances = instances;
		}

		public int getInstancecount() {
			if (instances != null) {
				return instances.size();
			} else {
				return 0;
			}
		}
	}
	
	public static class ConsumerInstance {
		
		private Long lastdatatimestamp;
		private int fetchcalls;
		private long rowsfetched;
		private String state;
		private List<ErrorEntity> errors;

		public ConsumerInstance() {
		}

		public ConsumerInstance(ConsumerInstanceController instance, ConsumerInstance prev) {
			this.lastdatatimestamp = instance.getLastProcessed();
			this.state = instance.getState().name();
			errors = instance.getErrorListRecursive();
			if (prev == null) {
				this.fetchcalls = instance.getFetchCalls();
				this.rowsfetched = instance.getRowsFetched();
			} else {
				this.fetchcalls = instance.getFetchCalls() - prev.fetchcalls;
				this.rowsfetched = instance.getRowsFetched() - prev.rowsfetched;
			}
		}

		public Long getLastdatatimestamp() {
			return lastdatatimestamp;
		}

		public void setLastdatatimestamp(Long lastdatatimestamp) {
			this.lastdatatimestamp = lastdatatimestamp;
		}

		public int getFetchcalls() {
			return fetchcalls;
		}

		public void setFetchcalls(int fetchcalls) {
			this.fetchcalls = fetchcalls;
		}

		public long getRowsfetched() {
			return rowsfetched;
		}

		public void setRowsfetched(long rowsfetched) {
			this.rowsfetched = rowsfetched;
		}

		public String getState() {
			return state;
		}

		public void setState(String state) {
			this.state = state;
		}

		public List<ErrorEntity> getErrors() {
			return errors;
		}

		public void setErrors(List<ErrorEntity> errors) {
			this.errors = errors;
		}
	}
	
}
