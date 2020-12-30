package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.rtdi.bigdata.connector.pipeline.foundation.enums.RuleResult;

public class OperationLogContainer {
	private int size;
	private StateDisplayEntry[] entities;
	private int index = 0; // first element to overwrite

	public OperationLogContainer() {
		this(10);
	}

	public OperationLogContainer(int size) {
		this.size = size;
		this.entities = new StateDisplayEntry[size];
	}

	public synchronized void add(String text, String description, RuleResult state) {
		entities[index % size] = new StateDisplayEntry(text, description, state);
		index++;
	}

	public synchronized void add(String text) {
		add(text, null, RuleResult.PASS);
	}
	
	public StateDisplayEntry[] getEntities() {
		return entities;
	}

	public void setEntities(StateDisplayEntry[] entities) {
		this.entities = entities;
	}

	public List<StateDisplayEntry> asList() {
		synchronized (entities) {
			StateDisplayEntry[] a = Arrays.copyOf(entities, size);
			List<StateDisplayEntry> l = new ArrayList<>();
			for (StateDisplayEntry e : a) {
				if (e != null) {
					l.add(e);
				}
			}
			Collections.sort(l);
			return l;
		}
	}
	
	public static class StateDisplayEntry implements Comparable<StateDisplayEntry> {
		private long time;
		private String text;
		private RuleResult state;
		private String description;

		public StateDisplayEntry() {
		}
		
		public StateDisplayEntry(String text) {
			this.time = System.currentTimeMillis();
			this.text = text;
			this.state = RuleResult.PASS;
		}
		
		public StateDisplayEntry(String text, String description, RuleResult state) {
			this.time = System.currentTimeMillis();
			this.text = text;
			this.state = state;
			this.description = description;
		}

		public long getTime() {
			return time;
		}

		public String getText() {
			return text;
		}

		@Override
		public int compareTo(StateDisplayEntry o) {
			if (o == null) {
				return 1;
			} else if (time == o.time) {
				return 0;
			} else if (time > o.time) {
				return 1;
			} else {
				return -1;
			}
		}

		public void setTime(long time) {
			this.time = time;
		}

		public void setText(String text) {
			this.text = text;
		}

		public RuleResult getState() {
			return state;
		}

		public void setState(RuleResult state) {
			this.state = state;
		}

		public String getDescription() {
			return description;
		}

		public void setDescription(String description) {
			this.description = description;
		}

	}

}
