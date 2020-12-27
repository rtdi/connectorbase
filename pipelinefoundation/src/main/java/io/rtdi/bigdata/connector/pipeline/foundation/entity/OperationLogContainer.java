package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

	public synchronized void add(String text) {
		entities[index % size] = new StateDisplayEntry(text);
		index++;
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

		public StateDisplayEntry() {
		}
		
		public StateDisplayEntry(String text) {
			this.time = System.currentTimeMillis();
			this.text = text;
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

	}

}
