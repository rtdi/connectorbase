package io.rtdi.bigdata.connectors.pipeline.kafkadirect;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

import io.rtdi.bigdata.connector.pipeline.foundation.entity.LoadInfo;

/**
 * A cache of all producer's LoadInfos
 *
 */
public class LoadInfoContainer {
	private long lastreadtime = 0L;
	private Cache<String, Map<Integer, Map<String, LoadInfo>>> loadinfocache = Caffeine
			.newBuilder()
			.expireAfterAccess(Duration.ofMinutes(120))
			.removalListener((String key, Map<Integer, Map<String, LoadInfo>> value, RemovalCause cause) -> cachetimeout(key, value, cause)).build();

	public long getLastReadTime() {
		return lastreadtime;
	}

	public void setLastReadTime(long timestamp) {
		this.lastreadtime = timestamp;
	}

	public void put(String producername, Integer instanceno, String schemaname, LoadInfo i) {
		Map<Integer, Map<String, LoadInfo>> instance = loadinfocache.get(producername, x -> new HashMap<>());
		Map<String, LoadInfo> schemadata = instance.get(instanceno);
		if (schemadata == null) {
			schemadata = new HashMap<>();
			instance.put(instanceno, schemadata);
		}
		schemadata.put(schemaname, i);
	}

	public void remove(String producername, Integer instanceno, String schemaname) {
		Map<Integer, Map<String, LoadInfo>> instance = loadinfocache.get(producername, x -> new HashMap<>());
		if (instance != null) {
			Map<String, LoadInfo> schemadata = instance.get(instanceno);
			if (schemadata != null) {
				schemadata.remove(schemaname);
			}
		}
	}

	public Map<Integer, Map<String, LoadInfo>> get(String producername) {
		return loadinfocache.getIfPresent(producername);
	}

	private void cachetimeout(String key, Map<Integer, Map<String, LoadInfo>> value, RemovalCause cause) {
		/*
		 * No matter what entry has been removed, the reader needs to start at the beginning
		 */
		lastreadtime = 0L;
	}
	
}
