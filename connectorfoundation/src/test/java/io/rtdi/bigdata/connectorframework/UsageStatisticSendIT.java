package io.rtdi.bigdata.connectorframework;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.rtdi.bigdata.connector.connectorframework.entity.UsageStatistics;
import io.rtdi.bigdata.connector.connectorframework.utils.UsageStatisticSender;

public class UsageStatisticSendIT {

	private UsageStatistics data;
	
	@Before
	public void setUp() throws Exception {
		data = new UsageStatistics();
		data.setConnectorname("Test1");
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		try {
			UsageStatisticSender sender = new UsageStatisticSender();
			sender.setUsageData(data);
			Thread t = new Thread(sender);
			t.start();
			t.join(5000);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
