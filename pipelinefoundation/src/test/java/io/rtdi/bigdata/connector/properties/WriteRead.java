package io.rtdi.bigdata.connector.properties;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.rtdi.bigdata.connector.properties.ConsumerProperties;
import io.rtdi.bigdata.connector.properties.atomic.PropertyGroup;

public class WriteRead {

	private ConsumerProperties props;

	@Before
	public void setUp() throws Exception {
		props = new ConsumerProperties("testproperties");
		props.setFlushMaxRecords(100);
		props.setFlushMaxTime(1000);
		props.setTopicPattern("ab\":\\cd");
		PropertyGroup g = props.getPropertyGroup().addPropertyGroupProperty("list", "list", null, null, true);
		g.addPasswordProperty("pw", "pw", null, null, null, true);
		g.setProperty("pw", "my password");
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		try {
			File dir = new File("./src/test/resources");
			props.write(dir);
			
			ConsumerProperties propsnew = new ConsumerProperties("testproperties");
			propsnew.read(dir);
			
			assertTrue(props.toString().equals(propsnew.toString()));
		} catch (Exception e) {
			e.printStackTrace();
			fail("Not yet implemented");
		}
	}

}
