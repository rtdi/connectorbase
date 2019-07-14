package io.rtdi.bigdata.connector.connectorframework;

import java.util.Arrays;

import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;


/**
 * Initialize all Jersey infrastructure.<br/>
 * If a connector needs more than the default Jersey services it would extend this class,
 * override the {@link #getPackages()} method on register the new one in the web.xml.
 * Otherwise this class is added to the web.xml.
 *
 */
public class JerseyApplication extends ResourceConfig {
	
	public JerseyApplication() {
		super();
		String[] p = getPackages();
		if (p == null) {
			packages("io.rtdi.bigdata.connector.connectorframework.rest.service");
		} else {
			p = Arrays.copyOf(p, p.length+1);
			p[p.length-1] = "io.rtdi.bigdata.connector.connectorframework.rest.service";
			packages(p);
		}
		register(JacksonFeature.class);
		register(RolesAllowedDynamicFeature2.class);
	}

	/**
	 * In case 
	 * @return null or a String[] of package names containing additional Jersey services
	 */
	protected String[] getPackages() {
		return null;
	}
}