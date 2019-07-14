package io.rtdi.bigdata.pipelinehttpserver.servlet;

import javax.servlet.annotation.WebListener;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.connector.pipeline.foundation.ConsumerSession;
import io.rtdi.bigdata.connector.pipeline.foundation.ProducerSession;

/**
 * Application Lifecycle Listener implementation class UserSessionListener
 *
 */
@WebListener
public class UserSessionListener implements HttpSessionListener {
	protected final Logger logger = LogManager.getLogger(this.getClass().getName());

    /**
     * Default constructor. 
     */
    public UserSessionListener() {
    }

	/**
     * @see HttpSessionListener#sessionCreated(HttpSessionEvent)
     */
    public void sessionCreated(HttpSessionEvent se)  { 
		logger.info("User session created with id \"{}\"", se.getSession().getId());
    }

	/**
     * @see HttpSessionListener#sessionDestroyed(HttpSessionEvent)
     */
    public void sessionDestroyed(HttpSessionEvent se)  { 
		ProducerSession<?> producersession = (ProducerSession<?>) se.getSession().getAttribute("PRODUCERSESSION");
		if (producersession != null) {
			producersession.close();
		}
		ConsumerSession<?> consumersession = (ConsumerSession<?>) se.getSession().getAttribute("CONSUMER");    
		if (consumersession != null) {
			consumersession.close();
		}
		logger.info("User session for id \"{}\" got closed", se.getSession().getId());
	}
	
}
