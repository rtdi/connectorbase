package io.rtdi.bigdata.pipelinehttpserver.servlet;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.annotation.HttpConstraint;
import javax.servlet.annotation.ServletSecurity;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.connector.pipeline.foundation.IOUtils;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineServer;
import io.rtdi.bigdata.connector.pipeline.foundation.ProducerSession;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.pipelinehttpserver.WebAppController;

@WebServlet("/data/producer")
@ServletSecurity(@HttpConstraint(rolesAllowed = JerseyApplication.DEFAULT_ROLE))
public class DataProducer extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static Map<String, Map<String, TopicHandler>> topiccache = new HashMap<>();
	protected final Logger logger = LogManager.getLogger(this.getClass().getName());

    /**
     * @see HttpServlet#HttpServlet()
     */
    public DataProducer() {
        super();
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		response.getWriter().append("Served at: ").append(request.getContextPath());
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		IPipelineServer<?,?,?,?> api = WebAppController.getPipelineAPIOrFail(getServletContext());
		IOUtils io = new IOUtils();
		boolean committed = true;
		String tenantid;
		HttpSession session = request.getSession(true);
		ProducerSession<?> producersession = (ProducerSession<?>) session.getAttribute("PRODUCERSESSION");
		if (producersession == null) {
			tenantid = request.getHeader("TENANTID"); // The tenant is taken from the header when the session is created
			producersession = api.createNewProducerSession(tenantid);
			session.setAttribute("PRODUCERSESSION", producersession);
			Index.getServerStatisticsHandler().incProducerSession();
		} else {
			tenantid = producersession.getTenantId();
		}
		
		try (
			ServletInputStream in = request.getInputStream();
				) {
			/* 
			 * One message consists of
			 * <ul><li>Size of the Topicname</li>
			 * <li>Topicname as UTF-8 encoded byte array</li>
			 * <li>The partition integer, negative means NULL</li>
			 * <li>Size of the key Avro message</li>
			 * <li>payload of the key</li>
			 * <li>Size of the value Avro message</li>
			 * <li>payload of the value</li></ul>
			 */
			producersession.beginImpl( );
			while (io.readNextIntValue(in)) {
				switch (io.getNextIntValue()) {
				case 1: // add row message
					String topic = io.readString(in);
					int p = io.readInt(in);
					Integer partition;
					if (p < 0) {
						partition = null;
					} else {
						partition = p;
					}

					byte[] key = io.readBytes(in);
					byte[] value = io.readBytes(in);
					
					Map<String, TopicHandler> tenanttopiccache = topiccache.get(tenantid);
					if (tenanttopiccache == null) {
						tenanttopiccache = new HashMap<>();
						topiccache.put(tenantid, tenanttopiccache);
					}
					TopicHandler topichandler = tenanttopiccache.get(topic);
					if (topichandler == null) {
						topichandler = api.getTopic(new TopicName(tenantid, topic));
						tenanttopiccache.put(topic, topichandler);
					}
					producersession.addRowBinary(topichandler, partition, key, value);
					committed = false;
					Index.getServerStatisticsHandler().incProducerRecord();
					break;
				case 2: // commit message
					producersession.commitTransaction();
					Index.getServerStatisticsHandler().incProducerCommit();
					committed = true;
				}
			}
			try (OutputStream out = response.getOutputStream(); ) {
				if (committed) {
					out.write(0x02);
				} else {
					out.write(0x01);
				}
				out.flush();
			}
		} catch (IOException e) {
			logger.info("Streaming data to the service \"/data/producer\" ran into an error", e);
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Communication with the source failed.\n" + e.getMessage());
		}
	}

}
