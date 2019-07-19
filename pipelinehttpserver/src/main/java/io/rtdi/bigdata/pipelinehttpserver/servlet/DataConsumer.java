package io.rtdi.bigdata.pipelinehttpserver.servlet;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.HttpConstraint;
import javax.servlet.annotation.ServletSecurity;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.ws.rs.core.Response.Status;

import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.connector.pipeline.foundation.AvroSerializer;
import io.rtdi.bigdata.connector.pipeline.foundation.ConsumerSession;
import io.rtdi.bigdata.connector.pipeline.foundation.IOUtils;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineServer;
import io.rtdi.bigdata.connector.pipeline.foundation.IProcessFetchedRow;
import io.rtdi.bigdata.pipelinehttpserver.WebAppController;


/**
 * Servlet implementation class DataConsumer
 */
@WebServlet("/data/consumer")
@ServletSecurity(@HttpConstraint(rolesAllowed = JerseyApplication.DEFAULT_ROLE))
public class DataConsumer extends HttpServlet {
	private static final long serialVersionUID = 1L;
	protected final Logger logger = LogManager.getLogger(this.getClass().getName());
      
    /**
     * @see HttpServlet#HttpServlet()
     */
    public DataConsumer() {
        super();
    }

	/**
	 * Called by the fetch() method
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		HttpSession session = request.getSession(false);
		if (session == null) {
			response.sendError(Status.BAD_REQUEST.getStatusCode(), "No session information provided");
		} else {
			try {
				ConsumerSession<?> consumersession = (ConsumerSession<?>) session.getAttribute("CONSUMER");
				if (consumersession == null) {
					response.sendError(Status.BAD_REQUEST.getStatusCode(), "Session does not contain a consumersession object");
				}
				IOUtils io = new IOUtils();
				long rununtil = System.currentTimeMillis() + consumersession.getProperties().getFlushMaxTime();
				int maxrows = consumersession.getProperties().getFlushMaxRecords();
				int rowcount = 0;
				
				ServletOutputStream out = response.getOutputStream();
				RowCopy rowcopy = new RowCopy(out, io);
				while (System.currentTimeMillis() < rununtil && rowcount < maxrows) {
					int rowsfetched = consumersession.fetchBatch(rowcopy);
					if (rowsfetched == 0) {
						io.sendInt(out, 0); // send a keep-alive packet
					}
					out.flush();
					rowcount += rowsfetched;
				}
				out.flush();
				out.close();
				Index.getServerStatisticsHandler().incRowsConsumed(rowcount);
			} catch (IOException e) {
				logger.info("Calling the Restful service \"/data/consumer\" (GET) ran into an error", e);
				response.sendError(Status.BAD_REQUEST.getStatusCode(), e.getMessage());
			}
		}
	}
	
	private class RowCopy implements IProcessFetchedRow {

		private ServletOutputStream out;
		private IOUtils io;

		public RowCopy(ServletOutputStream out, IOUtils io) {
			this.out = out;
			this.io = io;
		}

		@Override
		public void process(String topic, long offset, int partition, byte[] key, byte[] value) throws IOException {
			io.sendInt(out, 1);
			io.sendString(out, topic);
			io.sendLong(out, offset);
			io.sendInt(out, partition);
			io.sendBytes(out, key);
			io.sendBytes(out, value);
		}

		@Override
		public void process(String name, long offset, int partition, GenericRecord keyRecord, GenericRecord valueRecord, int keyschemaid, int valueschemaid) throws IOException {
			byte[] key = AvroSerializer.serialize(keyschemaid, keyRecord);
			byte[] value = AvroSerializer.serialize(valueschemaid, valueRecord);
			process(name, offset, partition, key, value);
		}
		
	}

	/**
	 * Executed by the open() method.<br>
	 * Task is to create a session object in the webserver, receive the request which 
	 * topics to listen on and to attach an active ConsumerSession.
	 * 
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		IPipelineServer<?,?,?,?> api = WebAppController.getPipelineAPIOrFail(getServletContext());
		IOUtils io = new IOUtils();
		String topicpattern = null;
		String tenantid = null;
		String consumername = null;
		int fetchmaxrecords;
		long fetchmaxtime;
		try ( ServletInputStream in = request.getInputStream(); ) {
			tenantid = io.readString(in);
			consumername = io.readString(in);
			topicpattern = io.readString(in);
			fetchmaxrecords = io.readInt(in);
			fetchmaxtime = io.readLong(in);
			if (in.read() != -1) {
				logger.info("Calling the Restful service \"/data/consumer\" received wrong data over the wire");
				response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Extra data received, protocol error");
			}
			ConsumerSession<?> consumersession = api.createNewConsumerSession(consumername, topicpattern, tenantid);
			consumersession.getProperties().setFlushMaxRecords(fetchmaxrecords);
			consumersession.getProperties().setFlushMaxTime(fetchmaxtime);
			consumersession.open();
			consumersession.setTopics();
			
			HttpSession session = request.getSession(true);
			session.setAttribute("CONSUMER", consumersession);
			Index.getServerStatisticsHandler().incConsumerSession();
		} catch (IOException e) {
			logger.info("Calling the Restful service \"/data/consumer\" (POST) ran into an error", e);
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
		}
	}

	/**
	 * Execute a commit on the receiver to confirm all data had been processed.
	 * 
	 * @see HttpServlet#doPut(HttpServletRequest, HttpServletResponse)
	 */
	protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		HttpSession session = request.getSession(false);
		if (session == null) {
			response.sendError(Status.BAD_REQUEST.getStatusCode(), "No session information provided");
		} else {
			try {
				ConsumerSession<?> consumersession = (ConsumerSession<?>) session.getAttribute("CONSUMER");
				if (consumersession == null) {
					response.sendError(Status.BAD_REQUEST.getStatusCode(), "Session does not contain a consumersession object");
				} else {
					consumersession.commit();
					Index.getServerStatisticsHandler().incConsumerCommit();
				}
			} catch (IOException e) {
				logger.info("Calling the Restful service \"/data/consumer\" (PUT) ran into an error", e);
				response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Error when commiting data" + e.getMessage());
			}
		}
	}

	/**
	 * Called by the close() method
	 * 
	 * @see HttpServlet#doDelete(HttpServletRequest, HttpServletResponse)
	 */
	protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		HttpSession session = request.getSession(false);
		if (session == null) {
			response.sendError(Status.BAD_REQUEST.getStatusCode(), "No session information provided");
		} else {
			ConsumerSession<?> consumersession = (ConsumerSession<?>) session.getAttribute("CONSUMER");
			if (consumersession == null) {
				response.sendError(Status.BAD_REQUEST.getStatusCode(), "http session has no consumer object");
			} else {
				session.invalidate();
				consumersession.close();
			}
		}
	}

}
