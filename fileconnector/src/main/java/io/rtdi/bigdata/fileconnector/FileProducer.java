package io.rtdi.bigdata.fileconnector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.avro.Schema;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import io.rtdi.bigdata.connector.connectorframework.Producer;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.controller.ThreadBasedController;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RowType;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;
import io.rtdi.bigdata.fileconnector.entity.EditSchemaData;
import io.rtdi.bigdata.fileconnector.service.FilePreviewService;

public class FileProducer extends Producer<FileConnectionProperties, FileProducerProperties> {

	private long pollinterval;
	private FilenameFilter filter;
	private List<File> filelist;
	private File directory;
	private TopicHandler topichandler;
	private String producername;
	private EditSchemaData format;
	private SchemaHandler schemahandler;

	public FileProducer(ProducerInstanceController instance) throws IOException {
		super(instance);
		pollinterval = getProducerProperties().getPollInterval()*1000L;
		directory = new File(getConnectionProperties().getRootDirectory());
		if (!directory.exists()) {
			throw new PropertiesException("The specified root directory does not exist", "Check the connection properties this producer belongs to");
		} else if (!directory.isDirectory()) {
			throw new PropertiesException("The specified root directory exists but is not a directory", "Check the connection properties this producer belongs to");
		}
		try {
			String schemafilename = getProducerProperties().getSchemaFile();
			if (schemafilename == null) {
				throw new PropertiesException("No schema file name specified", "check the producer settings");
			}
			File schemadir = EditSchemaData.getSchemaDirectory(getConnectionController());
			File schemafile = EditSchemaData.getSchemaFile(schemadir, schemafilename);
			format = new EditSchemaData(schemafile);
			Pattern filepattern = Pattern.compile(format.getFilenamepattern());
			filter = new FileFilter(filepattern);
		} catch (PatternSyntaxException e) {
			throw new PropertiesException("The filename pattern is not valid", "Check the pattern if it is a true regexp syntax");
		}
		this.producername = getProducerProperties().getName();
	}

	@Override
	public void startProducerChangeLogging() throws IOException {
	}

	@Override
	public void startProducerCapture() throws IOException {
		startLongRunningProducer(new FileParser());
	}
		
	protected Schema createSchema(String sourceschemaname) throws SchemaException, IOException {
		return format.createSchema();
	}
	
	@Override
	public void createTopiclist() throws IOException {
		topichandler = getTopic(getProducerProperties().getTargetTopic());
		if (topichandler == null) {
			topichandler = getPipelineAPI().topicCreate(getProducerProperties().getTargetTopic(), 1, 1);
		}
		String fileschemaname = getProducerProperties().getSchemaFile();
		schemahandler = getSchemaHandler(fileschemaname);
		if (topichandler == null) {
			throw new PropertiesException("The specified target topic does not exist", "Check producer properties");
		} else if (schemahandler == null) {
			throw new PropertiesException("The specified target schema does not exist", "Check producer properties");
		} else {
			this.addTopicSchema(topichandler, schemahandler);
		}
	}

	@Override
	public String getLastSuccessfulSourceTransaction() throws IOException {
		// The file producer does not "restart". It simply reads all files not renamed yet.
		return null;
	}

	@Override
	public void initialLoad() throws IOException {
		// Reads all files just as the realtime push does, hence nothing to do special here.
	}

	@Override
	public void restartWith(String lastsourcetransactionid) throws IOException {
		// The file producer does not "restart". It simply reads all files not renamed yet.
	}

	@Override
	public long getPollingInterval() {
		return pollinterval;
	}

	@Override
	public void closeImpl() {
	}

	private class FileParser extends ThreadBasedController<FileParser> {
		
		public FileParser() {
			super("FileParser");
		}

		@Override
		protected void runUntilError() {
			while (isRunning()) {
				filelist = readDirectory();
				if (filelist != null) {
					for (File file : filelist) {
						logger.info("Reading File {}", file.getName());
						String transactionid = file.getName();
						queueBeginTransaction(transactionid, file);
						try (FileInputStream in = new FileInputStream(file); ) {
							CsvParserSettings settings = format.getSettings();
							
							CsvParser parser = new CsvParser(settings);
							parser.beginParsing(in, FilePreviewService.getCharset(format));
							Record r;
							int rownumber = 1;
							while ((r = parser.parseNextRecord()) != null) {
								logger.info("parsing record {}", r.toString());
								String rownum = String.valueOf(rownumber);
								JexlRecord valuerecord = new JexlRecord(schemahandler.getValueSchema());
								valuerecord.put(SchemaConstants.SCHEMA_COLUMN_SOURCE_SYSTEM, producername);
								valuerecord.put("FILENAME", file.getName());
								valuerecord.put(SchemaConstants.SCHEMA_COLUMN_SOURCE_ROWID, rownum);
								
								for (String header : settings.getHeaders()) {
									valuerecord.put(header, r.getString(header));
								}
								

								logger.info("Queueing record {}", valuerecord.toString());
								queueRecord(topichandler, null, schemahandler, valuerecord, RowType.INSERT, 
										rownum, producername, transactionid);
								rownumber++;
							}
							queueCommitRecord();
						} catch (IOException e) {
							logger.info("FileProducer got an IOException", e);
							// TODO: error handling
						}
						
					}
				}
				waitTransactionsCompleted();
				try {
					Thread.sleep(getPollingInterval());
				} catch (InterruptedException e) {
				}
			}
		}

		@Override
		protected String getControllerType() {
			return "FileParser";
		}

		@Override
		protected void startThreadControllerImpl() throws IOException {
		}

		@Override
		protected void stopThreadControllerImpl(ControllerExitType exittype) {
		}
	}
	
	@Override
	public void commit(String transactionid, Object payload) throws ConnectorRuntimeException {
		File file = (File) payload;
		if (file != null) {
			File newfile = new File(file.getAbsolutePath() + ".processed");
			try {
				Files.move(file.toPath(), newfile.toPath(), StandardCopyOption.ATOMIC_MOVE);
			} catch (IOException e) {
				throw new ConnectorRuntimeException("Cannot rename the file to .processed", e, null, transactionid);
			}
		}
	}


	private List<File> readDirectory() {
		File[] files = directory.listFiles(filter);
		if (files != null) {
			return Arrays.asList(files);
		} else {
			return null;
		}
	}
	
	private class FileFilter implements FilenameFilter {
		private Pattern filepattern;

		public FileFilter(Pattern filepattern) {
			this.filepattern = filepattern;
		}

		@Override
		public boolean accept(File dir, String name) {
			if (name.endsWith(".processed")) { // failsafe in case the pattern is something like .* 
				return false;
			}
			Matcher matcher = filepattern.matcher(name);
	        boolean matches = matcher.matches();
	        if (getProducerController().getProducerCount() > 1) {
	        	/*
	        	 * In case of multiple producer instances, each producer reads one file only. Which one is derived from the file name hash value.
	        	 */
	        	int hash = name.hashCode();
	        	matches &= (hash % getProducerController().getProducerCount()) == getProducerInstance().getInstanceNumber();
	        }
	        return matches;
		}
		
	}

}
