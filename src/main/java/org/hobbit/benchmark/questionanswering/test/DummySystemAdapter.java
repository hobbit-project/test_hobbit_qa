package org.hobbit.benchmark.questionanswering.test;

import java.io.IOException;
import java.util.HashMap;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonArray;
import org.hobbit.core.components.AbstractSystemAdapter;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.qaldbuilder.QaldBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummySystemAdapter extends AbstractSystemAdapter {
	private static final Logger LOGGER = LoggerFactory.getLogger(DummySystemAdapter.class);
	private final String LARGSCALE="largescale", MULTILINGUAL="multilingual";
	private final String TESTING="testing", TRAINING="training";
	
	private String language,task,dataSetType,endpoint;
	private HashMap<Integer, HashMap<String,String>> multiTestingQueries;
	private HashMap<Integer, HashMap<String,String>> multiTrainingQueries;
	private HashMap<Integer, String> largeTestingQueries;
	private HashMap<Integer, String> largeTrainingQueries;
	
	private boolean setParams;
	
	public DummySystemAdapter() {
		LOGGER.info("QaDummySys: Constructed.");
	}

	@Override
	public void init() throws Exception {
		this.setParams=true;
		
		multiTrainingQueries = new HashMap<Integer, HashMap<String,String>>();
		multiTestingQueries = new HashMap<Integer, HashMap<String,String>>();
		largeTrainingQueries=new HashMap<Integer, String>();
		largeTestingQueries=new HashMap<Integer, String>();
		
		LOGGER.info("QaDummySys: Initializing - loading correct answers ...");
		this.loadDataAll();
		LOGGER.info("QaDummySys: Initializing - correct answers loaded...");
		super.init();
        LOGGER.info("QaDummySys: Initialized.");
	}

	public void receiveGeneratedData(byte[] data) {
		// Nothing to handle here.
		LOGGER.info("QaDummySys: Data received. Oops.");
	}

	public void receiveGeneratedTask(String taskIdString, byte[] data) {
		LOGGER.info("QaDummySys: Task "+taskIdString+" received.");
		
		String taskAsString = RabbitMQUtils.readString(data);
		if(this.setParams) {
			this.setParams(taskAsString);
			LOGGER.info("QaDummySys: Task = "+this.task);
			LOGGER.info("QaDummySys: Data Set = "+this.dataSetType);
			LOGGER.info("QaDummySys: Sparql Service = "+this.endpoint);
			LOGGER.info("QaDummySys: Language = "+this.language);
		}
		/** START - GET INFO from task data **/	
		//LOGGER.info("QaDummySys: From Benchmark = \n" +taskAsString);
		QaldBuilder qaldBuilder=new QaldBuilder(taskAsString);
		//LOGGER.info("QaDummySys: ID = "+qaldBuilder.getID());
		String query ="";
		if(this.task.equalsIgnoreCase(LARGSCALE)) {
			if(this.dataSetType.equalsIgnoreCase(TESTING)) {
				query=this.largeTestingQueries.get(qaldBuilder.getID());
			}
			else if(this.dataSetType.equalsIgnoreCase(TRAINING)) {
				query=this.largeTrainingQueries.get(qaldBuilder.getID());
			}
		}
		else if(this.task.equalsIgnoreCase(MULTILINGUAL)) {
			if(this.dataSetType.equalsIgnoreCase(TESTING)) {
				query=this.multiTestingQueries.get(qaldBuilder.getID()).get(this.language);
			}
			else if(this.dataSetType.equalsIgnoreCase(TRAINING)) {
				query=this.multiTrainingQueries.get(qaldBuilder.getID()).get(this.language);
			}
		}
		qaldBuilder.setQuery(query);
		//LOGGER.info("QaDummySys: From System = \n" +qaldBuilder.getQaldQuestion());
		try {
			qaldBuilder.setAnswers(this.endpoint);
		}catch(Exception ex){
			LOGGER.error("QaDummySys:"+ex.getMessage());
		}
		String qaldAnswer = qaldBuilder.getQaldQuestion();
		byte[] sendData = RabbitMQUtils.writeString(qaldAnswer);
		try {
			sendResultToEvalStorage(taskIdString, sendData);
			//LOGGER.info("QaDummySys: Data has been sent to evaluation model");
		} catch (IOException e) {
			LOGGER.error("QaDummySys: "+e.getMessage());
		}
		/** END - PREPARING AND SENDING answer **/
	}
	@Override
	public void close() throws IOException {
		LOGGER.info("QaDummySys: Closing.");
		super.close();
		LOGGER.info("QaDummySys: Closed.");
	}
	
	private void loadDataAll() {

		JsonArray multiTrainingData= JSON.readAny("data/multilingual_training.json").getAsArray();
		JsonArray multiTestingData= JSON.readAny("data/multilingual_testing.json").getAsArray();
		JsonArray largeScaleTrainingData= JSON.readAny("data/largescale_training.json").getAsArray();
		JsonArray largeScaleTestingData=JSON.readAny("data/largescale_testing.json").getAsArray();
		
		JsonArray queries;
		HashMap<String,String> mulitLingQueries;
		int id=-1;
		String query="",lang="";
		// Large scale Training
		for(int i=0;i<largeScaleTrainingData.size();i++) {
			id =Integer.parseInt(largeScaleTrainingData.get(i).getAsObject().get("id").toString());
			query = largeScaleTrainingData.get(i).getAsObject().get("query").getAsObject().get("sparql").toString();
			query = query.replace("\"", "").trim();
			
			this.largeTrainingQueries.put(id, query);
		}
		LOGGER.info("Large Scale Training Size = "+this.largeTrainingQueries.size());
		// Large scale Testing
		for(int i=0;i<largeScaleTestingData.size();i++) {
			id =Integer.parseInt(largeScaleTestingData.get(i).getAsObject().get("id").toString());
			query = largeScaleTestingData.get(i).getAsObject().get("query").getAsObject().get("sparql").toString();
			query = query.replace("\"", "").trim();
			
			this.largeTestingQueries.put(id, query);
		}
		LOGGER.info("Large Scale Testing Size = "+this.largeTestingQueries.size());
		// Multilingual Training
		for(int i=0;i<multiTrainingData.size();i++) {
			id =Integer.parseInt(multiTrainingData.get(i).getAsObject().get("id").toString());
			
			mulitLingQueries = new HashMap<String,String>();
			queries=multiTrainingData.get(i).getAsObject().get("query").getAsArray();
			for(int j=0;j<queries.size();j++) {
				query=queries.get(j).getAsObject().get("sparql").toString();
				lang=queries.get(j).getAsObject().get("language").toString();
				query = query.replace("\"", "").trim();
				lang = lang.replace("\"", "").trim();
				mulitLingQueries.put(lang,query);
			}
			this.multiTrainingQueries.put(id, mulitLingQueries);
		}
		LOGGER.info("Multilingual Training Size = "+this.multiTrainingQueries.size());
		// Multilingual Testing
		for(int i=0;i<multiTestingData.size();i++) {
			id =Integer.parseInt(multiTestingData.get(i).getAsObject().get("id").toString());
			
			mulitLingQueries = new HashMap<String,String>();
			queries=multiTestingData.get(i).getAsObject().get("query").getAsArray();
			for(int j=0;j<queries.size();j++) {
				query=queries.get(j).getAsObject().get("sparql").toString();
				lang=queries.get(j).getAsObject().get("language").toString();
				query = query.replace("\"", "").trim();
				lang = lang.replace("\"", "").trim();
				mulitLingQueries.put(lang,query);
			}
			this.multiTestingQueries.put(id, mulitLingQueries);
		}
		LOGGER.info("Multilingual Testing Size = "+this.multiTestingQueries.size());
		multiTestingData = null;
		multiTrainingData = null;
		largeScaleTestingData = null;
		largeScaleTrainingData = null;
		queries=null;
		mulitLingQueries=null;
		System.gc();
	}
	private void setParams(String jsonQusetion) {
		this.setParams=false;
		QaldBuilder tempQB = new QaldBuilder(jsonQusetion);
		//hobbit_qa_1498123456789_42_largescale_training
		String[] info = tempQB.getDatasetID().split("_");
		this.task = info[4];
		this.dataSetType = info[5].replace("\"", "");
		this.language=tempQB.getQuestionLanguage();
		this.setEndpoint(this.language);
		//this.endpoint="http://dbpedia.org/sparql";
	}
	private void setEndpoint(String lange) {
		
		switch(lange) {
			case "de":
				this.endpoint="http://dbpedia-16-10-en-de:8890/sparql";
				break;
			case "it":
				this.endpoint="http://dbpedia-16-10-en-de-it:8890/sparql";
				break;
			case "en":
			default:
				this.endpoint="http://local-dbpedia:8890/sparql";
				break;
		}
	}

}