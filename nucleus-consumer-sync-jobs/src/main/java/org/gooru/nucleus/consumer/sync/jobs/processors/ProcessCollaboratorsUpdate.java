package org.gooru.nucleus.consumer.sync.jobs.processors;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.gooru.nucleus.consumer.sync.jobs.constants.AttributeConstants;
import org.gooru.nucleus.consumer.sync.jobs.constants.QueryConstants;
import org.gooru.nucleus.consumer.sync.jobs.infra.DBHandler;
import org.gooru.nucleus.consumer.sync.jobs.infra.TransactionExecutor;
import org.javalite.activejdbc.Base;
import org.json.JSONObject;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessCollaboratorsUpdate {
	
	  private JSONObject event;
	  private List<Map> user = null;
	  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessCollaboratorsUpdate.class);
	
	public ProcessCollaboratorsUpdate(JSONObject event) {
	    this.event = event;
	  }

	  public void execute() {
	    LOGGER.debug("Processing Collaborator Update Event : {} " + event);
	    JSONObject context = event.getJSONObject(AttributeConstants.ATTR_CONTEXT);
	    JSONObject payLoad = event.getJSONObject(AttributeConstants.ATTR_PAY_LOAD);
	    JSONObject userObject = event.getJSONObject(AttributeConstants.USER);
	    String contentGooruId =
	            context.isNull(AttributeConstants.ATTR_CONTENT_GOORU_ID) ? null : context.getString(AttributeConstants.ATTR_CONTENT_GOORU_ID);
	    String contentFormat = payLoad.isNull(AttributeConstants.ATTR_CONTENT_FORMAT) ? null : payLoad.getString(AttributeConstants.ATTR_CONTENT_FORMAT);
	    String userId = userObject.getString(AttributeConstants.GOORUID);
	    LOGGER.debug("contentGooruId : {}", contentGooruId);
	    LOGGER.debug("contentFormat : {}", contentFormat);
	    LOGGER.debug("userId : {}", userId);


	    TransactionExecutor.executeWithAnalyticsDBTransaction(new DBHandler() {
	      @Override
	      public Object execute() {
	    	  
	          JSONObject data = payLoad.getJSONObject(AttributeConstants.DATA);
	          if (data != null && contentFormat != null && AttributeConstants.CONTENT_FORMAT_FOR_TITLES.matcher(contentFormat).matches()) {
	        	  JSONArray collab = data.getJSONArray("collaborators");	        	  
	        	  List<String> collabID = IntStream.range(0, collab.length())
	        	            .mapToObj(index -> (String) collab.get(index))
	        	            .collect(Collectors.toList());
	        	  
	        	  collabID.forEach(id -> {
	        	      LOGGER.info("The Collaborator ID is: " + id);
	      	          user = Base.findAll(QueryConstants.SELECT_AUTHORIZED_USER_EXISTS, contentGooruId, id);
	    	          if (user != null && contentFormat != null && contentFormat.equalsIgnoreCase(AttributeConstants.ATTR_CLASS)) {
	    	            updateClassAuthorizedTable(contentGooruId, id);
	    	          }
	        	    });	        	  
	          }
	          
	        return null;
	      }
	    });
	    LOGGER.debug("DONE");
	  }

	  private void updateClassAuthorizedTable(String contentGooruId, String userId) {
		    if (user.isEmpty()) {
		      LOGGER.debug("classId : {} - userId : {}", contentGooruId, userId);
		      Base.exec(QueryConstants.INSERT_AUTHORIZED_USER, contentGooruId, userId, "collaborator");
		      LOGGER.debug("Class authorized data inserted successfully...");
		    } else {
		      LOGGER.debug("User already present. Do nothing...");
		    }

		  }
}
