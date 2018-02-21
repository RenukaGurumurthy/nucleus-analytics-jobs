package org.gooru.nucleus.consumer.sync.jobs.processors;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import org.gooru.nucleus.consumer.sync.jobs.constants.AttributeConstants;
import org.gooru.nucleus.consumer.sync.jobs.constants.QueryConstants;
import org.gooru.nucleus.consumer.sync.jobs.infra.DBHandler;
import org.gooru.nucleus.consumer.sync.jobs.infra.TransactionExecutor;
import org.javalite.activejdbc.Base;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessUserActivity {
	
	  private JSONObject event;
	  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessUserActivity.class);
	  private static SimpleDateFormat minuteDateFormatter;
	  public ProcessUserActivity(JSONObject event) {
	    this.event = event;
	    minuteDateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	  }

	  public void execute() {
	    LOGGER.debug("Processing User Sign In Event : {} " + event);
	    Long endTime = event.get(AttributeConstants.ATTR_END_TIME) != null ? Long.valueOf((event.get(AttributeConstants.ATTR_END_TIME).toString())) : null;
	    String eventName = event.getString(AttributeConstants.ATTR_EVENT_NAME);
	    JSONObject payLoad = event.getJSONObject(AttributeConstants.ATTR_PAY_LOAD);
	    JSONObject userObject = event.getJSONObject(AttributeConstants.USER);
	    String userId = userObject.getString(AttributeConstants.GOORUID);
	    LOGGER.debug("userId : {}", userId);

	    TransactionExecutor.executeWithAnalyticsDBTransaction(new DBHandler() {
	      @Override
	      public Object execute() {
	        
	        JSONObject data = payLoad.getJSONObject(AttributeConstants.DATA);	        
	        if (endTime != null) {
	          Timestamp updatedAt = null;	          
	          updatedAt = new Timestamp(endTime);
	          updateUserActivity(eventName, data.isNull(AttributeConstants.ATTR_ID) ? null : data.getString(AttributeConstants.ATTR_ID),
	        		  data.isNull(AttributeConstants.ATTR_TENANT_ID) ? null : data.getString(AttributeConstants.ATTR_TENANT_ID),	        		  
	        				  data.isNull(AttributeConstants.ATTR_LOGIN_TYPE) ? null : data.getString(AttributeConstants.ATTR_LOGIN_TYPE),	        		  
	        				  data.isNull(AttributeConstants.ATTR_USER_CATEGORY) ? null : data.getString(AttributeConstants.ATTR_USER_CATEGORY),
                              updatedAt);
	        }
	        return null;
	      }
	    });
	    LOGGER.debug("DONE");
	  }
	  
	  private void updateUserActivity(String eventName, String id, String tenantId, String loginType, String userCat, Timestamp updated_at) {
		  if (id != null) {
			  Base.exec(QueryConstants.INSERT_USER_ACTIVITY, eventName, id, tenantId, loginType, userCat, updated_at);
		      LOGGER.debug("User Sign In Activity inserted successfully...");  
		  } else {
		      LOGGER.debug("User Sign In Activity Record cannot inserted");
		  }

		  }

}
