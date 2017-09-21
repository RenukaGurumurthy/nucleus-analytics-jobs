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

public class ProcessItemCreate {

  private JSONObject event;
  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessItemCreate.class);
  private List<Map> user = null;
  private static SimpleDateFormat minuteDateFormatter;
  public ProcessItemCreate(JSONObject event) {
    this.event = event;
    minuteDateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
  }

  public void execute() {
    LOGGER.debug("Processing Create Event : {} " + event);
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
        user = Base.findAll(QueryConstants.SELECT_AUTHORIZED_USER_EXISTS, contentGooruId, userId);
        if (user != null && contentFormat != null && contentFormat.equalsIgnoreCase(AttributeConstants.ATTR_CLASS)) {
          updateClassAuthorizedTable(contentGooruId, userId);
        }
        
        JSONObject data = payLoad.getJSONObject(AttributeConstants.DATA);
        if (data != null && contentFormat != null && AttributeConstants.CONTENT_FORMAT_FOR_TITLES.matcher(contentFormat).matches()) {
          updateContentTable(contentGooruId, contentFormat, data.isNull(AttributeConstants.TITLE) ? null : data.getString(AttributeConstants.TITLE),
                  data.isNull(AttributeConstants.SUBJECT_BUCKET) ? null : data.getString(AttributeConstants.SUBJECT_BUCKET),
                  data.isNull(AttributeConstants.CODE) ? null : data.getString(AttributeConstants.CODE),
                  data.isNull(AttributeConstants.TAXONOMY) ? null : data.getJSONObject(AttributeConstants.TAXONOMY).toString());
        }
                
        if (data != null && contentFormat != null && contentFormat.equalsIgnoreCase(AttributeConstants.BOOKMARK)) {
          Timestamp updatedAt = null;
          if (!data.isNull(AttributeConstants.ATTR_UPDATED_AT)) {
            try {
              updatedAt = new Timestamp(minuteDateFormatter.parse(data.getString(AttributeConstants.ATTR_UPDATED_AT)).getTime());
            } catch (ParseException e) {
              LOGGER.error("Date ParseException while parsing updated at attribute {} ", e);
            }
          }
          updateLearnerBookmarksTable(data.isNull(AttributeConstants.ATTR_ID) ? null : data.getString(AttributeConstants.ATTR_ID),
                  data.isNull(AttributeConstants.ATTR_CONTENT_ID) ? null : data.getString(AttributeConstants.ATTR_CONTENT_ID),
                  data.isNull(AttributeConstants.ATTR_USER_ID) ? null : data.getString(AttributeConstants.ATTR_USER_ID),
                  data.isNull(AttributeConstants.ATTR_CONTENT_TYPE) ? null : data.getString(AttributeConstants.ATTR_CONTENT_TYPE),
                  data.isNull(AttributeConstants.TITLE) ? null : data.getString(AttributeConstants.TITLE), updatedAt);
        }
        return null;
      }
    });
    LOGGER.debug("DONE");
  }

  private void updateClassAuthorizedTable(String contentGooruId, String userId) {
    if (user.isEmpty()) {
      LOGGER.debug("classId : {} - userId : {}", contentGooruId, userId);
      Base.exec(QueryConstants.INSERT_AUTHORIZED_USER, contentGooruId, userId,"creator");
      LOGGER.debug("Class authorized data inserted successfully...");
    } else {
      LOGGER.debug("User already present. Do nothing...");
    }

  }

  private void updateContentTable(String contentGooruId, String contentFormat, String title, String taxSubjectId,String code, String taxonomy) {
    if (title != null) {
      LOGGER.debug("contentGooruId : {} - title : {} - contentFormat : {}", contentGooruId, title, taxSubjectId, contentFormat);
      LOGGER.debug("code : {}", code);
      LOGGER.debug("taxonomy : {}", taxonomy);
      Base.exec(QueryConstants.INSERT_CONTENT, contentGooruId, contentFormat, title, taxSubjectId,code,taxonomy);
      LOGGER.debug("Content inserted successfully...");
    } else {
      LOGGER.debug("Title can not be null...");
    }

  }
  
  private void updateLearnerBookmarksTable(String id, String contentId, String userId, String contentType, String title, Timestamp updated_at) {
	  if (id != null && contentId != null && userId != null && contentType != null) {
		  Base.exec(QueryConstants.INSERT_LEARNER_BOOKMARKS, id, contentId, userId, contentType, title, updated_at);
	      LOGGER.debug("Learner Bookmarks inserted successfully...");  
	  } else {
	      LOGGER.debug("id, contentId, userId or contentType cannot be null for bookmarks. Record not inserted");
	  }

	  }
}
