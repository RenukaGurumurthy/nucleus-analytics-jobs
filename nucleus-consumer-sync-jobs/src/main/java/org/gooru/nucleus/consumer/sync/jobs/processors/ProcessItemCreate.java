package org.gooru.nucleus.consumer.sync.jobs.processors;

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

  public ProcessItemCreate(JSONObject event) {
    this.event = event;
  }

  public void execute() {
    System.out.println("Processing Create Event : {} " + event);
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
        user = Base.findAll(QueryConstants.SELECT_AUTHORIZED_USER_EXISIST, contentGooruId, userId);
        if (user != null && contentFormat != null && contentFormat.equalsIgnoreCase(AttributeConstants.ATTR_CLASS)) {
          updateClassAuthorizedTable(contentGooruId, userId);
        }
        if (user != null && contentFormat != null) {
          updateContentTable(contentGooruId, contentFormat, payLoad.getJSONObject(AttributeConstants.DATA).getString(AttributeConstants.TITLE));
        }
        return null;
      }
    });
    LOGGER.debug("DONE");
  }

  private void updateClassAuthorizedTable(String contentGooruId, String userId) {
    if (user.isEmpty()) {
      LOGGER.debug("classId : {} - userId : {}", contentGooruId, userId);
      Base.exec(QueryConstants.INSERT_AUTHORIZED_USER, contentGooruId, userId);
      LOGGER.debug("Class authorized data inserted successfully...");
    } else {
      LOGGER.debug("User already present. Do nothing...");
    }

  }

  private void updateContentTable(String contentGooruId, String contentFormat, String title) {
    if (title != null) {
      LOGGER.debug("contentGooruId : {} - title : {} - contentFormat : {}", contentGooruId, title, contentFormat);
      Base.exec(QueryConstants.INSERT_CONTENT, contentGooruId, contentFormat, title);
      LOGGER.debug("Content inserted successfully...");
    } else {
      LOGGER.debug("Title can not be null...");
    }

  }
}
