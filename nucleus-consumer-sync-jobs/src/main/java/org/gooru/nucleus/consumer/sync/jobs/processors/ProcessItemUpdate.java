package org.gooru.nucleus.consumer.sync.jobs.processors;

import org.gooru.nucleus.consumer.sync.jobs.constants.AttributeConstants;
import org.gooru.nucleus.consumer.sync.jobs.constants.QueryConstants;
import org.gooru.nucleus.consumer.sync.jobs.infra.DBHandler;
import org.gooru.nucleus.consumer.sync.jobs.infra.TransactionExecutor;
import org.javalite.activejdbc.Base;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessItemUpdate {

  private JSONObject event;
  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessItemUpdate.class);

  public ProcessItemUpdate(JSONObject event) {
    this.event = event;
  }

  public void execute() {
    LOGGER.debug("Processing Update Event : {} " + event);
    JSONObject context = event.getJSONObject(AttributeConstants.ATTR_CONTEXT);
    JSONObject payLoad = event.getJSONObject(AttributeConstants.ATTR_PAY_LOAD);
    String contentGooruId =
            context.isNull(AttributeConstants.ATTR_CONTENT_GOORU_ID) ? null : context.getString(AttributeConstants.ATTR_CONTENT_GOORU_ID);
    String contentFormat = payLoad.isNull(AttributeConstants.ATTR_CONTENT_FORMAT) ? null : payLoad.getString(AttributeConstants.ATTR_CONTENT_FORMAT);
    LOGGER.debug("contentGooruId : {}", contentGooruId);
    LOGGER.debug("contentFormat : {}", contentFormat);

    TransactionExecutor.executeWithAnalyticsDBTransaction(new DBHandler() {
      @Override
      public Object execute() {
        if (contentGooruId != null && contentFormat != null) {
          updateContentTable(contentGooruId, payLoad.getJSONObject(AttributeConstants.DATA).getString(AttributeConstants.TITLE),payLoad.getJSONObject(AttributeConstants.DATA).getString(AttributeConstants.SUBJECT_BUCKET));
        }
        return null;
      }
    });
    LOGGER.debug("DONE");
  }

  private void updateContentTable(String contentGooruId, String title, String taxSubjectId) {
    if (title != null) {
      LOGGER.debug("contentGooruId : {} - title : {} ", contentGooruId, title);
      Base.exec(QueryConstants.UPDATE_CONTENT, title, taxSubjectId, contentGooruId);
      LOGGER.debug("Content updated successfully...");
    } else {
      LOGGER.debug("Title can not be null...");
    }

  }
}
