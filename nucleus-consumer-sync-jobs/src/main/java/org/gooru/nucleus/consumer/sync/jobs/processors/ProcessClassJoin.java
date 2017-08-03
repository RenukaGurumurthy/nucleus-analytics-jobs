package org.gooru.nucleus.consumer.sync.jobs.processors;

import org.gooru.nucleus.consumer.sync.jobs.constants.AttributeConstants;
import org.gooru.nucleus.consumer.sync.jobs.constants.QueryConstants;
import org.gooru.nucleus.consumer.sync.jobs.infra.DBHandler;
import org.gooru.nucleus.consumer.sync.jobs.infra.TransactionExecutor;
import org.javalite.activejdbc.Base;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessClassJoin {
  /**
   * @author daniel
   */
  private JSONObject event;
  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessClassJoin.class);

  public ProcessClassJoin(JSONObject event) {
    this.event = event;
  }

  public void execute() {
    LOGGER.debug("Processing class.join Event : {} " + event);
    JSONObject context = event.getJSONObject(AttributeConstants.ATTR_CONTEXT);
    JSONObject userObject = event.getJSONObject(AttributeConstants.USER);
    String contentGooruId =
            context.isNull(AttributeConstants.ATTR_CONTENT_GOORU_ID) ? null : context.getString(AttributeConstants.ATTR_CONTENT_GOORU_ID);
    String userId = userObject.getString(AttributeConstants.GOORUID);
    LOGGER.debug("classId : {}", contentGooruId);
    LOGGER.debug("userId : {}", userId);

    TransactionExecutor.executeWithAnalyticsDBTransaction(new DBHandler() {
      @Override
      public Object execute() {
        Base.exec(QueryConstants.INSERT_CLASS_MEMEBER, contentGooruId, userId, "joined");
        LOGGER.debug("Inserted successfully :" + contentGooruId);
        return null;
      }
    });
    LOGGER.debug("DONE");
  }

}
