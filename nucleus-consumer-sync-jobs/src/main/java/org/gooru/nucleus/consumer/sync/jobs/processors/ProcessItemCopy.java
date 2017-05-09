package org.gooru.nucleus.consumer.sync.jobs.processors;

import java.sql.PreparedStatement;
import java.sql.SQLException;
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

public class ProcessItemCopy {

  private JSONObject event;
  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessItemCopy.class);
  private List<Map> totalCount = null;
  private List<Map> user = null;

  public ProcessItemCopy(JSONObject event) {
    this.event = event;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void execute() {
    LOGGER.debug("Processing Copy Event : {} " + event);
    JSONObject payLoad = event.getJSONObject(AttributeConstants.ATTR_PAY_LOAD);
    JSONObject source = payLoad.isNull("source") ? null : payLoad.getJSONObject("source");
    JSONObject target = payLoad.isNull("target") ? null : payLoad.getJSONObject("target");
    JSONObject context = event.isNull("context") ? null : event.getJSONObject("context");
    JSONObject userObject = event.getJSONObject(AttributeConstants.USER);

    LOGGER.debug("context : {}", context);
    LOGGER.debug("source : {}", source);
    LOGGER.debug("target : {}", target);
    String contentFormat = payLoad.getString(AttributeConstants.ATTR_CONTENT_FORMAT);
    LOGGER.debug("ContentFormat : {}", contentFormat);
    
    String contentGooruId =
            context.isNull(AttributeConstants.ATTR_CONTENT_GOORU_ID) ? null : context.getString(AttributeConstants.ATTR_CONTENT_GOORU_ID);
    String userId = userObject.getString(AttributeConstants.GOORUID);
    LOGGER.debug("contentGooruId : {}", contentGooruId);
    LOGGER.debug("contentFormat : {}", contentFormat);
    LOGGER.debug("userId : {}", userId);

    if (contentFormat.equalsIgnoreCase(AttributeConstants.ATTR_COURSE) || contentFormat.equalsIgnoreCase(AttributeConstants.ATTR_UNIT)
            || contentFormat.equalsIgnoreCase(AttributeConstants.ATTR_LESSON)) {
      this.totalCount = (List<Map>) TransactionExecutor.executeWithCoreDBTransaction(new DBHandler() {
        @Override
        public Object execute() {
	
          return getCoreDBCollectionCount(
                  target.isNull(AttributeConstants.ATTR_COURSE_GOORU_ID) ? null : target.getString(AttributeConstants.ATTR_COURSE_GOORU_ID),
                  target.isNull(AttributeConstants.ATTR_UNIT_GOORU_ID) ? null : target.getString(AttributeConstants.ATTR_UNIT_GOORU_ID),
                  target.isNull(AttributeConstants.ATTR_LESSON_GOORU_ID) ? null : target.getString(AttributeConstants.ATTR_LESSON_GOORU_ID),
                  context.isNull(AttributeConstants.ATTR_CONTENT_GOORU_ID) ? null : context.getString(AttributeConstants.ATTR_CONTENT_GOORU_ID),
                  contentFormat);
        }
      });

    }

    TransactionExecutor.executeWithAnalyticsDBTransaction(new DBHandler() {
      @Override
      public Object execute() {
      	  LOGGER.debug("Finding users from class_authorized_table");
          user = Base.findAll(QueryConstants.SELECT_AUTHORIZED_USER_EXISIST, contentGooruId, userId);
          if (user != null && contentFormat != null && contentFormat.equalsIgnoreCase(AttributeConstants.ATTR_CLASS)) {
        	  LOGGER.debug("Updating users from class_authorized_table");  
            updateClassAuthorizedTable(contentGooruId, userId);
          }
    	  
        if (target != null) {
          updateCourseCollectionCount(
                  target.isNull(AttributeConstants.ATTR_COURSE_GOORU_ID) ? null : target.getString(AttributeConstants.ATTR_COURSE_GOORU_ID),
                  target.isNull(AttributeConstants.ATTR_UNIT_GOORU_ID) ? null : target.getString(AttributeConstants.ATTR_UNIT_GOORU_ID),
                  target.isNull(AttributeConstants.ATTR_LESSON_GOORU_ID) ? null : target.getString(AttributeConstants.ATTR_LESSON_GOORU_ID),
                  context.isNull(AttributeConstants.ATTR_CONTENT_GOORU_ID) ? null : context.getString(AttributeConstants.ATTR_CONTENT_GOORU_ID),
                  payLoad.getString(AttributeConstants.ATTR_CONTENT_FORMAT));
        }
        LOGGER.info("DONE");
        return null;
      }
    });

  }
  

  private void updateClassAuthorizedTable(String contentGooruId, String userId) {
    if (user.isEmpty()) {
      LOGGER.debug("classId : {} - userId : {}", contentGooruId, userId);
      Base.exec(QueryConstants.INSERT_AUTHORIZED_USER, contentGooruId, userId,"creator");
      LOGGER.info("Class authorized data inserted successfully...");
    } else {
      LOGGER.info("User already present. Do nothing...");
    }

  }

  private void updateCourseCollectionCount(String courseId, String unitId, String lessonId, String leastContentId, String contentFormat) {
    switch (contentFormat) {
    case AttributeConstants.ATTR_COLLECTION:
    case AttributeConstants.ATTR_ASSESSMENT:
    case AttributeConstants.ATTR_EXTERNAL_ASSESSMENT:
      // Do nothing. it will be handled in item.move event.
      break;
    case AttributeConstants.ATTR_COURSE:
    case AttributeConstants.ATTR_UNIT:
    case AttributeConstants.ATTR_LESSON:
      try {
        insertBatchData();
      } catch (SQLException e) {
        LOGGER.error(e.getMessage());
      }
      break;
    default:
      LOGGER.warn("Invalid content format. Please have a look at it.");
    }
  }

  private void insertBatchData() throws SQLException {
    PreparedStatement ps = Base.startBatch(QueryConstants.INSERT_COURSE_COLLECTION_COUNT);
    if (this.totalCount != null && !this.totalCount.isEmpty()) {
      totalCount.forEach(r -> {
        Base.addBatch(ps, r.get(AttributeConstants.ATTR_COURSE_ID), r.get(AttributeConstants.ATTR_UNIT_ID), r.get(AttributeConstants.ATTR_LESSON_ID),
                r.get(AttributeConstants.ATTR_COLLECTION_COUNT), r.get(AttributeConstants.ATTR_ASSESSMENT_COUNT),
                r.get(AttributeConstants.ATTR_EXT_ASSESSMENT_COUNT));
      });
      Base.executeBatch(ps);
      ps.close();
    }
  }

  private List<Map> getCoreDBCollectionCount(String courseId, String unitId, String lessonId, String leastContentId, String contentFormat) {
    List<Map> totalCount = null;
    switch (contentFormat) {
    case AttributeConstants.ATTR_COURSE:
      totalCount = Base.findAll(QueryConstants.SELECT_COLLECTION_COUNT_BY_COURSE_ID, leastContentId);
      break;
    case AttributeConstants.ATTR_UNIT:
      totalCount = Base.findAll(QueryConstants.SELECT_COLLECTION_COUNT_BY_CU_ID, courseId, leastContentId);
      break;
    case AttributeConstants.ATTR_LESSON:
      totalCount = Base.findAll(QueryConstants.SELECT_COLLECTION_COUNT_BY_CUL_ID, courseId, unitId, leastContentId);
      break;
    default:
      LOGGER.warn("Invalid content format. Please have a look at it.");
    }
    return totalCount;
  }
}
