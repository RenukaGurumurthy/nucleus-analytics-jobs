package org.gooru.nucleus.consumer.sync.jobs.processors;

import org.gooru.nucleus.consumer.sync.jobs.constants.AttributeConstants;
import org.gooru.nucleus.consumer.sync.jobs.constants.QueryConstants;
import org.gooru.nucleus.consumer.sync.jobs.infra.DBHandler;
import org.gooru.nucleus.consumer.sync.jobs.infra.TransactionExecutor;
import org.javalite.activejdbc.Base;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessItemMove {

  private JSONObject event;

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessItemMove.class);

  public ProcessItemMove(JSONObject event) {
    this.event = event;
  }

  public void execute() {
    TransactionExecutor.executeWithAnalyticsDBTransaction(new DBHandler() {
      @Override
      public Object execute() {
        JSONObject context = event.getJSONObject(AttributeConstants.ATTR_CONTEXT);
        JSONObject payLoad = event.getJSONObject(AttributeConstants.ATTR_PAY_LOAD);
        String lessonId = context.isNull(AttributeConstants.ATTR_LESSON_GOORU_ID) ? null : context.getString(AttributeConstants.ATTR_LESSON_GOORU_ID);
        String unitId = context.isNull(AttributeConstants.ATTR_UNIT_GOORU_ID) ? null : context.getString(AttributeConstants.ATTR_UNIT_GOORU_ID);
        String courseId = context.isNull(AttributeConstants.ATTR_COURSE_GOORU_ID) ? null : context.getString(AttributeConstants.ATTR_COURSE_GOORU_ID);
        String leastContentId =
                context.isNull(AttributeConstants.ATTR_CONTENT_GOORU_ID) ? null : context.getString(AttributeConstants.ATTR_CONTENT_GOORU_ID);
        String contentFormat =
                payLoad.isNull(AttributeConstants.ATTR_CONTENT_FORMAT) ? null : payLoad.getString(AttributeConstants.ATTR_CONTENT_FORMAT);
        LOGGER.debug("courseId : {}", courseId);
        LOGGER.debug("unitId : {}", unitId);
        LOGGER.debug("lessonId : {}", lessonId);
        LOGGER.debug("contentGooruId : {}", leastContentId);
        LOGGER.debug("contentFormat : {}", contentFormat);
        updateCourseCollectionCount(courseId, unitId, lessonId, leastContentId, contentFormat);
        deleteCourseCollectionCount(courseId, unitId, lessonId, leastContentId, contentFormat);
        LOGGER.info("DONE");
        return null;
      }
    });

  }

  private void updateCourseCollectionCount(String courseId, String unitId, String lessonId, String leastContentId, String contentFormat) {
    boolean rowExist = false;
    Object rowCount = Base.firstCell(QueryConstants.SELECT_ROW_COUNT, courseId, unitId, lessonId);
    if (Integer.valueOf(rowCount.toString()) > 0) {
      rowExist = true;
    }
    switch (contentFormat) {
    // `-1` indicates decrement 1 from existing value.
    case AttributeConstants.ATTR_COLLECTION:
      if (!rowExist) {
        Base.exec(QueryConstants.INSERT_COURSE_COLLECTION_COUNT, courseId, unitId, lessonId, 1, 0, 0);
      } else {
        Base.exec(QueryConstants.UPDATE_COLLECTION_COUNT, 1, courseId, unitId, lessonId);
      }
      break;
    case AttributeConstants.ATTR_ASSESSMENT:
      if (!rowExist) {
        Base.exec(QueryConstants.INSERT_COURSE_COLLECTION_COUNT, courseId, unitId, lessonId, 0, 1, 0);
      } else {
        Base.exec(QueryConstants.UPDATE_ASSESSMENT_COUNT, 1, courseId, unitId, lessonId);
      }
      break;
    case AttributeConstants.ATTR_EXTERNAL_ASSESSMENT:
      if (!rowExist) {
        Base.exec(QueryConstants.INSERT_COURSE_COLLECTION_COUNT, courseId, unitId, lessonId, 0, 0, 1);
      } else {
        Base.exec(QueryConstants.UPDATE_EXT_ASSESSMENT_COUNT, 1, courseId, unitId, lessonId);
      }
      break;
    case AttributeConstants.ATTR_COURSE:
      // Do nothing...
      break;
    case AttributeConstants.ATTR_UNIT:
      // Do nothing...
      break;
    case AttributeConstants.ATTR_LESSON:
      // Do nothing...
      break;
    default:
      LOGGER.warn("Invalid content format. Please have a look at it.");
    }
  }
  
  private void deleteCourseCollectionCount(String courseId, String unitId, String lessonId, String leastContentId, String contentFormat) {
    switch (contentFormat) {
    // `-1` indicates decrement 1 from existing value.
    case AttributeConstants.ATTR_COLLECTION:
      Base.exec(QueryConstants.UPDATE_COLLECTION_COUNT, -1, courseId, unitId, lessonId);
      break;
    case AttributeConstants.ATTR_ASSESSMENT:
      Base.exec(QueryConstants.UPDATE_ASSESSMENT_COUNT, -1, courseId, unitId, lessonId);
      break;
    case AttributeConstants.ATTR_EXTERNAL_ASSESSMENT:
      Base.exec(QueryConstants.UPDATE_EXT_ASSESSMENT_COUNT, -1, courseId, unitId, lessonId);
      break;
    case AttributeConstants.ATTR_COURSE:
      Base.exec(QueryConstants.DELETE_COURSE_LEVEL, leastContentId);
      break;
    case AttributeConstants.ATTR_UNIT:
      Base.exec(QueryConstants.DELETE_UNIT_LEVEL, courseId, leastContentId);
      break;
    case AttributeConstants.ATTR_LESSON:
      Base.exec(QueryConstants.DELETE_LESSON_LEVEL, courseId, unitId, leastContentId);
      break;
    default:
      LOGGER.warn("Invalid content format. Please have a look at it.");
    }
  }
}
