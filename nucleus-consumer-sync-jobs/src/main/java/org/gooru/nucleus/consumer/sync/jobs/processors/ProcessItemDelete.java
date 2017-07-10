package org.gooru.nucleus.consumer.sync.jobs.processors;

import org.gooru.nucleus.consumer.sync.jobs.constants.AttributeConstants;
import org.gooru.nucleus.consumer.sync.jobs.constants.QueryConstants;
import org.gooru.nucleus.consumer.sync.jobs.infra.DBHandler;
import org.gooru.nucleus.consumer.sync.jobs.infra.TransactionExecutor;
import org.javalite.activejdbc.Base;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessItemDelete {

  private JSONObject event;

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessItemDelete.class);

  public ProcessItemDelete(JSONObject event) {
    this.event = event;
  }

  public void execute() {
    LOGGER.debug("Processing Delete Event : {}", event);
    TransactionExecutor.executeWithAnalyticsDBTransaction(new DBHandler() {
      @Override
      public Object execute() {
        JSONObject context = event.getJSONObject(AttributeConstants.ATTR_CONTEXT);
        JSONObject payLoad = event.getJSONObject(AttributeConstants.ATTR_PAY_LOAD);
        String classId = context.isNull(AttributeConstants.ATTR_CLASS_GOORU_ID) ? null : context.getString(AttributeConstants.ATTR_CLASS_GOORU_ID);
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
        LOGGER.info("updateCourseCollectionCount DONE");
        if (classId != null) {
          reCompute(classId, leastContentId, contentFormat);
        }
        
        if (contentFormat != null && contentFormat.equalsIgnoreCase(AttributeConstants.BOOKMARK)) {
        	updateLearnerBookmarksTable(context.isNull(AttributeConstants.ATTR_CONTENT_GOORU_ID) ? null
        			: context.getString(AttributeConstants.ATTR_CONTENT_GOORU_ID));            
          }
        return null;
      }
    });

  }

  private void updateCourseCollectionCount(String courseId, String unitId, String lessonId, String leastContentId, String contentFormat) {
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

  private void reCompute(String classId, String leastContentId, String contentFormat) {
    String query = null;
    switch (contentFormat) {
    case AttributeConstants.ATTR_COURSE:
      query = QueryConstants.DELETE_BASEREPORT_BY_COURSE;
      break;
    case AttributeConstants.ATTR_UNIT:
      query = QueryConstants.DELETE_BASEREPORT_BY_UNIT;
      break;
    case AttributeConstants.ATTR_LESSON:
      query = QueryConstants.DELETE_BASEREPORT_BY_LESSON;
      break;
    case AttributeConstants.ATTR_COLLECTION:
      query = QueryConstants.DELETE_BASEREPORT_BY_COLLECTION;
      break;
    case AttributeConstants.ATTR_ASSESSMENT:
      query = QueryConstants.DELETE_BASEREPORT_BY_COLLECTION;
      break;
    default:
      LOGGER.info("Invalid content format..");
    }
    if (query != null) {
      Base.exec(query, classId, leastContentId);
    } else {
      LOGGER.warn("Nothing to process");
    }
    LOGGER.debug("Deleted record for class : {} - contentFormat : {} - content : {} ", classId, contentFormat, leastContentId);

  }
  
  private void updateLearnerBookmarksTable(String id) {
	  if (id != null) {
		  Base.exec(QueryConstants.DELETE_LEARNER_BOOKMARKS, id);
	      LOGGER.debug("Learner Bookmark deleted");  
	  } else {
	      LOGGER.debug("id, contentId, userId cannot be null for bookmarks. Record not deleted from Learner Bookmarks");
	  }
  }
}
