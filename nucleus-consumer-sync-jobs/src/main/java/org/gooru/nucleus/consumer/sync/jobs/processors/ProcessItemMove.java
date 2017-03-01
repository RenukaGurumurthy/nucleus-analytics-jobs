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

public class ProcessItemMove {

  private JSONObject event;
  private List<Map> totalCount = null;

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessItemMove.class);

  public ProcessItemMove(JSONObject event) {
    this.event = event;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void execute() {
    JSONObject payLoad = event.getJSONObject(AttributeConstants.ATTR_PAY_LOAD);
    JSONObject source = payLoad.isNull("source") ? null : payLoad.getJSONObject("source");
    JSONObject target = payLoad.isNull("target") ? null : payLoad.getJSONObject("target");
    JSONObject context = event.isNull("context") ? null : event.getJSONObject("context");

    LOGGER.debug("context : {}", context);
    LOGGER.debug("source : {}", source);
    LOGGER.debug("target : {}", target);
    String contentFormat = payLoad.getString(AttributeConstants.ATTR_CONTENT_FORMAT);
    LOGGER.debug("ContentFormat : {}", contentFormat);

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
        if (source != null) {
          deleteCourseCollectionCount(
                  source.isNull(AttributeConstants.ATTR_COURSE_GOORU_ID) ? null : source.getString(AttributeConstants.ATTR_COURSE_GOORU_ID),
                  source.isNull(AttributeConstants.ATTR_UNIT_GOORU_ID) ? null : source.getString(AttributeConstants.ATTR_UNIT_GOORU_ID),
                  source.isNull(AttributeConstants.ATTR_LESSON_GOORU_ID) ? null : source.getString(AttributeConstants.ATTR_LESSON_GOORU_ID),
                  source.isNull(AttributeConstants.ATTR_CONTENT_GOORU_ID) ? null : source.getString(AttributeConstants.ATTR_CONTENT_GOORU_ID),
                  payLoad.getString(AttributeConstants.ATTR_CONTENT_FORMAT));
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

  private void updateCourseCollectionCount(String courseId, String unitId, String lessonId, String leastContentId, String contentFormat) {
    boolean rowExist = false;
    Object rowCount = Base.firstCell(QueryConstants.SELECT_ROW_COUNT, courseId, unitId, lessonId);
    if (Integer.valueOf(rowCount.toString()) > 0) {
      rowExist = true;
    }
    switch (contentFormat) {
    
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
      this.totalCount.forEach(r -> {
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
