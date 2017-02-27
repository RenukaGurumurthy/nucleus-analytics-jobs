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

  public ProcessItemCopy(JSONObject event) {
    this.event = event;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void execute() {
    LOGGER.debug("Process Copy Event : {} " + event);
    JSONObject payLoad = event.getJSONObject(AttributeConstants.ATTR_PAY_LOAD);
    JSONObject source = payLoad.getJSONObject("source");
    JSONObject target = payLoad.getJSONObject("target");
    JSONObject context = payLoad.getJSONObject("context");

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
          return getCoreDBCollectionCount(target.getString(AttributeConstants.ATTR_COURSE_GOORU_ID),
                  target.getString(AttributeConstants.ATTR_UNIT_GOORU_ID), target.getString(AttributeConstants.ATTR_LESSON_GOORU_ID),context.getString(AttributeConstants.ATTR_CONTENT_GOORU_ID), contentFormat);
        }
      });

    }

    TransactionExecutor.executeWithAnalyticsDBTransaction(new DBHandler() {
      @Override
      public Object execute() {

        if (target != null) {
          updateCourseCollectionCount(target.getString(AttributeConstants.ATTR_COURSE_GOORU_ID),
                  target.getString(AttributeConstants.ATTR_UNIT_GOORU_ID), target.getString(AttributeConstants.ATTR_LESSON_GOORU_ID),
                  context.getString(AttributeConstants.ATTR_CONTENT_GOORU_ID), payLoad.getString(AttributeConstants.ATTR_CONTENT_FORMAT));
        }
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
      //Do nothing. it will be handled in item.move event.
      break;
    case AttributeConstants.ATTR_ASSESSMENT:
      //Do nothing. it will be handled in item.move event.
      break;
    case AttributeConstants.ATTR_EXTERNAL_ASSESSMENT:
      //Do nothing. it will be handled in item.move event.
      break;
    case AttributeConstants.ATTR_COURSE:
      try {
        insertBatchData();
      } catch (SQLException e) {
        LOGGER.error(e.getMessage());
      }
      break;
    case AttributeConstants.ATTR_UNIT:
      try {
        insertBatchData();
      } catch (SQLException e) {
        LOGGER.error(e.getMessage());
      }
      break;
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
                r.get(AttributeConstants.ATTR_ASSESSMENT_COUNT));
      });
      Base.executeBatch(ps);
      ps.close();
    }
  }

  private List<Map> getCoreDBCollectionCount(String courseId, String unitId, String lessonId,String leastContentId, String contentFormat) {
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
