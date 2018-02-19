package org.gooru.nucleus.consumer.sync.jobs.processors;

import java.util.ArrayList;
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

public class ProcessItemDelete {

  private JSONObject event;
  private Double score;
  private Double max_score;
  private Long timeSpent;

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
        String collectionId = context.isNull(AttributeConstants.ATTR_COLLECTION_GOORU_ID) ? null : context.getString(AttributeConstants.ATTR_COLLECTION_GOORU_ID);
        String deleteContentId =
                context.isNull(AttributeConstants.ATTR_CONTENT_GOORU_ID) ? null : context.getString(AttributeConstants.ATTR_CONTENT_GOORU_ID);
        String contentFormat =
                payLoad.isNull(AttributeConstants.ATTR_CONTENT_FORMAT) ? null : payLoad.getString(AttributeConstants.ATTR_CONTENT_FORMAT);
        
        LOGGER.debug("courseId : {}", courseId);
        LOGGER.debug("unitId : {}", unitId);
        LOGGER.debug("lessonId : {}", lessonId);
        LOGGER.debug("collectionId : {}", collectionId);
        LOGGER.debug("contentGooruId : {}", deleteContentId);
        LOGGER.debug("contentFormat : {}", contentFormat);
        updateCourseCollectionCount(courseId, unitId, lessonId, deleteContentId, contentFormat);
        LOGGER.info("updateCourseCollectionCount DONE");
        reCompute(courseId, unitId, lessonId, collectionId, deleteContentId, contentFormat);

        return null;
      }
    });

  }

  private void updateCourseCollectionCount(String courseId, String unitId, String lessonId, String deleteContentId, String contentFormat) {
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
      Base.exec(QueryConstants.DELETE_COURSE_LEVEL, deleteContentId);
      break;
    case AttributeConstants.ATTR_UNIT:
      Base.exec(QueryConstants.DELETE_UNIT_LEVEL, courseId, deleteContentId);
      break;
    case AttributeConstants.ATTR_LESSON:
      Base.exec(QueryConstants.DELETE_LESSON_LEVEL, courseId, unitId, deleteContentId);
      break;
    default:
      LOGGER.warn("Invalid content format. Please have a look at it.");
    }
  }

  private void reCompute(String courseId, String unitId, String lessonId, String collectionId, String deleteContentId, String contentFormat) {
    String query = null;
    switch (contentFormat) {
    case AttributeConstants.ATTR_COURSE:
      query = QueryConstants.DELETE_BASEREPORT_BY_COURSE;
	  if (query != null) {
		    Base.exec(query, deleteContentId);
		    LOGGER.debug("Deleted record for course : {} ", courseId);
		  } else {
		    LOGGER.warn("Course cannot be deleted");
		  }
      break;
    case AttributeConstants.ATTR_CLASS:
        query = QueryConstants.DELETE_BASEREPORT_BY_CLASS;
  	  if (query != null) {
  		    Base.exec(query, deleteContentId);
  		    LOGGER.debug("Deleted record for course : {} ", courseId);
  		  } else {
  		    LOGGER.warn("Course cannot be deleted");
  		  }
        break;
    case AttributeConstants.ATTR_UNIT:
      query = QueryConstants.DELETE_BASEREPORT_BY_UNIT;
	  if (query != null) {
		    Base.exec(query, courseId, deleteContentId);
		    LOGGER.debug("Deleted record for course : {} - content : {} ", courseId, deleteContentId);
		  } else {
		    LOGGER.warn("Unit cannot be deleted");
		  }
      break;
    case AttributeConstants.ATTR_LESSON:
      query = QueryConstants.DELETE_BASEREPORT_BY_LESSON;
      if (query != null) {
		    Base.exec(query, courseId, deleteContentId);
		    LOGGER.debug("Deleted record for course : {} - contentFormat : {} - content : {} ", courseId, deleteContentId);
		  } else {
		    LOGGER.warn("Lesson cannot be deleted");
		  }
      break;
    case AttributeConstants.ATTR_COLLECTION:
      query = QueryConstants.DELETE_BASEREPORT_BY_COLLECTION;
      if (query != null) {
		    Base.exec(query, deleteContentId);
		    LOGGER.debug("Deleted record for content : {} ", deleteContentId);
		  } else {
		    LOGGER.warn("Collection cannot be deleted");
		  }
      break;
    case AttributeConstants.ATTR_ASSESSMENT:
      query = QueryConstants.DELETE_BASEREPORT_BY_COLLECTION;
      if (query != null) {
		    Base.exec(query, deleteContentId);
		    LOGGER.debug("Deleted record for content : {} ", deleteContentId);
		  } else {
		    LOGGER.warn("Collection cannot be deleted");
		  }
      break;
    case AttributeConstants.ATTR_QUESTION:
        query = QueryConstants.DELETE_BASEREPORT_BY_RESOURCE;
        resourceRecompute(query, collectionId, deleteContentId, contentFormat);
        break;
    case AttributeConstants.ATTR_RESOURCE:
        query = QueryConstants.DELETE_BASEREPORT_BY_RESOURCE;
        resourceRecompute(query, collectionId, deleteContentId, contentFormat);
        break;
    default:
      LOGGER.info("Invalid content format..");
    }

  }
  
  private void resourceRecompute(String query, String collectionId, String deleteContentId, String contentFormat) {
	  
	  if (query != null) {		  
		  if (contentFormat.equals(AttributeConstants.ATTR_QUESTION)) {
			      List<String> sessionIds =
			    		  Base.firstColumn(QueryConstants.GET_SESSIONS, collectionId, deleteContentId);			      
			      if (!sessionIds.isEmpty()) {			    	
			    	  for (String sessionId : sessionIds) {
			    		  LOGGER.debug("Deleting records for collectionId : {} - sessionId : {} - resourceId : {}", collectionId, sessionId, deleteContentId);
				    	  Base.exec(query, collectionId, deleteContentId, sessionId);				    	  
				        	  LOGGER.debug("Computing total score, post resource deletion...");
				               List<Map> scoreTS = Base.findAll(QueryConstants.COMPUTE_SCORE_POST_DELETE_QUESTION, 
				            		  collectionId, sessionId);
				              LOGGER.debug("scoreTS {} ", scoreTS);
				              if (scoreTS != null && !scoreTS.isEmpty()) {
				                scoreTS.forEach(m -> {		                  
				                  score = (m.get(AttributeConstants.ATTR_SCORE) != null ? Double.valueOf(m.get(AttributeConstants.ATTR_SCORE).toString()) : null);
				                  LOGGER.debug("score {} ", score);
				                  max_score = (m.get(AttributeConstants.ATTR_MAX_SCORE) != null ? Double.valueOf(m.get(AttributeConstants.ATTR_MAX_SCORE).toString()) : null);
				                  LOGGER.debug("max_score {} ", max_score);
				                });
				                            
				                if (score != null && max_score != null && max_score != 0.0) {
				                  score = ((score * 100) / max_score);
				                  LOGGER.debug("Re-Computed total score {} and max_score {} ", score, max_score);
				                }
				              }
				              Object ts = Base.firstCell(QueryConstants.COMPUTE_TS_POST_DELETE, collectionId, sessionId);
				              //timeSpent = (ts != null ? Long.valueOf(ts.toString()) : null);
				              Base.exec(QueryConstants.UPDATE_SCORE_TS_POST_DELETE_QUESTION, score, max_score, (ts != null ? Long.valueOf(ts.toString()) : null),
				            		  collectionId, sessionId);
				              LOGGER.debug("Total score and timespent updated successfully post question delete");
							  Base.exec(QueryConstants.UPDATE_QUESTION_COUNT_POST_DELETE_RESOURCE, -1, collectionId, sessionId);
							  LOGGER.debug("Question Count decremented successfully...");
				      }
			      }
			  
		} else if (contentFormat.equals(AttributeConstants.ATTR_RESOURCE)) {				  
			      List<String> sessionIds =
			    		  Base.firstColumn(QueryConstants.GET_SESSIONS, collectionId, deleteContentId);			      
			      for (String sessionId : sessionIds) {
			    	  Base.exec(query, collectionId, deleteContentId, sessionId);
		              Object ts = Base.firstCell(QueryConstants.COMPUTE_TS_POST_DELETE, collectionId, sessionId);
		              //timeSpent = (ts != null ? Long.valueOf(ts.toString()) : null);
		              Base.exec(QueryConstants.UPDATE_SCORE_TS_POST_DELETE_QUESTION, score, max_score, (ts != null ? Long.valueOf(ts.toString()) : null),
		            		  collectionId, sessionId);
		              LOGGER.debug("Total timespent recomputed post resource delete");
			      }
			  		
		}

	  }
	  
  }  

}
