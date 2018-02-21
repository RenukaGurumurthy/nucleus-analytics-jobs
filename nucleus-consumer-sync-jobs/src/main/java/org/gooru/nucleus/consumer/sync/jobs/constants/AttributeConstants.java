package org.gooru.nucleus.consumer.sync.jobs.constants;

import java.util.regex.Pattern;

public class AttributeConstants {

  public static final String ATTR_EVENT_NAME = "eventName";

  public static final String ATTR_CLASS_GOORU_ID = "classGooruId";

  public static final String ATTR_COURSE_GOORU_ID = "courseGooruId";

  public static final String ATTR_UNIT_GOORU_ID = "unitGooruId";

  public static final String ATTR_LESSON_GOORU_ID = "lessonGooruId";
  
  public static final String ATTR_COLLECTION_GOORU_ID = "collectionGooruId";

  public static final String ATTR_CONTENT_GOORU_ID = "contentGooruId";

  public static final String ATTR_CONTENT_FORMAT = "contentFormat";

  public static final String ATTR_ASSESSMENT = "assessment";
  
  public static final String ATTR_QUESTION = "question";
  
  public static final String ATTR_RESOURCE = "resource";
  
  public static final String ATTR_SCORE = "score";
  
  public static final String ATTR_MAX_SCORE = "max_score";
  
  public static final String ATTR_TIME_SPENT = "time_spent";

  public static final String ATTR_EXTERNAL_ASSESSMENT = "assessment-external";

  public static final String ATTR_COLLECTION = "collection";

  public static final String ATTR_CLASS = "class";

  public static final String ATTR_COURSE = "course";

  public static final String ATTR_UNIT = "unit";

  public static final String ATTR_LESSON = "lesson";

  public static final String ATTR_USER = "user";

  public static final String ATTR_PAY_LOAD = "payLoadObject";

  public static final String ATTR_METRICS = "metrics";

  public static final String ATTR_CONTEXT = "context";
  
  public static final  String ATTR_GOORUID = "gooruUId" ;

  public static final  String ATTR_COURSE_ID = "course_id" ;

  public static final  String ATTR_UNIT_ID = "unit_id" ;

  public static final  String ATTR_LESSON_ID = "lesson_id" ;

  public static final  String ATTR_COLLECTION_COUNT = "collection_count" ;

  public static final  String ATTR_ASSESSMENT_COUNT = "assessment_count" ;

  public static final  String ATTR_EXT_ASSESSMENT_COUNT = "ext_assessment_count" ;
  
  public static final  String ATTR_ID = "id" ;
  
  public static final  String ATTR_CONTENT_ID = "content_id";
  
  public static final  String ATTR_USER_ID = "user_id";
  
  public static final  String ATTR_CONTENT_TYPE = "content_type";
  
  public static final  String ATTR_TENANT_ID = "tenant_id" ;
  
  public static final  String ATTR_LOGIN_TYPE = "login_type" ;
  
  public static final  String ATTR_USER_CATEGORY = "user_category" ;
  
  public static final  String ATTR_UPDATED_AT = "updated_at";
  
  public static final  String ATTR_CREATED_AT = "created_at";
  
  public static final  String ATTR_END_TIME = "endTime";
  
  public static final  String USER = "user";
  
  public static final  String GOORUID = "gooruUId" ;
  
  public static final  String DATA = "data" ;

  public static final  String TITLE = "title" ;
  
  public static final  String SUBJECT_BUCKET = "subject_bucket" ;

  public static final  String CODE = "code" ;

  public static final  String DISPLAY_CODE = "display_code" ;

  public static final Pattern CONTENT_FORMAT_FOR_TITLES =  Pattern.compile("collection|assessment|course|class|unit|lesson|question|resource");
  
  public static final String BOOKMARK = "bookmark";
  
  public static final  String TAXONOMY = "taxonomy" ;
  
  public static final String SYNC_COURSE_COMPETENCY_TOTAL_COUNT = "SyncCourseCompetencyTotalCount";

  public static final String SYNC_STATIC_COURSE_COMPETENCY = "SyncStaticCourseCompetency";

  public static final String DELAY = "delay";

  public static final String EXPORT_QUERY = "exportQuery";
  
  public static final String DUMP_LOCATION = "dumpLocation";

  public static final String DUMP_FILE_NAME = "dumpFileName";

  public static final String TRUNCATE_TABLE = "truncateTable";

  public static final String IMPORT_TABLE = "importTable";

  public static final String ATTRIBUTES = "attributes";


}
