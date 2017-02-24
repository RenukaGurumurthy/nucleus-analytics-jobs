package org.gooru.nucleus.consumer.sync.jobs.constants;

public class QueryConstants {

  public static final String UPDATE_ALL_COUNT = "UPDATE course_collection_count SET collection_count = ? , assessment_count = ? , ext_assessment_count = ? WHERE course_id = ? AND unit_id = ? AND lesson_id = ?";

  public static final String UPDATE_COLLECTION_COUNT = "UPDATE course_collection_count SET collection_count = (collection_count + ?) WHERE course_id = ? AND unit_id = ? AND lesson_id = ?";

  public static final String UPDATE_ASSESSMENT_COUNT = "UPDATE course_collection_count SET assessment_count = (assessment_count + ?)  WHERE course_id = ? AND unit_id = ? AND lesson_id = ?";

  public static final String UPDATE_EXT_ASSESSMENT_COUNT = "UPDATE course_collection_count SET ext_assessment_count = (ext_assessment_count + ?) WHERE course_id = ? AND unit_id = ? AND lesson_id = ?";

  public static final String INSERT_COURSE_COLLECTION_COUNT = "INSERT INTO course_collection_count (course_id , unit_id , lesson_id , collection_count , assessment_count , ext_assessment_count)VALUES (?,?,?,?,?,?)";
  
  public static final String DELETE_COURSE_LEVEL = "DELETE FROM course_collection_count WHERE course_id = ?";
  
  public static final String DELETE_UNIT_LEVEL = "DELETE FROM course_collection_count WHERE course_id = ? AND unit_id = ?";
  
  public static final String DELETE_LESSON_LEVEL = "DELETE FROM course_collection_count WHERE course_id = ? AND unit_id = ? AND lesson_id = ?";

  public static final String SELECT_ROW_COUNT = "SELECT COUNT(1) AS rowCount FROM course_collection_count WHERE course_id = ? AND unit_id = ? AND lesson_id = ?";
}
