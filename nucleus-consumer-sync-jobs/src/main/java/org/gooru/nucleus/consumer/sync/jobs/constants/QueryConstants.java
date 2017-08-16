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

  public static final String SELECT_COLLECTION_COUNT_BY_CUL_ID = "select co.id as course_id,u.unit_id,l.lesson_id,count(distinct col.id) as collection_count,count(distinct ass.id) as assessment_count,count(distinct extass.id) as ext_assessment_count from course co left join unit u on (co.id = u.course_id) left join lesson l on (u.unit_id = l.unit_id) left join collection col on (l.lesson_id = col.lesson_id and col.format = 'collection' and col.is_deleted = false) left join collection ass on (l.lesson_id = ass.lesson_id and ass.format = 'assessment' and ass.is_deleted = false) left join collection extass on (l.lesson_id = extass.lesson_id and extass.format = 'assessment-external' and extass.is_deleted = false) where co.id = ? ::uuid AND u.unit_id = ? ::uuid AND l.lesson_id = ? ::uuid group by co.id,u.unit_id,l.lesson_id";

  public static final String SELECT_COLLECTION_COUNT_BY_CU_ID = "select co.id as course_id,u.unit_id,l.lesson_id,count(distinct col.id) as collection_count,count(distinct ass.id) as assessment_count,count(distinct extass.id) as ext_assessment_count from course co left join unit u on (co.id = u.course_id) left join lesson l on (u.unit_id = l.unit_id) left join collection col on (l.lesson_id = col.lesson_id and col.format = 'collection' and col.is_deleted = false) left join collection ass on (l.lesson_id = ass.lesson_id and ass.format = 'assessment' and ass.is_deleted = false) left join collection extass on (l.lesson_id = extass.lesson_id and extass.format = 'assessment-external' and extass.is_deleted = false) where co.id = ? ::uuid AND u.unit_id = ? ::uuid AND l.lesson_id IS NOT NULL group by co.id,u.unit_id,l.lesson_id";

  public static final String SELECT_COLLECTION_COUNT_BY_COURSE_ID = "select co.id as course_id,u.unit_id,l.lesson_id,count(distinct col.id) as collection_count,count(distinct ass.id) as assessment_count,count(distinct extass.id) as ext_assessment_count from course co left join unit u on (co.id = u.course_id) left join lesson l on (u.unit_id = l.unit_id) left join collection col on (l.lesson_id = col.lesson_id and col.format = 'collection' and col.is_deleted = false) left join collection ass on (l.lesson_id = ass.lesson_id and ass.format = 'assessment' and ass.is_deleted = false) left join collection extass on (l.lesson_id = extass.lesson_id and extass.format = 'assessment-external' and extass.is_deleted = false) where co.id = ? ::uuid AND u.unit_id IS NOT NULL AND l.lesson_id IS NOT NULL group by co.id,u.unit_id,l.lesson_id";

  /*************************** DELETE Queries For ReComputations Purpose *************************/
  
  public static final String DELETE_BASEREPORT_BY_COURSE = "DELETE FROM base_reports WHERE class_id = ? AND course_id = ?";
 
  public static final String DELETE_BASEREPORT_BY_UNIT = "DELETE FROM base_reports WHERE class_id = ? AND course_id = ?";
  
  public static final String DELETE_BASEREPORT_BY_LESSON = "DELETE FROM base_reports WHERE class_id = ? AND lesson_id = ?";
  
  public static final String DELETE_BASEREPORT_BY_COLLECTION = "DELETE FROM base_reports WHERE class_id = ? AND collection_id = ?";
  
  /**********************************************************************************************/
  
  /*************************** Class Authorized user update *************************************/

  public static final String SELECT_AUTHORIZED_USER_EXISIST =  "SELECT * FROM class_authorized_users WHERE class_id = ? AND user_id = ?";
  public static final String INSERT_AUTHORIZED_USER = "INSERT INTO class_authorized_users(class_id,user_id,user_type)VALUES(?,?,?)";
  public static final String UPDATE_AUTHORIZED_USER = "UPDATE class_authorized_users SET user_id = ? WHERE class_id = ? ";
 
  /**********************************************************************************************/
  
  /*************************** Content update **************************************************/

  public static final String INSERT_CONTENT = "INSERT INTO content(id,content_format,title, tax_subject_id,class_code, taxonomy)VALUES(?,?,?,?,?,?)";
  public static final String UPDATE_CONTENT = "UPDATE content SET title = ?, tax_subject_id = ?, taxonomy = ? WHERE id = ? ";
 
  /**********************************************************************************************/

  /*************************** Class Member update **************************************************/
  public static final String INSERT_CLASS_MEMEBER = "INSERT INTO class_member (class_id, user_id, class_member_status) VALUES (?,?,?)";
  public static final String DELETE_CLASS_MEMBER = "DELETE FROM class_member WHERE class_id = ? AND user_id = ?";
  
  /*************************** Update Learner Bookmarks Table **************************************************/
  
  public static final String INSERT_LEARNER_BOOKMARKS = "INSERT INTO learner_bookmarks (id, content_id, user_id, "
  		+ "content_type, title, updated_at) VALUES (?,?,?,?,?,?)";
  public static final String DELETE_LEARNER_BOOKMARKS = "DELETE FROM learner_bookmarks WHERE id = ? AND content_id = ? AND user_id = ?";
  
}
