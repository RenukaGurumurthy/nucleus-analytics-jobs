package org.gooru.nucleus.consumer.sync.jobs.commands;

import org.gooru.nucleus.consumer.sync.jobs.processors.ProcessClassJoin;
import org.gooru.nucleus.consumer.sync.jobs.processors.ProcessClassStudentRemove;
import org.gooru.nucleus.consumer.sync.jobs.processors.ProcessItemCopy;
import org.gooru.nucleus.consumer.sync.jobs.processors.ProcessItemCreate;
import org.gooru.nucleus.consumer.sync.jobs.processors.ProcessItemDelete;
import org.gooru.nucleus.consumer.sync.jobs.processors.ProcessItemMove;
import org.gooru.nucleus.consumer.sync.jobs.processors.ProcessItemUpdate;
import org.gooru.nucleus.consumer.sync.jobs.processors.ProcessUserActivity;
import org.gooru.nucleus.consumer.sync.jobs.processors.ProcessCollaboratorsUpdate;
import org.json.JSONObject;

public final class CommandProcessorFactory {
  public static void ItemDelete(JSONObject event) {
    new ProcessItemDelete(event).execute();
  }
  public static void ItemCopy(JSONObject event) {
    new ProcessItemCopy(event).execute();
  }
  public static void ItemMove(JSONObject event) {
    new ProcessItemMove(event).execute();
  }
  public static void ItemCreate(JSONObject event) {
    new ProcessItemCreate(event).execute();
  }
  public static void ItemUpdate(JSONObject event) {
    new ProcessItemUpdate(event).execute();
  }
  public static void ClassJoin(JSONObject event) {
    new ProcessClassJoin(event).execute();
  }
  public static void ClassStudentRemove(JSONObject event) {
    new ProcessClassStudentRemove(event).execute();
  }
  public static void CollaboratorsUpdate(JSONObject event) {
	    new ProcessCollaboratorsUpdate(event).execute();	    
	  }  
  public static void UserSignIn(JSONObject event) {
	    new ProcessUserActivity(event).execute();	    
	  }
  public static void UserSignOut(JSONObject event) {
	    new ProcessUserActivity(event).execute();	    
	  }
}
