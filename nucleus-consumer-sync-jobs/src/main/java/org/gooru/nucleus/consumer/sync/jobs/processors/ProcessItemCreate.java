package org.gooru.nucleus.consumer.sync.jobs.processors;

import org.gooru.nucleus.consumer.sync.jobs.infra.DBHandler;
import org.gooru.nucleus.consumer.sync.jobs.infra.TransactionExecutor;
import org.javalite.activejdbc.Base;
import org.json.JSONObject;

public class ProcessItemCreate {

  private JSONObject event;
  
  public ProcessItemCreate(JSONObject event){
    this.event = event;
  }
 
  public void execute() {
    System.out.println("Process Delete Event : {} " + event);
   Object alue =  TransactionExecutor.executeWithCoreDBTransaction(new DBHandler() {
      
      @Override
      public Object execute() {
        Object d =  Base.firstCell("select count(1) from basereports");
        return d;
      }
    });
   
   Object data =  TransactionExecutor.executeWithAnalyticsDBTransaction(new DBHandler() {
     
     @Override
     public Object execute() {
       Object d =  Base.firstCell("select count(1) from course_collection_count");
       return d;
     }
   });
   
   System.out.println(alue);
    
  }
}
