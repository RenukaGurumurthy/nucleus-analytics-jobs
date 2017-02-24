package org.gooru.nucleus.consumer.sync.jobs.commands;

import org.gooru.nucleus.consumer.sync.jobs.processors.ProcessItemCopy;
import org.gooru.nucleus.consumer.sync.jobs.processors.ProcessItemCreate;
import org.gooru.nucleus.consumer.sync.jobs.processors.ProcessItemDelete;
import org.gooru.nucleus.consumer.sync.jobs.processors.ProcessItemMove;
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
}
