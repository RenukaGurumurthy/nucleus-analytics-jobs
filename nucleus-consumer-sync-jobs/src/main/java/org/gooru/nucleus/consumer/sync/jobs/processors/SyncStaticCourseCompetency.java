package org.gooru.nucleus.consumer.sync.jobs.processors;

import java.util.TimerTask;

import org.gooru.nucleus.consumer.sync.jobs.constants.AttributeConstants;
import org.gooru.nucleus.consumer.sync.jobs.infra.DBHandler;
import org.gooru.nucleus.consumer.sync.jobs.infra.TransactionExecutor;
import org.javalite.activejdbc.Base;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncStaticCourseCompetency extends TimerTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(SyncStaticCourseCompetency.class);
  private JSONObject config;

  private String dumpLocation = null;
  private String dumpFileName = null;

  private SyncStaticCourseCompetency() {
  }

  public SyncStaticCourseCompetency(JSONObject config) {
    this.config = config;
  }

  @Override
  public void run() {

    LOGGER.debug("SyncStaticCompetencyCount is running..");

    if (this.config.isNull(AttributeConstants.EXPORT_QUERY)) {
      LOGGER.error("Query not found!!");
    } else {
      String exportQuery = config.getString(AttributeConstants.EXPORT_QUERY);

      if (this.config.isNull(AttributeConstants.DUMP_LOCATION)) {
        this.dumpLocation = "/tmp";
      } else {
        this.dumpLocation = this.config.getString(AttributeConstants.DUMP_LOCATION);
      }

      if (this.config.isNull(AttributeConstants.DUMP_FILE_NAME)) {
        this.dumpFileName = "dump.csv";
      } else {
        this.dumpFileName = this.config.getString(AttributeConstants.DUMP_FILE_NAME);
      }

      TransactionExecutor.executeWithCoreDBCopyMgr(exportQuery.toString(), dumpLocation, dumpFileName);
      if (!this.config.isNull(AttributeConstants.TRUNCATE_TABLE)) {
        TransactionExecutor.executeWithAnalyticsDBTransaction(new DBHandler() {
          @Override
          public Object execute() {
            try {
              Base.exec("TRUNCATE " + config.getString(AttributeConstants.TRUNCATE_TABLE));
              LOGGER.info("Table truncated..");
            } catch (Exception e) {
              LOGGER.error("Error while truncating table", e);
            }
            return null;
          }
        });
      }

      if (!this.config.isNull(AttributeConstants.IMPORT_TABLE)) {
        String attributes = null;
        if (!this.config.isNull(AttributeConstants.ATTRIBUTES)) {
          attributes = this.config.getString(AttributeConstants.ATTRIBUTES);
        }
        TransactionExecutor.executeWithAnalyticsDBCopyMgr(this.config.getString(AttributeConstants.IMPORT_TABLE), attributes, dumpLocation,
                dumpFileName);
        LOGGER.info("SyncStaticCompetencyCount Imported..");
      }

    }

  }

}
