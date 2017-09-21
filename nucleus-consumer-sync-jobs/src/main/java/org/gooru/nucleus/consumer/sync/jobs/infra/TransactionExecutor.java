package org.gooru.nucleus.consumer.sync.jobs.infra;

import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;

import org.javalite.activejdbc.Base;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TransactionExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionExecutor.class);

  private TransactionExecutor() {
    throw new AssertionError();
  }

  public static Object executeWithAnalyticsDBTransaction(DBHandler handler) {
    Object result = null;
    try {
      Base.open(DataSourceRegistry.getInstance().getAnalyticsDataSource());
      Base.openTransaction();
      result = handler.execute();
      Base.commitTransaction();
    } catch (Throwable e) {
      Base.rollbackTransaction();
      LOGGER.error("Caught exception, need to rollback and abort", e);
    } finally {
      Base.close();
    }
    return result;
  }

  public static Object executeWithCoreDBTransaction(DBHandler handler) {
    Object result = null;
    try {
      Base.open(DataSourceRegistry.getInstance().getCoreDataSource());
      Base.openTransaction();
      result = handler.execute();
      Base.commitTransaction();
    } catch (Throwable e) {
      Base.rollbackTransaction();
      LOGGER.error("Caught exception, need to rollback and abort", e);
    } finally {
      Base.close();
    }
    return result;
  }

  public static void executeWithCoreDBCopyMgr(String sqlStmt, String dumpLocation, String dumpFileName) {
    Connection conn = null;
    FileWriter fw = null;
    try {
      conn = DataSourceRegistry.getInstance().getCoreDataSource().getConnection();
      final CopyManager copyManager = new CopyManager(conn.unwrap(BaseConnection.class));
      fw = new FileWriter(dumpLocation + "/" + dumpFileName);
      StringBuilder exportQuery = new StringBuilder();
      exportQuery.append("COPY(").append(sqlStmt).append(") TO STDOUT WITH DELIMITER ','");
      LOGGER.debug("exportQuery :{}", exportQuery.toString());
      copyManager.copyOut(exportQuery.toString(), fw);
    } catch (Exception e) {
      LOGGER.error("Error while taking dump in CORE DB!. Please check", e);
    } finally {
      try {
        fw.close();
        conn.close();
      } catch (Exception e) {
        LOGGER.error("Error while closing connection!!", e);
      }
    }

  }

  public static void executeWithCoreDBCopyMgr(String tableName, String attributes, String dumpLocation, String dumpFileName) {
    Connection conn = null;
    FileWriter fw = null;
    try {
      conn = DataSourceRegistry.getInstance().getCoreDataSource().getConnection();
      final CopyManager copyManager = new CopyManager(conn.unwrap(BaseConnection.class));
      fw = new FileWriter(dumpLocation + "/" + dumpFileName);
      StringBuilder exportQuery = new StringBuilder();
      exportQuery.append("COPY ").append(tableName).append("(").append(attributes).append(")").append(" TO STDOUT WITH DELIMITER ','");
      LOGGER.debug("exportQuery :{}", exportQuery.toString());
      copyManager.copyOut(exportQuery.toString(), fw);
    } catch (Exception e) {
      LOGGER.error("Error while taking dump in CORE DB!. Please check! ", e);
    } finally {
      try {
        fw.close();
        conn.close();
      } catch (Exception e) {
        LOGGER.error("Error while closing connection!!", e);
      }
    }

  }

  public static void executeWithAnalyticsDBCopyMgr(String tableName, String attributes, String dumpLocation, String dumpFileName) {
    Connection conn = null;
    FileReader fr = null;
    try {
      conn = DataSourceRegistry.getInstance().getAnalyticsDataSource().getConnection();
      final CopyManager copyManager = new CopyManager(conn.unwrap(BaseConnection.class));
      fr = new FileReader(dumpLocation + "/" + dumpFileName);
      StringBuilder importQuery = new StringBuilder();
      if(attributes != null) {        
        importQuery.append("COPY ").append(tableName).append(" (").append(attributes).append(") ").append(" FROM STDIN WITH DELIMITER ','");
      }else {
        importQuery.append("COPY ").append(tableName).append(" FROM STDIN WITH DELIMITER ','");
      }
      LOGGER.debug("importQuery : {} ", importQuery.toString());
      copyManager.copyIn(importQuery.toString(), fr);
    } catch (Exception e) {
      LOGGER.error("Error while importing dump in Analytics DB!. Please check {} ", e);
    } finally {
      try {
        fr.close();
        conn.close();
      } catch (Exception e) {
        LOGGER.error("Error while closing connection {} ", e);
      }
    }

  }
}
