package org.gooru.nucleus.consumer.sync.jobs.infra;

import org.javalite.activejdbc.Base;
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
      Base.open(DataSourceRegistry.getInstance().getAnalyticsDataSource());
      Base.openTransaction();
     result =  handler.execute();
      Base.commitTransaction();
    } catch (Throwable e) {
      Base.rollbackTransaction();
      LOGGER.error("Caught exception, need to rollback and abort", e);
    } finally {
      Base.close();
    }
    return result;
  }
}
