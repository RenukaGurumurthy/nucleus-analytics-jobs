package org.gooru.analytics.jobs.executor;

import org.gooru.analytics.jobs.infra.ArchivedCassandraClusterClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.ConstantBackoff;

public class AstyanaxClientTest {
  private static final ArchivedCassandraClusterClient archivedCassandraClusterClient = ArchivedCassandraClusterClient.instance();
  private static final Logger LOG = LoggerFactory.getLogger(EventMigration.class);

  public static void main(String args[]) {
  ColumnList<String> result = readWithKey("search_setting", "collection.content.boost");
    for (String columnName : result.getColumnNames()) {
      LOG.info(columnName);
    }
  }

  public static ColumnList<String> readWithKey(String cfName, String key) {

    ColumnList<String> result = null;
    try {
      result = (archivedCassandraClusterClient.getCassandraKeyspace()).prepareQuery(archivedCassandraClusterClient.accessColumnFamily(cfName))
              .setConsistencyLevel(ConsistencyLevel.CL_QUORUM).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key).execute().getResult();

    } catch (Exception e) {
      LOG.error("Failure in reading with key", e);
    }

    return result;
  }
}
