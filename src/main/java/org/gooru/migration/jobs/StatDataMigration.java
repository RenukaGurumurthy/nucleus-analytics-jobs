package org.gooru.migration.jobs;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.gooru.migration.connections.ConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.retry.ConstantBackoff;

public class StatDataMigration {
	private static final Logger LOG = LoggerFactory.getLogger(StatDataMigration.class);
	private static ConnectionProvider connectionProvider = ConnectionProvider.instance();
	private static final int QUEUE_LIMIT = connectionProvider.getConfigsettingsloader().getStatMigrationQueueLimit();
	private static final String STAT_PUBLISHER_QUEUE = "stat_publisher_queue";
	private static final String METRICS = "metrics";
	private static final String VIEWS = "views";
	private static final String _METRICS_NAME = "metrics_name";
	private static final String _GOORU_OID = "gooru_oid";
	private static final String MIGRATE_METRICS = "migrateMetrics";
	private static final String COUNT_VIEWS = "count~views";
	private static final String TIME_SPENT_TOTAL = "time_spent~total";
	private static final String COUNT_COPY = "count~copy";
	private static final String TOTAL_TIMESPENT_IN_MS = "totalTimeSpentInMs";
	private static final String COPY = "copy";
	private static final String COUNT_RESOURCE_ADDED_PUBLIC = "count~resourceAddedPublic";
	private static final String ID = "id";
	private static final String VIEWS_COUNT = "viewsCount";
	private static final String COLLECTION_REMIX_COUNT = "collectionRemixCount";
	private static final String USED_IN_COLLECTION_COUNT = "usedInCollectionCount";
	private static final String COLLABORATOR_COUNT = "collaboratorCount";
	private static final String indexName = "gooru_local_statistics_v2";
	private static final String typeName = "statistics";
	
	
	private static final Timer timer = new Timer();
	private static final long JOB_DELAY = 0;
	private static final long JOB_INTERVAL = connectionProvider.getConfigsettingsloader().getStatMigrationInterval();
	private static XContentBuilder contentBuilder = null;

	private static PreparedStatement UPDATE_STATISTICAL_COUNTER_DATA = connectionProvider.getAnalyticsCassandraSession()
			.prepare(
					"UPDATE statistical_data SET metrics_value = metrics_value+? WHERE clustering_key = ? AND metrics_name = ?");

	private static PreparedStatement SELECT_STATISTICAL_COUNTER_DATA = connectionProvider.getAnalyticsCassandraSession()
			.prepare(
					"SELECT metrics_value AS metrics FROM statistical_data WHERE clustering_key = ? AND metrics_name = ?");

	public static void main(String args[]) throws InterruptedException {
		LOG.info("Please make sure that we have loaded list of content oids in stat_publisher_queue column family.");
		LOG.info("Press Ctrl+C if you want to kill process from job executed location.");
		Thread.sleep(10000);

		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				try {
					ResultSet queueSet = getPublisherQueue(MIGRATE_METRICS);
					for (Row queue : queueSet) {
						String gooruOid = queue.getString(_GOORU_OID);
						ColumnList<String> statMetricsColumns = getStatMetrics(gooruOid);
						if (statMetricsColumns != null) {
							contentBuilder = jsonBuilder().startObject();
							LOG.info("migrating id : " + gooruOid);
							for (Column<String> statMetrics : statMetricsColumns) {
								long viewCount = 0L;
								long remixCount = 0L;
								long usedInCollectionCount = 0L;
								switch (statMetrics.getName()) {
								case COUNT_VIEWS:
									viewCount = statMetrics.getLongValue();
									//updateStatisticalCounterData(gooruOid,VIEWS, viewCount);
									balanceCounterData(gooruOid, VIEWS, viewCount);
									break;
								case TIME_SPENT_TOTAL:
									//updateStatisticalCounterData(gooruOid,TOTAL_TIMESPENT_IN_MS,statMetrics.getLongValue());
									balanceCounterData(gooruOid, TOTAL_TIMESPENT_IN_MS, statMetrics.getLongValue());
									break;
								case COUNT_COPY:
									remixCount = statMetrics.getLongValue();
									//updateStatisticalCounterData(gooruOid,COPY, remixCount);
									balanceCounterData(gooruOid, COPY, remixCount);
									break;
								case COUNT_RESOURCE_ADDED_PUBLIC:
									usedInCollectionCount = statMetrics.getLongValue();
									//updateStatisticalCounterData(gooruOid,COPY, remixCount);
									balanceCounterData(gooruOid, USED_IN_COLLECTION_COUNT, usedInCollectionCount);
									break;
								default:
									LOG.info("Unused metric: " + statMetrics.getName());
								}
								/**
								 * Generate content builder to write in search
								 * index.
								 */
								contentBuilder.field(ID, gooruOid);
								contentBuilder.field(VIEWS_COUNT, viewCount);
								contentBuilder.field(COLLECTION_REMIX_COUNT, remixCount);
								contentBuilder.field(USED_IN_COLLECTION_COUNT, usedInCollectionCount);
								contentBuilder.field(COLLABORATOR_COUNT, 0);
							}
							indexingES(indexName, typeName, gooruOid, contentBuilder);
						}
						deleteFromPublisherQueue(MIGRATE_METRICS, gooruOid);
					}
				} catch (IOException e) {
					LOG.error("Error while migrating data : {}", e);
				}
				LOG.info("Job running at {}", new Date());
			}
		};
		timer.scheduleAtFixedRate(task, JOB_DELAY, JOB_INTERVAL);
	}

	private static ResultSet getPublisherQueue(String metricsName) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(connectionProvider.getEventCassandraName(), STAT_PUBLISHER_QUEUE)
					.where(QueryBuilder.eq(_METRICS_NAME, metricsName)).limit(QUEUE_LIMIT)
					.setConsistencyLevel(ConsistencyLevel.QUORUM);
			ResultSetFuture resultSetFuture = (connectionProvider.getEventCassandraSession()).executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Error while reading publisher data into queue..", e);
		}
		return result;
	}

	private static void deleteFromPublisherQueue(String metricsName, String gooruOid) {
		try {
			LOG.info("Removing -" + gooruOid + "- from the statistical queue");
			Statement select = QueryBuilder.delete().all()
					.from(connectionProvider.getEventCassandraName(), STAT_PUBLISHER_QUEUE)
					.where(QueryBuilder.eq(_METRICS_NAME, metricsName)).and(QueryBuilder.eq(_GOORU_OID, gooruOid))
					.setConsistencyLevel(ConsistencyLevel.QUORUM);
			ResultSetFuture resultSetFuture = (connectionProvider.getEventCassandraSession()).executeAsync(select);
			resultSetFuture.get();
		} catch (Exception e) {
			LOG.error("Error while reading publisher data into queue..", e);
		}
	}

	private static ColumnList<String> getStatMetrics(String gooruOid) {
		ColumnList<String> result = null;
		try {
			result = connectionProvider.getCassandraKeyspace()
					.prepareQuery(connectionProvider.accessColumnFamily("live_dashboard"))
					.setConsistencyLevel(com.netflix.astyanax.model.ConsistencyLevel.CL_QUORUM)
					.withRetryPolicy(new ConstantBackoff(2000, 5)).getKey("all~" + gooruOid).execute().getResult();

		} catch (Exception e) {
			LOG.error("Error while retrieve stat metrics..", e);
		}
		return result;
	}

	private static boolean updateStatisticalCounterData(String clusteringKey, String metricsName, Object metricsValue) {
		try {
			BoundStatement boundStatement = new BoundStatement(UPDATE_STATISTICAL_COUNTER_DATA);
			boundStatement.bind(metricsValue, clusteringKey, metricsName);
			connectionProvider.getAnalyticsCassandraSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while update stat metrics..", e);
			return false;
		}
		return true;
	}

	private static boolean balanceCounterData(String clusteringKey, String metricsName, Long metricsValue) {
		try {
			BoundStatement selectBoundStatement = new BoundStatement(SELECT_STATISTICAL_COUNTER_DATA);
			selectBoundStatement.bind(clusteringKey, metricsName);
			ResultSetFuture resultFuture = connectionProvider.getAnalyticsCassandraSession()
					.executeAsync(selectBoundStatement);
			ResultSet result = resultFuture.get();

			long existingValue = 0;
			if (result != null) {
				for (Row resultRow : result) {
					existingValue = resultRow.getLong(METRICS);
				}
			}
			long balancedMatrics = ((Number) metricsValue).longValue() - existingValue;
			
			BoundStatement boundStatement = new BoundStatement(UPDATE_STATISTICAL_COUNTER_DATA);
			boundStatement.bind(balancedMatrics, clusteringKey, metricsName);
			connectionProvider.getAnalyticsCassandraSession().executeAsync(boundStatement);
		} catch (Exception e) {
			LOG.error("Error while balance stat metrics..", e);
			return false;
		}
		return true;
	}

	private static void indexingES(String indexName, String indexType, String id, XContentBuilder contentBuilder) {
		connectionProvider.getElsClient().prepareIndex(indexName, indexType, id).setSource(contentBuilder).execute()
				.actionGet();
	}
}
