package org.gooru.migration;

import java.util.Timer;
import java.util.TimerTask;

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
	private static ConnectionProvider connectionProvider = ConnectionProvider.instance();
	private static final String STAT_PUBLISHER_QUEUE = "stat_publisher_queue";
	private static final String METRICS = "metrics";
	private static final String VIEWS = "views";
	private static final String _METRICS_NAME = "metrics_name";
	private static final String _GOORU_OID = "gooru_oid";
	private static final String MIGRATE_METRICS = "migrateMetrics";
	private static final int QUEUE_LIMIT = 10;
	private static final String COUNT_VIEWS = "count~views";
	private static final String TIME_SPENT_TOTAL = "time_spent~total";
	private static final String COUNT_COPY = "count~copy";
	private static final String TOTAL_TIMESPENT_IN_MS = "totalTimeSpentInMs";
	private static final String COPY = "copy";

	private static final Timer timer = new Timer();
	private static final long JOB_DELAY = 0;
	private static final long JOB_INTERVAL = 5000; // 1 Mintue

	private static PreparedStatement UPDATE_STATISTICAL_COUNTER_DATA = connectionProvider
			.getAnalyticsCassandraSession().prepare(
					"UPDATE statistical_data SET metrics_value = metrics_value+? WHERE clustering_key = ? AND metrics_name = ?");

	private static PreparedStatement SELECT_STATISTICAL_COUNTER_DATA = connectionProvider
			.getAnalyticsCassandraSession().prepare(
					"SELECT metrics_value AS metrics FROM statistical_data WHERE clustering_key = ? AND metrics_name = ?");

	public static void main(String args[]) throws InterruptedException {
		System.out.println("Please make sure that we have loaded list of content oids in stat_publisher_queue column family.");
		System.out.println("Press Ctrl+C if you want to kill process from job executed location.");
		System.out.println();
		Thread.sleep(10000);
		
		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				ResultSet queueSet = getPublisherQueue(MIGRATE_METRICS);
				for (Row queue : queueSet) {
					ColumnList<String> statMetricsColumns = getStatMetrics(queue.getString(_GOORU_OID));
					for (Column<String> statMetrics : statMetricsColumns) {
						switch (statMetrics.getName()) {
						case COUNT_VIEWS:
							updateStatisticalCounterData(queue.getString(_GOORU_OID), VIEWS,
									statMetrics.getLongValue());
							break;
						case TIME_SPENT_TOTAL:
							updateStatisticalCounterData(queue.getString(_GOORU_OID), TOTAL_TIMESPENT_IN_MS,
									statMetrics.getLongValue());
							break;
						case COUNT_COPY:
							updateStatisticalCounterData(queue.getString(_GOORU_OID), COPY, statMetrics.getLongValue());
							break;
						default:
							System.out.println("Unused metric: " + statMetrics.getName());
						}
					}
					 deleteFromPublisherQueue(MIGRATE_METRICS,queue.getString(_GOORU_OID));
				}
			}
		};
		timer.scheduleAtFixedRate(task, JOB_DELAY, JOB_INTERVAL);
	}

	private static ResultSet getPublisherQueue(String metricsName) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(connectionProvider.getAnalyticsCassandraName(), STAT_PUBLISHER_QUEUE)
					.where(QueryBuilder.eq(_METRICS_NAME, metricsName)).limit(QUEUE_LIMIT)
					.setConsistencyLevel(ConsistencyLevel.QUORUM);
			ResultSetFuture resultSetFuture = (connectionProvider.getAnalyticsCassandraSession())
					.executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	private static void deleteFromPublisherQueue(String metricsName, String gooruOid) {
		try {
			System.out.println("Removing -" + gooruOid + "- from the statistical queue");
			Statement select = QueryBuilder.delete().all()
					.from(connectionProvider.getAnalyticsCassandraName(), STAT_PUBLISHER_QUEUE)
					.where(QueryBuilder.eq(_METRICS_NAME, metricsName)).and(QueryBuilder.eq(_GOORU_OID, gooruOid))
					.setConsistencyLevel(ConsistencyLevel.QUORUM);
			ResultSetFuture resultSetFuture = (connectionProvider.getAnalyticsCassandraSession())
					.executeAsync(select);
			resultSetFuture.get();
		} catch (Exception e) {
			e.printStackTrace();
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
			e.printStackTrace();
		}
		return result;
	}

	private static boolean updateStatisticalCounterData(String clusteringKey, String metricsName, Object metricsValue) {
		try {
			BoundStatement boundStatement = new BoundStatement(UPDATE_STATISTICAL_COUNTER_DATA);
			boundStatement.bind(metricsValue, clusteringKey, metricsName);
			connectionProvider.getAnalyticsCassandraSession().executeAsync(boundStatement);
		} catch (Exception e) {
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

			BoundStatement boundStatement = new BoundStatement(UPDATE_STATISTICAL_COUNTER_DATA);
			boundStatement.bind((result.one().getLong(METRICS) - metricsValue), clusteringKey, metricsName);
			connectionProvider.getAnalyticsCassandraSession().executeAsync(boundStatement);
		} catch (Exception e) {
			return false;
		}
		return true;
	}
}
