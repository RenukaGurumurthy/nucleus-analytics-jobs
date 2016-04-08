package org.gooru.migration.jobs;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.gooru.migration.connections.ConnectionProvider;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import kafka.producer.KeyedMessage;

public class StatMetricsPublisher {
	private static ConnectionProvider connectionProvider = ConnectionProvider.instance();
	private static final String STATISTICAL_DATA = "statistical_data";
	private static final String STAT_PUBLISHER_QUEUE = "stat_publisher_queue";
	private static final String KAFKA_QUEUE_TOPIC = connectionProvider.getMetricsPublisherQueueTopic();
	private static final String _CLUSTERING_KEY = "clustering_key";
	private static final String _METRICS_NAME = "metrics_name";
	private static final String _METRICS_VALUE = "metrics_value";
	private static final String GOORU_OID = "gooruOid";
	private static final String _GOORU_OID = "gooru_oid";
	private static final String PUBLISH_METRICS = "publishMetrics";
	private static final int QUEUE_LIMIT = 10;

	private static Timer timer = new Timer();
	private static long JOB_DELAY = 0;
	private static long JOB_INTERVAL = 60000; // 1 Mintue

	public static void main(String arg[]) {
		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				ResultSet queueSet = getPublisherQueue(PUBLISH_METRICS);
				JSONArray jArray = new JSONArray();
				for (Row queue : queueSet) {
					ResultSet resultSet = getStatMetrics(queue.getString(_GOORU_OID));
					for (Row result : resultSet) {
						JSONObject jObject = new JSONObject();
						jObject.put(GOORU_OID, result.getString(_CLUSTERING_KEY));
						jObject.put(result.getString(_METRICS_NAME), result.getLong(_METRICS_VALUE));
						jArray.add(jObject);
					}
					 deleteFromPublisherQueue(PUBLISH_METRICS,queue.getString(_GOORU_OID));
				}
				KeyedMessage<String, String> data = new KeyedMessage<String, String>(KAFKA_QUEUE_TOPIC,
						jArray.toString());
				(connectionProvider.getKafkaProducer().getPublisher()).send(data);
				System.out.println("Statistical data publishing completed by " + new Date());
			}
		};
		timer.scheduleAtFixedRate(task, JOB_DELAY, JOB_INTERVAL);
	}

	private static ResultSet getStatMetrics(String gooruOids) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(connectionProvider.getAnalyticsCassandraName(), STATISTICAL_DATA)
					.where(QueryBuilder.eq(_CLUSTERING_KEY, gooruOids)).setConsistencyLevel(ConsistencyLevel.QUORUM);
			ResultSetFuture resultSetFuture = (connectionProvider.getAnalyticsCassandraSession())
					.executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
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
}
