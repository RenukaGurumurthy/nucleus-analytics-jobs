package org.gooru.analyics.jobs.executor;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.gooru.analyics.jobs.constants.Constants;
import org.gooru.analyics.jobs.infra.ConnectionProvider;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class StatMetricsPublisher {
	private static final Logger LOG = LoggerFactory.getLogger(StatDataMigration.class);
	private static ConnectionProvider connectionProvider = ConnectionProvider.instance();
	private static final String KAFKA_QUEUE_TOPIC = connectionProvider.getMetricsPublisherQueueTopic();
	private static final int QUEUE_LIMIT = connectionProvider.getConfigsettingsloader().getStatPublisherQueueLimit();
	private static Timer timer = new Timer();
	private static long JOB_DELAY = 0;
	private static long JOB_INTERVAL = connectionProvider.getConfigsettingsloader().getStatPublisherInterval();
	private static KafkaProducer<String, String> producer = connectionProvider.getKafkaProducer().getPublisher();
	
	public StatMetricsPublisher()  {
		LOG.info("deploying StatMetricsPublisher....");
		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				ResultSet queueSet = getPublisherQueue(Constants.PUBLISH_METRICS);
				
				JSONObject statObject = new JSONObject();
				JSONArray jArray = new JSONArray();
				for (Row queue : queueSet) {
					ResultSet resultSet = getStatMetrics(queue.getString(Constants._GOORU_OID), Constants.VIEWS);
					for (Row result : resultSet) {
						JSONObject jObject = new JSONObject();
						jObject.put(Constants.ID, result.getString(Constants._CLUSTERING_KEY));
						jObject.put(Constants.VIEWS_COUNT, result.getLong(Constants._METRICS_VALUE));
						jObject.put(Constants.TYPE, queue.getString(Constants.TYPE));
						jArray.add(jObject);
					}
					deleteFromPublisherQueue(Constants.PUBLISH_METRICS, queue.getString(Constants._GOORU_OID));
				}
				statObject.put(Constants.EVENT_NAME, Constants.VIEWS_UPDATE);
				statObject.put(Constants.DATA, jArray);

				if (!jArray.isEmpty()) {
					LOG.info("message: " + statObject.toString());
					LOG.info("KAFKA_QUEUE_TOPIC: " + KAFKA_QUEUE_TOPIC);
					ProducerRecord<String, String> data = new ProducerRecord<String, String>(KAFKA_QUEUE_TOPIC,
							statObject.toString());
					try {
						LOG.info("going to publish");
						producer.send(data);
						LOG.info("Statistical data publishing completed by " + new Date());
					} catch (Exception e) {
						LOG.error("Exception while publish message", e);
					} 
				}
			}
		};
		timer.scheduleAtFixedRate(task, JOB_DELAY, JOB_INTERVAL);
	}

	private static ResultSet getStatMetrics(String gooruOids, String metricsName) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(connectionProvider.getAnalyticsCassandraName(), Constants.STATISTICAL_DATA)
					.where(QueryBuilder.eq(Constants._CLUSTERING_KEY, gooruOids))
					.and(QueryBuilder.eq(Constants._METRICS_NAME, metricsName))
					.setConsistencyLevel(ConsistencyLevel.QUORUM);
			ResultSetFuture resultSetFuture = (connectionProvider.getAnalyticsCassandraSession()).executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.info("Error while get stat metrics..");
		}
		return result;
	}

	private static ResultSet getPublisherQueue(String metricsName) {
		ResultSet result = null;
		try {
			Statement select = QueryBuilder.select().all()
					.from(connectionProvider.getEventCassandraName(), Constants.STAT_PUBLISHER_QUEUE)
					.where(QueryBuilder.eq(Constants._METRICS_NAME, metricsName)).limit(QUEUE_LIMIT)
					.setConsistencyLevel(ConsistencyLevel.QUORUM);
			ResultSetFuture resultSetFuture = (connectionProvider.getEventCassandraSession()).executeAsync(select);
			result = resultSetFuture.get();
		} catch (Exception e) {
			LOG.info("Error while get stat publisher queue..");
		}
		return result;
	}

	private static void deleteFromPublisherQueue(String metricsName, String gooruOid) {
		try {
			LOG.info("Removing -" + gooruOid + "- from the statistical queue");
			Statement select = QueryBuilder.delete().all()
					.from(connectionProvider.getEventCassandraName(), Constants.STAT_PUBLISHER_QUEUE)
					.where(QueryBuilder.eq(Constants._METRICS_NAME, metricsName))
					.and(QueryBuilder.eq(Constants._GOORU_OID, gooruOid)).setConsistencyLevel(ConsistencyLevel.QUORUM);
			ResultSetFuture resultSetFuture = (connectionProvider.getEventCassandraSession()).executeAsync(select);
			resultSetFuture.get();
		} catch (Exception e) {
			LOG.info("Error while delete stat publisher queue..");
		}
	}
}
