package org.gooru.migration;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.retry.ConstantBackoff;

public class EventMigration {
	private static SimpleDateFormat minuteDateFormatter = new SimpleDateFormat("yyyyMMddkkmm");
	private static ConnectionProvider cassandraConnectionProvider = ConnectionProvider.instance();
	private static String EVENT_TIMIELINE = "event_timelinge";
	private static String EVENT_DETAIL = "event_detail";
	private static PreparedStatement insertEvents = (cassandraConnectionProvider.getAnalyticsCassandraSession())
			.prepare("INSERT INTO events(event_id,fields)VALUES(?,?)");
	private static PreparedStatement insertEventTimeLine = (cassandraConnectionProvider.getAnalyticsCassandraSession())
			.prepare("INSERT INTO events_timeline(event_time,event_id)VALUES(?,?);");

	public static void main(String args[]) {

		try {
			String start = args[0];
			String end = args[1];

			Long startTime = minuteDateFormatter.parse(start).getTime();
			System.out.println("startTime : " + start);
			Long endTime = minuteDateFormatter.parse(end).getTime();
			System.out.println("endTime : " + end);
			// String start = "201508251405";
			// Long endTime = new Date().getTime();

			for (Long startDate = startTime; startDate < endTime;) {
				String currentDate = minuteDateFormatter.format(new Date(startDate));
				System.out.println("Running for :" + currentDate);
				// Incrementing time - one minute
				ColumnList<String> et = readWithKey(EVENT_TIMIELINE, currentDate);
				for (String eventId : et.getColumnNames()) {
					ColumnList<String> ef = readWithKey(EVENT_DETAIL, et.getStringValue(eventId, "NA"));
					// Insert event_time_line
					insertData(currentDate, et.getStringValue(eventId, "NA"), insertEventTimeLine);
					// Insert events
					insertData(et.getStringValue(eventId, "NA"), ef.getStringValue("fields", "NA"), insertEvents);
				}
				startDate = new Date(startDate).getTime() + 60000;
				Thread.sleep(200);
			}
		} catch (Exception e) {
			if (e instanceof ArrayIndexOutOfBoundsException) {
				System.out.println("startTime or endTime can not be null. Please make sure the class execution format as below.");
				System.out.println(
						"java -classpath migration-scripts-fat.jar org.gooru.migration.EventMigration 201508251405 201508251410");
				System.out.println();
			} else {
				System.out.println("Something went wrong.");
				e.printStackTrace();
			}
			System.exit(500);
		}
	}

	public static ColumnList<String> readWithKey(String cfName, String key) {

		ColumnList<String> result = null;
		try {
			result = (cassandraConnectionProvider.getCassandraKeyspace())
					.prepareQuery(cassandraConnectionProvider.accessColumnFamily(cfName))
					.setConsistencyLevel(ConsistencyLevel.CL_QUORUM).withRetryPolicy(new ConstantBackoff(2000, 5))
					.getKey(key).execute().getResult();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return result;
	}

	public static void insertData(String key, String column, PreparedStatement preparedStatement) {
		try {
			BoundStatement boundStatement = new BoundStatement(preparedStatement);
			boundStatement.bind(key, column);
			(cassandraConnectionProvider.getAnalyticsCassandraSession()).executeAsync(boundStatement);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
