/**
 * 
 */
package org.gooru.reports.dbhelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.gooru.reports.components.DataSourceRegistry;
import org.gooru.reports.constants.AppConstants;
import org.gooru.reports.entities.Report;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gooru
 *
 */
public final class DBHelper {

	private static final Logger LOGGER = LoggerFactory.getLogger(DBHelper.class);

	private static final String FETCH_BY_WEEK = "SELECT tenant_id, COUNT(DISTINCT(user_id)) AS user_count FROM user_activity WHERE event_name = 'event.user.signin'"
			+ " AND EXTRACT(WEEK FROM updated_at) = ? AND EXTRACT(YEAR FROM updated_at) = ? GROUP BY tenant_id";

	private static final String FETCH_BY_MONTH = "SELECT tenant_id, COUNT(DISTINCT(user_id)) AS user_count FROM user_activity WHERE event_name = 'event.user.signin'"
			+ " AND EXTRACT(MONTH FROM updated_at) = ? AND EXTRACT(YEAR FROM updated_at) = ? GROUP BY tenant_id";
	

	private DBHelper() {
		throw new AssertionError();
	}

	public static Report fetchByWeekAndYear(LocalDate fromDate, LocalDate toDate) {
		WeekFields weekFields = WeekFields.of(Locale.getDefault());
		Report report = new Report();
		Map<String, Map<String, Integer>> userActivitiesByWeek = new HashMap<>();

		// Populate first default column
		List<String> columns = new ArrayList<>();
		columns.add(AppConstants.COLUMN_TENANT_ID);

		Connection conn = null;
		try {
			conn = DataSourceRegistry.getInstance().getDataSource(AppConstants.REPORTS_DATA_SOURCE).getConnection();
			PreparedStatement pstmt = conn.prepareStatement(FETCH_BY_WEEK);

			while (fromDate.isBefore(toDate)) {
				int weekNumber = fromDate.get(weekFields.weekOfWeekBasedYear());
				int year = fromDate.getYear();

				pstmt.setInt(1, weekNumber);
				pstmt.setInt(2, year);

				ResultSet result = pstmt.executeQuery();
				while (result.next()) {
					String tenantId = result.getString("tenant_id");
					int userCount = result.getInt("user_count");

					Map<String, Integer> userCounts = userActivitiesByWeek.containsKey(tenantId)
							? userActivitiesByWeek.get(tenantId)
							: new TreeMap<>();
					userCounts.put(fromDate.toString(), userCount);

					userActivitiesByWeek.put(tenantId, userCounts);
				}

				// increase fromDate by 1 week
				fromDate = fromDate.plus(1, ChronoUnit.WEEKS);
				columns.add(fromDate.toString());
			}

			// Populate last default column
			columns.add(AppConstants.COLUMN_TENANT_NAME);
			report.setColumns(columns);
			report.setData(userActivitiesByWeek);
		} catch (SQLException e) {
			LOGGER.error("error while fetching user activities by week", e);
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				LOGGER.error("error while closing the connection", e);
			}
		}

		return report;
	}

	public static Report fetchByMonthAndYear(LocalDate fromDate, LocalDate toDate) {
		Report report = new Report();
		Map<String, Map<String, Integer>> userActivitiesByMonth = new HashMap<>();
		DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern("MM/uuuu");

		// Populate first default column
		List<String> columns = new ArrayList<>();
		columns.add(AppConstants.COLUMN_TENANT_ID);

		Connection conn = null;
		try {
			conn = DataSourceRegistry.getInstance().getDataSource(AppConstants.REPORTS_DATA_SOURCE).getConnection();
			PreparedStatement pstmt = conn.prepareStatement(FETCH_BY_MONTH);

			while (fromDate.isBefore(toDate)) {
				int monthNumber = fromDate.get(ChronoField.MONTH_OF_YEAR);
				int year = fromDate.getYear();

				pstmt.setInt(1, monthNumber);
				pstmt.setInt(2, year);

				ResultSet result = pstmt.executeQuery();
				while (result.next()) {
					String tenantId = result.getString("tenant_id");
					int userCount = result.getInt("user_count");

					Map<String, Integer> userCounts = userActivitiesByMonth.containsKey(tenantId)
							? userActivitiesByMonth.get(tenantId)
							: new TreeMap<>();
					userCounts.put(dtFormatter.format(fromDate), userCount);

					userActivitiesByMonth.put(tenantId, userCounts);
				}

				columns.add(dtFormatter.format(fromDate));
				
				// increase fromDate by 1 month
				fromDate = fromDate.plus(1, ChronoUnit.MONTHS);
				
			}

			// Populate last default column
			columns.add(AppConstants.COLUMN_TENANT_NAME);
			report.setColumns(columns);
			report.setData(userActivitiesByMonth);
		} catch (SQLException e) {
			LOGGER.error("error while fetching user activities by month", e);
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				LOGGER.error("error while closing the connection", e);
			}
		}

		return report;
	}

	public static Map<String, String> fetchTenantNames(Set<String> tenants) {
		Map<String, String> names = new HashMap<String, String>();
		Connection conn = null;
		try {
			conn = DataSourceRegistry.getInstance().getDataSource(AppConstants.CORE_DATA_SOURCE).getConnection();
			PreparedStatement pstmt = conn.prepareStatement("SELECT id, name FROM tenant where id = ANY(?::uuid[])");
			pstmt.setString(1, listToPostgresArrayString(tenants));

			ResultSet results = pstmt.executeQuery();
			while (results.next()) {
				names.put(results.getString("id"), results.getString("name"));
			}

		} catch (SQLException e) {
			LOGGER.error("error while fetching tenant names from database", e);
		}
		return names;
	}

	private static String listToPostgresArrayString(Set<String> input) {
		int approxSize = ((input.size() + 1) * 36); // Length of UUID is around
													// 36
		// chars
		Iterator<String> it = input.iterator();
		if (!it.hasNext()) {
			return "{}";
		}

		StringBuilder sb = new StringBuilder(approxSize);
		sb.append('{');
		for (;;) {
			String s = it.next();
			sb.append('"').append(s).append('"');
			if (!it.hasNext()) {
				return sb.append('}').toString();
			}
			sb.append(',');
		}
	}
}
