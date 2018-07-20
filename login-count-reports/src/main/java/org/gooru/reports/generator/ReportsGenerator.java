/**
 * 
 */
package org.gooru.reports.generator;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Set;

import org.gooru.reports.constants.AppConstants;
import org.gooru.reports.csvgenerator.CSVGenerator;
import org.gooru.reports.dbhelper.DBHelper;
import org.gooru.reports.entities.Report;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gooru
 *
 */
public class ReportsGenerator {

	private static final Logger LOGGER = LoggerFactory.getLogger(ReportsGenerator.class);

	private final String frequency;
	private final String startDate;
	private final String endDate;

	public ReportsGenerator(String frequency, String startDate, String endDate) {
		this.frequency = frequency;
		this.startDate = startDate;
		this.endDate = endDate;
	}

	public void generate() {
		LOGGER.debug("report generation started");
		Report report = null;
		switch (this.frequency) {
		case AppConstants.FREQUENCY_WEEKLY:
			report = generateWeeklyReport();
			break;
		case AppConstants.FREQUENCY_MONTHLY:
			report = generateMonthlyReport();
			break;

		default:
			throw new IllegalArgumentException("incorrect frequency passed");
		}

		if (report != null) {
			print(report);
		}

		new CSVGenerator().generate(report);

		LOGGER.debug("report generated successfully");
	}

	private void print(Report report) {

		Map<String, Map<String, Integer>> data = report.getData();

		Set<String> keys = data.keySet();
		for (String key : keys) {
			LOGGER.info("Tenant:{}", key);

			Map<String, Integer> userCounts = data.get(key);

			userCounts.forEach((k, v) -> {
				LOGGER.info("{}:{}", k, v);
			});
		}
	}

	public Report generateWeeklyReport() {
		LOGGER.debug("generating weekly report");
		try {
			LocalDate fromDate = LocalDate.parse(this.startDate);
			LocalDate toDate = LocalDate.parse(this.endDate);

			if (fromDate.isAfter(toDate)) {
				throw new IllegalArgumentException("start date should not be later than end date");
			}

			return DBHelper.fetchByWeekAndYear(fromDate, toDate);

		} catch (DateTimeParseException e) {
			throw new IllegalArgumentException(e);
		}
	}

	public Report generateMonthlyReport() {
		LOGGER.debug("generating monthly report");
		try {
			LocalDate fromDate = LocalDate.parse(this.startDate);
			LocalDate toDate = LocalDate.parse(this.endDate);

			if (fromDate.isAfter(toDate)) {
				throw new IllegalArgumentException("start date should not be later than end date");
			}

			return DBHelper.fetchByMonthAndYear(fromDate, toDate);
		} catch (DateTimeParseException e) {
			throw new IllegalArgumentException(e);
		}
	}
}
