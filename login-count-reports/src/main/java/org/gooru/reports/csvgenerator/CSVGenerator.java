package org.gooru.reports.csvgenerator;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gooru.reports.dbhelper.DBHelper;
import org.gooru.reports.entities.Report;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CSVGenerator {

	private static final Logger LOGGER = LoggerFactory.getLogger(CSVGenerator.class);

	public void generate(Report report) {
		String fileName = "./" + System.currentTimeMillis() + ".csv";
		try {
			BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileName));
			List<String> columns = report.getColumns();
			writer.write(generateHeaders(columns));

			Map<String, Map<String, Integer>> data = report.getData();
			Map<String, String> tenantNames = DBHelper.fetchTenantNames(data.keySet());

			data.forEach((tenantId, userCounts) -> {
				try {
					writer.newLine();
					writer.write(generateRow(tenantId, tenantNames.get(tenantId), userCounts, columns));
				} catch (IOException e) {
					LOGGER.warn("error while writing row for tenant:{}", tenantId);
				}
			});

			writer.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private String generateHeaders(List<String> columns) {
		StringBuilder sb = new StringBuilder();
		Iterator<String> iterator = columns.iterator();

		for (;;) {
			String column = iterator.next();
			sb.append(column);
			if (!iterator.hasNext()) {
				return sb.toString();
			}
			sb.append(',');
		}
	}

	private String generateRow(String tenantId, String tenantName, Map<String, Integer> rowData, List<String> columns) {
		StringBuilder sb = new StringBuilder();
		sb.append(tenantId);
		sb.append(',');

		int columnCount = 1;
		for (String column : columns) {
			if (columnCount != 1 && columnCount != columns.size()) {
				int count = rowData.containsKey(column) ? rowData.get(column) : 0;
				sb.append(count);
				sb.append(',');
				
			}
			columnCount++;
		}

		sb.append(tenantName);
		return sb.toString();
	}

}
