package org.gooru.reports.bootstrap;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class Test {

	public static void main(String[] args) {
		LocalDate startDate = LocalDate.parse("2017-12-01");
		LocalDate endDate = LocalDate.parse("2018-03-31");
				
		List<String> columns = new ArrayList<>();
		columns.add("tenant_id");
		columns.add("tenant_name");
		while (startDate.isBefore(endDate)) {
			columns.add(startDate.toString());
			WeekFields weekFields = WeekFields.of(Locale.getDefault()); 
			int weekNumber = startDate.get(weekFields.weekOfWeekBasedYear());
			
			int year = startDate.getYear();
			System.out.print(startDate);
			System.out.print(" == " + weekNumber);
			System.out.println(" == " + year);
			startDate = startDate.plus(1, ChronoUnit.WEEKS);
		}
		
		System.out.println("=========");
		System.out.println(generateHeaders(columns));
	}
	
	private static String generateHeaders(List<String> columns) {
		StringBuilder sb = new StringBuilder();
		Iterator<String> iterator = columns.iterator();
		
		for(;;) {
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
		
		for (String column : columns) {
			int count = rowData.containsKey(column) ? rowData.get(column) : 0;
			sb.append(count);
			sb.append(',');
			
		}
		
		sb.append(tenantName);
		return sb.toString();
	}

}
