package org.gooru.reports.entities;

import java.util.List;
import java.util.Map;

public class Report {
	
	private Map<String, Map<String, Integer>> data;

	private List<String> columns;

	public Map<String, Map<String, Integer>> getData() {
		return data;
	}

	public void setData(Map<String, Map<String, Integer>> data) {
		this.data = data;
	}

	public List<String> getColumns() {
		return columns;
	}

	public void setColumns(List<String> columns) {
		this.columns = columns;
	}
}
