/**
 * 
 */
package org.gooru.reports.constants;

import java.util.Arrays;
import java.util.List;

/**
 * @author gooru
 *
 */
public final class AppConstants {

	private AppConstants() {
		throw new AssertionError();
	}

	public static final String CORE_DATA_SOURCE = "core_db";
	public static final String REPORTS_DATA_SOURCE = "reports_db";
	
	public static final String COLUMN_TENANT_ID = "tenant_id";
	public static final String COLUMN_TENANT_NAME = "tenant_name";

	public static final String FREQUENCY_YEARLY = "Y";
	public static final String FREQUENCY_QUARTELY = "Q";
	public static final String FREQUENCY_MONTHLY = "M";
	public static final String FREQUENCY_WEEKLY = "W";
	public static final String FREQUENCY_DAILY = "D";

	public static final List<String> VALID_FREQUENCIES = Arrays.asList(FREQUENCY_YEARLY, FREQUENCY_QUARTELY,
			FREQUENCY_MONTHLY, FREQUENCY_WEEKLY, FREQUENCY_DAILY);
	
	
}
