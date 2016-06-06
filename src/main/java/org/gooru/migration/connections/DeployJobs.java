package org.gooru.migration.connections;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import org.gooru.migration.jobs.StatDataMigration;
import org.gooru.migration.jobs.StatMetricsPublisher;
import org.gooru.migration.jobs.SyncClassMembers;
import org.gooru.migration.jobs.SyncContentAuthorizedUsers;
import org.gooru.migration.jobs.SyncTotalContentCounts;
import org.gooru.migration.utils.Args;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeployJobs {
	private static final Logger LOG = LoggerFactory.getLogger(DeployJobs.class);
	
	public static void main(String args[]) {
		JSONObject config = loadConfig(args);
		new ConfigSettingsLoader(config);
		new ConnectionProvider();

		if (config.getBoolean("stat.publisher.worker.start")) {
			new StatMetricsPublisher();
		}
		if (config.getBoolean("sync.class.members.worker.start")) {
			new SyncClassMembers();
		}
		if (config.getBoolean("sync.content.authorized.users.worker.start")) {
			new SyncContentAuthorizedUsers();
		}
		if (config.getBoolean("sync.total.content.counts.worker.start")) {
			new SyncTotalContentCounts();
		}
		LOG.info("All jobs started.....");
	}

	private static JSONObject loadConfig(String sargs[]) {
		Args args = new Args(sargs);
		String confArg = args.map.get("-conf");
		JSONObject conf = null;

		if (confArg != null) {
			try (Scanner scanner = new Scanner(new File(confArg)).useDelimiter("\\A")) {
				String sconf = scanner.next();
				try {
					conf = new JSONObject(sconf);
				} catch (Exception e) {
					System.out.println("Configuration file " + sconf + " does not contain a valid JSON object");
				}
			} catch (FileNotFoundException e) {
				try {
					conf = new JSONObject(confArg);
				} catch (Exception e2) {
					System.out.println("-conf option does not point to a file and is not valid JSON: " + confArg);
				}
			}
		}
		return conf;
	}
}
