package org.gooru.analyics.jobs.infra;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import org.gooru.analyics.jobs.executor.StatMetricsPublisher;
import org.gooru.analyics.jobs.executor.SyncClassMembers;
import org.gooru.analyics.jobs.executor.SyncContentAuthorizedUsers;
import org.gooru.analyics.jobs.executor.SyncTotalContentCounts;
import org.gooru.analyics.jobs.utils.Args;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeployJobs {
	private static final Logger LOG = LoggerFactory.getLogger(DeployJobs.class);
	
	public static void main(String args[]) {
        JSONObject config = loadConfig(args);
        // FIXME: Why are two new calls made and ignored the result? Are we expecting side effects?
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
                    LOG.warn("Configuration file '{}' does not contain a valid JSON object", sconf, e);
				}
			} catch (FileNotFoundException e) {
				try {
					conf = new JSONObject(confArg);
				} catch (Exception e2) {
                    LOG.warn("-conf option does not point to a file and is not valid JSON: '{}'", confArg);
				}
			}
		}
		return conf;
	}
}
