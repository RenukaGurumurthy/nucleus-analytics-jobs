package org.gooru.analytics.jobs.infra;

import org.gooru.analytics.jobs.infra.shutdown.Finalizer;
import org.gooru.analytics.jobs.infra.shutdown.Finalizers;
import org.gooru.analytics.jobs.infra.startup.Initializer;
import org.gooru.analytics.jobs.infra.startup.Initializers;
import org.gooru.analytics.jobs.infra.startup.JobInitializer;
import org.gooru.analytics.jobs.infra.startup.JobInitializers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;

public class DeployJobs extends AbstractVerticle {
	
	private static final Logger LOG = LoggerFactory.getLogger(DeployJobs.class);
	
	@Override
	public void start(Future<Void> startFuture) throws Exception {
		LOG.info("Calling DeployJobs....");

        Future<Void> startApplicationFuture = Future.future();
        Future<Void> startJobFuture = Future.future();
        
        startApplication(startApplicationFuture);
        startJob(startJobFuture);
        
        CompositeFuture.all(startApplicationFuture,startJobFuture).setHandler(result -> {
            if (result.succeeded()) {
                LOG.info("All jobs deployed and application started successfully");
                startFuture.complete();
            } else {
                LOG.error("Deployment or app startup failure", result.cause());
                startFuture.fail(result.cause());

                // Not much options now, no point in continuing
                Runtime.getRuntime().halt(1);
            }
        });
    
	}
	
	@Override
	public void stop(Future<Void> stopFuture) throws Exception {
		LOG.info("Ending DeployJobs....");
		shutdownApplication(stopFuture);
	}
	/*public static void main(String args[]) {
		JSONObject config = loadConfig(args);
		LOG.info("config :" + config);
		if (config != null) {
			new ConfigSettingsLoader(config);
		}
		if (ConfigSettingsLoader.getConfigLoaderStatus()) {
			ConnectionProvider.instance();
		}
		if (ConnectionProvider.getConnectionProviderStatus()) {
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
	}*/
	
	private void startJob(Future<Void> startJobFuture) {
        vertx.executeBlocking(future -> {
            JobInitializers jobInitializers = new JobInitializers();
            try {
                for (JobInitializer jobInitializer : jobInitializers) {
                	LOG.debug("Job name : " + jobInitializer.getClass().getName());
                	jobInitializer.deployJob(config());
                }
                future.complete();
            } catch (IllegalStateException ie) {
                LOG.error("Error starting jobs", ie);
                future.fail(ie);
            }
        }, result -> {
            if (result.succeeded()) {
                LOG.info("All jobs started successfully");
                startJobFuture.complete();
            } else {
                LOG.warn("Jobs startup failure", result.cause());
                startJobFuture.fail(result.cause());
            }
        });
    }
    private void startApplication(Future<Void> startApplicationFuture) {
        vertx.executeBlocking(future -> {
            Initializers initializers = new Initializers();
            try {
                for (Initializer initializer : initializers) {
                    initializer.initializeComponent(config());
                }
                future.complete();
            } catch (IllegalStateException ie) {
                LOG.error("Error initializing application", ie);
                future.fail(ie);
            }
        }, result -> {
            if (result.succeeded()) {
                LOG.info("All collection started successfully");
                startApplicationFuture.complete();
            } else {
                LOG.warn("Connections startup failure", result.cause());
                startApplicationFuture.fail(result.cause());
            }
        });
    }

    private void shutdownApplication(Future<Void> shutdownApplicationFuture) {
        vertx.executeBlocking(future -> {
            Finalizers finalizers = new Finalizers();
            for (Finalizer finalizer : finalizers) {
                finalizer.finalizeComponent();
            }
            future.complete();
        }, result -> {
            if (result.succeeded()) {
                LOG.info("Component finalization for application shutdown done successfully");
                shutdownApplicationFuture.complete();
            } else {
                LOG.warn("App shutdown failure", result.cause());
                shutdownApplicationFuture.fail(result.cause());
            }
        });
    }
}
