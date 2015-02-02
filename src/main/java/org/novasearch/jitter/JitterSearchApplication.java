package org.novasearch.jitter;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.novasearch.jitter.core.ReputationReader;
import org.novasearch.jitter.health.ResourceSelectionHealthCheck;
import org.novasearch.jitter.resources.ReputationResource;
import org.novasearch.jitter.resources.ResourceSelectionResource;
import org.novasearch.jitter.resources.SearchResource;
import org.novasearch.jitter.resources.TopTermsResource;
import org.novasearch.jitter.rs.ResourceSelection;
import org.novasearch.jitter.twitter.TwitterUserTimelinesManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class JitterSearchApplication extends Application<JitterSearchConfiguration> {
    final static Logger logger = LoggerFactory.getLogger(JitterSearchApplication.class);

    public static void main(String[] args) throws Exception {
        new JitterSearchApplication().run(args);
    }

    @Override
    public String getName() {
        return "jitter-responses";
    }

    @Override
    public void initialize(Bootstrap<JitterSearchConfiguration> bootstrap) {
        // nothing to do yet
    }

    @Override
    public void run(JitterSearchConfiguration configuration,
                    Environment environment) throws IOException {

        String reputationFile = configuration.getReputationFile();
        ReputationReader reputationReader = null;
        if (reputationFile != null) {
            reputationReader = new ReputationReader(new File(reputationFile));
            logger.info("Reading reputation file.");

            final ReputationResource reputationResource = new ReputationResource(reputationReader);
            environment.jersey().register(reputationResource);
        } else {
            logger.warn("No reputation file provided.");
        }

        SearchResource searchResource;
        if (reputationReader != null) {
            searchResource = new SearchResource(new File(configuration.getIndex()), reputationReader);
        } else {
            logger.warn("Continuing without reputation information.");
            searchResource = new SearchResource(new File(configuration.getIndex()));
        }
        environment.jersey().register(searchResource);

        final TopTermsResource topTermsResource = new TopTermsResource(new File(configuration.getIndex()));
        environment.jersey().register(topTermsResource);

        ResourceSelection resourceSelection = configuration.getResourceSelectionFactory().build(environment);
//        resourceSelection.index();

        final ResourceSelectionHealthCheck healthCheck =
                new ResourceSelectionHealthCheck(resourceSelection);
        environment.healthChecks().register("rs", healthCheck);

        final ResourceSelectionResource resourceSelectionResource = new ResourceSelectionResource(resourceSelection);
        environment.jersey().register(resourceSelectionResource);
    }
}
