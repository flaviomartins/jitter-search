package org.novasearch.jitter;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.novasearch.jitter.health.ResourceSelectionHealthCheck;
import org.novasearch.jitter.health.TwitterManagerHealthCheck;
import org.novasearch.jitter.resources.ResourceSelectionResource;
import org.novasearch.jitter.resources.SearchResource;
import org.novasearch.jitter.resources.TopTermsResource;
import org.novasearch.jitter.rs.ResourceSelection;
import org.novasearch.jitter.tasks.TwitterArchiverTask;
import org.novasearch.jitter.twitter.TwitterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Twitter;

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

        SearchResource searchResource = new SearchResource(new File(configuration.getIndex()));
        environment.jersey().register(searchResource);

        final TopTermsResource topTermsResource = new TopTermsResource(new File(configuration.getIndex()));
        environment.jersey().register(topTermsResource);

        final TwitterManager twitterManager = configuration.getTwitterManagerFactory().build(environment);
        final TwitterManagerHealthCheck twitterManagerHealthCheck =
                new TwitterManagerHealthCheck(twitterManager);
        environment.healthChecks().register("twitter-manager", twitterManagerHealthCheck);

        ResourceSelection resourceSelection = configuration.getResourceSelectionFactory().build(environment, twitterManager);
        final ResourceSelectionHealthCheck healthCheck =
                new ResourceSelectionHealthCheck(resourceSelection);
        environment.healthChecks().register("rs", healthCheck);

        final ResourceSelectionResource resourceSelectionResource = new ResourceSelectionResource(resourceSelection);
        environment.jersey().register(resourceSelectionResource);

        environment.admin().addTask(new TwitterArchiverTask(twitterManager));
    }
}
