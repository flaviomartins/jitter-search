package org.novasearch.jitter;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.novasearch.jitter.core.search.SearchManager;
import org.novasearch.jitter.core.selection.taily.TailyManager;
import org.novasearch.jitter.health.*;
import org.novasearch.jitter.resources.*;
import org.novasearch.jitter.core.selection.SelectionManager;
import org.novasearch.jitter.tasks.SelectionManagerIndexTask;
import org.novasearch.jitter.tasks.SearchManagerIndexTask;
import org.novasearch.jitter.tasks.TailyManagerIndexTask;
import org.novasearch.jitter.tasks.TwitterManagerArchiveTask;
import org.novasearch.jitter.core.twitter.manager.TwitterManager;
import org.novasearch.jitter.core.twitter.archiver.TwitterArchiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        final SearchManager searchManager = configuration.getSearchManagerFactory().build(environment);
        final SearchManagerHealthCheck searchManagerHealthCheck =
                new SearchManagerHealthCheck(searchManager);
        environment.healthChecks().register("search-manager", searchManagerHealthCheck);
        environment.admin().addTask(new SearchManagerIndexTask(searchManager));

        final SearchResource searchResource = new SearchResource(searchManager);
        environment.jersey().register(searchResource);

        final TopTermsResource topTermsResource = new TopTermsResource(searchManager);
        environment.jersey().register(topTermsResource);

        final SelectionManager selectionManager = configuration.getSelectionManagerFactory().build(environment);
        final SelectionManagerHealthCheck selectionManagerHealthCheck =
                new SelectionManagerHealthCheck(selectionManager);
        environment.healthChecks().register("selection", selectionManagerHealthCheck);
        environment.admin().addTask(new SelectionManagerIndexTask(selectionManager));

        final TailyManager tailyManager = configuration.getTailyManagerFactory().build(environment);
        final TailyManagerHealthCheck tailyManagerHealthCheck =
                new TailyManagerHealthCheck(tailyManager);
        environment.healthChecks().register("taily", tailyManagerHealthCheck);

        final TwitterManager twitterManager = configuration.getTwitterManagerFactory().build(environment);
        final TwitterManagerHealthCheck twitterManagerHealthCheck =
                new TwitterManagerHealthCheck(twitterManager);
        environment.healthChecks().register("twitter-manager", twitterManagerHealthCheck);
        environment.admin().addTask(new TwitterManagerArchiveTask(twitterManager));
        selectionManager.setTwitterManager(twitterManager);

        final TwitterArchiver twitterArchiver = configuration.getTwitterArchiverFactory().build(environment);
        final TwitterArchiverHealthCheck twitterArchiverHealthCheck =
                new TwitterArchiverHealthCheck(twitterArchiver);
        environment.healthChecks().register("twitter-archiver", twitterArchiverHealthCheck);
        selectionManager.setTwitterArchiver(twitterArchiver);

        final TailyResource tailyResource = new TailyResource(tailyManager);
        environment.jersey().register(tailyResource);
        environment.admin().addTask(new TailyManagerIndexTask(tailyManager));

        final SelectionResource selectionResource = new SelectionResource(selectionManager);
        environment.jersey().register(selectionResource);

        final SelectSearchResource selectSearchResource = new SelectSearchResource(searchManager, selectionManager);
        environment.jersey().register(selectSearchResource);
    }
}
