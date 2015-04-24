package org.novasearch.jitter;

import com.google.common.collect.Lists;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.novasearch.jitter.core.search.SearchManager;
import org.novasearch.jitter.core.selection.SelectionManager;
import org.novasearch.jitter.core.selection.taily.TailyManager;
import org.novasearch.jitter.core.stream.LiveStreamIndexer;
import org.novasearch.jitter.core.stream.SampleStream;
import org.novasearch.jitter.core.stream.StreamLogger;
import org.novasearch.jitter.core.stream.UserStream;
import org.novasearch.jitter.core.twitter.OAuth1;
import org.novasearch.jitter.core.twitter.manager.TwitterManager;
import org.novasearch.jitter.health.SearchManagerHealthCheck;
import org.novasearch.jitter.health.SelectionManagerHealthCheck;
import org.novasearch.jitter.health.TailyManagerHealthCheck;
import org.novasearch.jitter.health.TwitterManagerHealthCheck;
import org.novasearch.jitter.resources.*;
import org.novasearch.jitter.tasks.SearchManagerIndexTask;
import org.novasearch.jitter.tasks.SelectionManagerIndexTask;
import org.novasearch.jitter.tasks.TailyManagerIndexTask;
import org.novasearch.jitter.tasks.TwitterManagerArchiveTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.RawStreamListener;
import twitter4j.StatusListener;
import twitter4j.UserStreamListener;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import java.io.IOException;
import java.util.EnumSet;

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
                    Environment environment) throws IOException, InterruptedException {
        // Enable CORS headers
        final FilterRegistration.Dynamic cors =
                environment.servlets().addFilter("CORS", CrossOriginFilter.class);

        // Configure CORS parameters
        cors.setInitParameter("allowedOrigins", "*");
        cors.setInitParameter("allowedHeaders", "X-Requested-With,Content-Type,Accept,Origin");
        cors.setInitParameter("allowedMethods", "OPTIONS,GET,PUT,POST,DELETE,HEAD");

        // Add URL mapping
        cors.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");


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


        final TailyResource tailyResource = new TailyResource(tailyManager);
        environment.jersey().register(tailyResource);
        environment.admin().addTask(new TailyManagerIndexTask(tailyManager));

        final SelectionResource selectionResource = new SelectionResource(selectionManager);
        environment.jersey().register(selectionResource);

        final SelectSearchResource selectSearchResource = new SelectSearchResource(searchManager, selectionManager);
        environment.jersey().register(selectSearchResource);


        OAuth1 oAuth1 = configuration.getTwitterManagerFactory().getoAuth1Factory().build();

        final LiveStreamIndexer userStreamIndexer = new LiveStreamIndexer(selectionManager.getIndexPath(), 1);
        final StreamLogger userStreamLogger = new StreamLogger("./archive/user");
        final TimelineSseResource timelineSseResource = new TimelineSseResource();
        final UserStream userStream = new UserStream(oAuth1,
                Lists.<UserStreamListener>newArrayList(timelineSseResource, userStreamIndexer),
                Lists.<RawStreamListener>newArrayList(userStreamLogger));
        environment.lifecycle().manage(userStream);
        environment.lifecycle().manage(userStreamIndexer);
        environment.jersey().register(timelineSseResource);

        final LiveStreamIndexer statusStreamIndexer = new LiveStreamIndexer(searchManager.getIndexPath(), 1000);
        final StreamLogger statusStreamLogger = new StreamLogger("./archive/sample");
        final SampleSseResource sampleSseResource = new SampleSseResource();
        final SampleStream statusStream = new SampleStream(oAuth1,
                Lists.<StatusListener>newArrayList(sampleSseResource, statusStreamIndexer),
                Lists.<RawStreamListener>newArrayList(statusStreamLogger));
        environment.lifecycle().manage(statusStream);
        environment.lifecycle().manage(statusStreamIndexer);
        environment.jersey().register(sampleSseResource);
    }
}
