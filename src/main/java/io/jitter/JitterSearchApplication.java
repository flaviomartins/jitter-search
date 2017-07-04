package io.jitter;

import com.google.common.collect.Lists;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.jitter.core.search.SearchManager;
import io.jitter.core.taily.TailyManager;
import io.jitter.core.shards.ShardsManager;
import io.jitter.core.stream.SampleStream;
import io.jitter.core.stream.UserStream;
import io.jitter.core.twitter.OAuth1;
import io.jitter.core.twitter.manager.TwitterManager;
import io.jitter.core.utils.NoExitSecurityManager;
import io.jitter.core.wikipedia.WikipediaManager;
import io.jitter.health.*;
import io.jitter.resources.*;
import io.jitter.tasks.*;
import io.swagger.jaxrs.config.BeanConfig;
import io.swagger.jaxrs.listing.ApiListingResource;
import io.swagger.jaxrs.listing.SwaggerSerializers;
import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import io.jitter.core.selection.SelectionManager;
import io.jitter.core.stream.LiveStreamIndexer;
import io.jitter.core.stream.StreamLogger;
import io.jitter.core.twittertools.api.TrecMicroblogAPIWrapper;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import java.io.IOException;
import java.util.EnumSet;

public class JitterSearchApplication extends Application<JitterSearchConfiguration> {

    final static Logger logger = LoggerFactory.getLogger(JitterSearchApplication.class);

    public static void main(String[] args) throws Exception {
        // a static method that loads the RocksDB C++ library.
        RocksDB.loadLibrary();
        System.setSecurityManager(new NoExitSecurityManager());
        new JitterSearchApplication().run(args);
    }

    @Override
    public String getName() {
        return "jitter-responses";
    }

    @Override
    public void initialize(Bootstrap<JitterSearchConfiguration> bootstrap) {
        // API documentation bundles
        bootstrap.addBundle(new AssetsBundle("/swagger-ui", "/apidocs", "index.html", "swagger-ui"));
        bootstrap.addBundle(new AssetsBundle("/redoc", "/redoc", "index.html", "redoc"));
    }

    @Override
    public void run(JitterSearchConfiguration configuration,
                    Environment environment) throws IOException, InterruptedException {

        if (configuration.isCors()) {
            // Enable CORS headers
            final FilterRegistration.Dynamic cors =
                    environment.servlets().addFilter("CORS", CrossOriginFilter.class);

            // Configure CORS parameters
            cors.setInitParameter("allowedOrigins", "*");
            cors.setInitParameter("allowedHeaders", "X-Requested-With,Content-Type,Accept,Origin");
            cors.setInitParameter("allowedMethods", "OPTIONS,GET,PUT,POST,DELETE,HEAD");

            // Add URL mapping
            cors.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");
        }

        // Swagger API listing
        BeanConfig beanConfig = configuration.getApiDocsFactory().build();
        beanConfig.setResourcePackage("io.jitter.resources");
        beanConfig.setScan(true);

        environment.jersey().register(SwaggerSerializers.class);
        environment.jersey().register(new ApiListingResource());

        final WikipediaManager wikipediaManager = configuration.getWikipediaManagerFactory().build(environment);
        final WikipediaManagerHealthCheck wikipediaManagerHealthCheck =
                new WikipediaManagerHealthCheck(wikipediaManager);
        environment.healthChecks().register("wikipedia-manager", wikipediaManagerHealthCheck);

        final WikipediaSearchResource wikipediaSearchResource = new WikipediaSearchResource(wikipediaManager);
        environment.jersey().register(wikipediaSearchResource);

        final WikipediaTopTermsResource wikipediaTopTermsResource = new WikipediaTopTermsResource(wikipediaManager);
        environment.jersey().register(wikipediaTopTermsResource);

        final TwitterManager twitterManager = configuration.getTwitterManagerFactory().build(environment);
        final TwitterManagerHealthCheck twitterManagerHealthCheck =
                new TwitterManagerHealthCheck(twitterManager);
        environment.healthChecks().register("twitter-manager", twitterManagerHealthCheck);
        environment.admin().addTask(new TwitterManagerArchiveTask(twitterManager));

        final SearchManager searchManager = configuration.getSearchManagerFactory().build(environment, configuration.isLive());
        final SearchManagerHealthCheck searchManagerHealthCheck =
                new SearchManagerHealthCheck(searchManager);
        environment.healthChecks().register("search-manager", searchManagerHealthCheck);
        environment.admin().addTask(new SearchManagerIndexTask(searchManager));
        environment.admin().addTask(new SearchManagerForceMergeTask(searchManager));

        final SearchResource searchResource = new SearchResource(searchManager);
        environment.jersey().register(searchResource);

        final TopTermsResource topTermsResource = new TopTermsResource(searchManager);
        environment.jersey().register(topTermsResource);

        final FeedbackResource feedbackResource = new FeedbackResource(searchManager);
        environment.jersey().register(feedbackResource);

        final TailyManager tailyManager = configuration.getTailyManagerFactory().build(environment);
        final TailyManagerHealthCheck tailyManagerHealthCheck =
                new TailyManagerHealthCheck(tailyManager);
        environment.healthChecks().register("taily", tailyManagerHealthCheck);

        final ShardsManager shardsManager = configuration.getShardsManagerFactory().build(environment, configuration.isLive());
        // indexing
        shardsManager.setTwitterManager(twitterManager);
        // sharding
        shardsManager.setTailyManager(tailyManager);
        final ShardsManagerHealthCheck shardsManagerHealthCheck =
                new ShardsManagerHealthCheck(shardsManager);
        environment.healthChecks().register("shards", shardsManagerHealthCheck);
        environment.admin().addTask(new ShardsManagerIndexTask(shardsManager));
        environment.admin().addTask(new ShardsManagerForceMergeTask(shardsManager));
        environment.admin().addTask(new ShardsManagerStatsTask(shardsManager));
        
        final SelectionManager selectionManager = configuration.getSelectionManagerFactory().build(environment, configuration.isLive());
        // indexing
        selectionManager.setTwitterManager(twitterManager);
        // sharding
        selectionManager.setShardsManager(shardsManager);
        final SelectionManagerHealthCheck selectionManagerHealthCheck =
                new SelectionManagerHealthCheck(selectionManager);
        environment.healthChecks().register("selection", selectionManagerHealthCheck);
        environment.admin().addTask(new SelectionManagerIndexTask(selectionManager));
        environment.admin().addTask(new SelectionManagerForceMergeTask(selectionManager));
        environment.admin().addTask(new SelectionManagerStatsTask(selectionManager));

        final TailyResource tailyResource = new TailyResource(tailyManager);
        environment.jersey().register(tailyResource);
        environment.admin().addTask(new TailyManagerIndexTask(tailyManager));
        
        final SelectionStatsResource selectionStatsResource = new SelectionStatsResource(selectionManager);
        environment.jersey().register(selectionStatsResource);

        final ShardsStatsResource shardsStatsResource = new ShardsStatsResource(shardsManager);
        environment.jersey().register(shardsStatsResource);

        final SelectionTopTermsResource selectionTopTermsResource = new SelectionTopTermsResource(selectionManager);
        environment.jersey().register(selectionTopTermsResource);

        final ShardsTopTermsResource shardsTopTermsResource = new ShardsTopTermsResource(shardsManager);
        environment.jersey().register(shardsTopTermsResource);

        final SelectionResource selectionResource = new SelectionResource(selectionManager);
        environment.jersey().register(selectionResource);

        final SelectSearchResource selectSearchResource = new SelectSearchResource(selectionManager, shardsManager, tailyManager);
        environment.jersey().register(selectSearchResource);

        final MultiFeedbackResource multiFeedbackResource = new MultiFeedbackResource(searchManager, selectionManager, shardsManager, tailyManager);
        environment.jersey().register(multiFeedbackResource);

        final RMTSResource RMTSResource = new RMTSResource(searchManager, selectionManager, shardsManager, tailyManager);
        environment.jersey().register(RMTSResource);


        // TREC

        final TrecMicroblogAPIWrapper trecMicroblogAPIWrapper = configuration.getTrecMicroblogAPIWrapperFactory().build(environment);

        final TrecSearchResource trecSearchResource = new TrecSearchResource(trecMicroblogAPIWrapper);
        environment.jersey().register(trecSearchResource);

        final TrecFeedbackResource trecFeedbackResource = new TrecFeedbackResource(trecMicroblogAPIWrapper);
        environment.jersey().register(trecFeedbackResource);
        
        final TrecShardsFeedbackResource trecShardsFeedbackResource = new TrecShardsFeedbackResource(trecMicroblogAPIWrapper, shardsManager);
        environment.jersey().register(trecShardsFeedbackResource);

        final TrecMultiFeedbackResource trecMultiFeedbackResource = new TrecMultiFeedbackResource(trecMicroblogAPIWrapper, selectionManager, shardsManager, tailyManager);
        environment.jersey().register(trecMultiFeedbackResource);

        final TrecRMTSResource trecRMTSResource = new TrecRMTSResource(trecMicroblogAPIWrapper, selectionManager, shardsManager, tailyManager);
        environment.jersey().register(trecRMTSResource);


        if (configuration.isLive()) {
            OAuth1 oAuth1 = configuration.getTwitterManagerFactory().getOAuth1Factory().build();

            String userLogPath = "./data/archive/user";
            if (StringUtils.isNotBlank(configuration.getUserStreamLogPath()))
                userLogPath = configuration.getUserStreamLogPath();

            final StreamLogger userStreamLogger = new StreamLogger(userLogPath);
            final TimelineSseResource timelineSseResource = new TimelineSseResource();
            environment.jersey().register(timelineSseResource);

            final LiveStreamIndexer userStreamIndexer = new LiveStreamIndexer(shardsManager.getIndexPath(), 10, true);
            final UserStream userStream = new UserStream(oAuth1,
                    Lists.newArrayList(timelineSseResource, userStreamIndexer),
                    Lists.newArrayList(timelineSseResource, userStreamLogger));
            environment.lifecycle().manage(userStream);
            environment.lifecycle().manage(userStreamIndexer);
            
            String statusLogPath = "./data/archive/sample";
            if (StringUtils.isNotBlank(configuration.getStatusStreamLogPath()))
                statusLogPath = configuration.getStatusStreamLogPath();

            final StreamLogger statusStreamLogger = new StreamLogger(statusLogPath);
            final SampleSseResource sampleSseResource = new SampleSseResource();
            environment.jersey().register(sampleSseResource);


            final LiveStreamIndexer statusStreamIndexer = new LiveStreamIndexer(searchManager.getIndexPath(), 10000, false);
            final SampleStream statusStream = new SampleStream(oAuth1,
                    Lists.newArrayList(sampleSseResource, statusStreamIndexer),
                    Lists.newArrayList(sampleSseResource, statusStreamLogger));
            environment.lifecycle().manage(statusStream);
            environment.lifecycle().manage(statusStreamIndexer);
        }
    }
}
