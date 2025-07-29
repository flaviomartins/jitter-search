package io.jitter;

import com.google.common.collect.Lists;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.jetty.setup.ServletEnvironment;
import io.dropwizard.web.WebBundle;
import io.dropwizard.web.conf.WebConfiguration;
import io.jitter.core.search.SearchManager;
import io.jitter.core.selection.SelectionManager;
import io.jitter.core.shards.ShardsManager;
import io.jitter.core.stream.LiveStreamIndexer;
import io.jitter.core.stream.RawStreamLogger;
import io.jitter.core.stream.SampleStream;
import io.jitter.core.stream.UserStream;
import io.jitter.core.taily.TailyManager;
import io.jitter.core.twitter.OAuth1;
import io.jitter.core.twitter.OAuth2BearerToken;
import io.jitter.core.twitter.manager.TwitterManager;
import io.jitter.core.twittertools.api.TrecMicroblogAPIWrapper;
import io.jitter.core.wikipedia.WikipediaManager;
import io.jitter.health.*;
import io.jitter.resources.*;
import io.jitter.tasks.*;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JitterSearchApplication extends Application<JitterSearchConfiguration> {

    final static Logger logger = LoggerFactory.getLogger(JitterSearchApplication.class);

    public static void main(String[] args) throws Exception {
        // a static method that loads the RocksDB C++ library.
        RocksDB.loadLibrary();
        new JitterSearchApplication().run(args);
    }

    @Override
    public String getName() {
        return "jitter-responses";
    }

    @Override
    public void initialize(Bootstrap<JitterSearchConfiguration> bootstrap) {
        bootstrap.addBundle(new WebBundle<>() {
            @Override
            public WebConfiguration getWebConfiguration(final JitterSearchConfiguration configuration) {
                return configuration.getWebConfiguration();
            }

            // Optional: Override Servlet environment to apply the configuration to the admin servlets
            @Override
            protected ServletEnvironment getServletEnvironment(Environment environment) {
                return environment.admin();
            }
        });
        // API documentation bundles
        bootstrap.addBundle(new AssetsBundle("/swagger-ui", "/apidocs", "index.html", "swagger-ui"));
        bootstrap.addBundle(new AssetsBundle("/redoc", "/redoc", "index.html", "redoc"));
    }

    @Override
    public void run(JitterSearchConfiguration configuration,
                    Environment environment) throws IOException {

        final TwitterManager twitterManager = configuration.getTwitterManagerFactory().build(environment);
        final TwitterManagerHealthCheck twitterManagerHealthCheck =
                new TwitterManagerHealthCheck(twitterManager);
        environment.healthChecks().register("twitter-manager", twitterManagerHealthCheck);

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

        final WikipediaManager wikipediaManager = configuration.getWikipediaManagerFactory().build(environment);
        final WikipediaManagerHealthCheck wikipediaManagerHealthCheck =
                new WikipediaManagerHealthCheck(wikipediaManager);
        environment.healthChecks().register("wikipedia-manager", wikipediaManagerHealthCheck);

        final WikipediaSearchResource wikipediaSearchResource = new WikipediaSearchResource(wikipediaManager);
        environment.jersey().register(wikipediaSearchResource);

        final WikipediaTopTermsResource wikipediaTopTermsResource = new WikipediaTopTermsResource(wikipediaManager);
        environment.jersey().register(wikipediaTopTermsResource);

        final WikipediaFeedbackResource wikipediaFeedbackResource = new WikipediaFeedbackResource(searchManager, wikipediaManager);
        environment.jersey().register(wikipediaFeedbackResource);

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

        final SelectionResource selectionResource = new SelectionResource(selectionManager, tailyManager);
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

        final TrecWikipediaFeedbackResource trecWikipediaFeedbackResource = new TrecWikipediaFeedbackResource(trecMicroblogAPIWrapper, wikipediaManager);
        environment.jersey().register(trecWikipediaFeedbackResource);

        if (configuration.isLive()) {
            OAuth1 oAuth1 = configuration.getTwitterManagerFactory().getOAuth1Factory().build();
            OAuth2BearerToken oAuth2BearerToken = configuration.getTwitterManagerFactory().getOAuth2BearerTokenFactory().build();

            String userLogPath = "./data/archive/user";
            if (StringUtils.isNotBlank(configuration.getUserStreamLogPath()))
                userLogPath = configuration.getUserStreamLogPath();

            final RawStreamLogger userRawStreamLogger = new RawStreamLogger(userLogPath);
            final TimelineSseResource timelineSseResource = new TimelineSseResource();
            environment.jersey().register(timelineSseResource);

            UserStream userStream;
            if (configuration.isIndexing()) {
                final LiveStreamIndexer userStreamIndexer = new LiveStreamIndexer(shardsManager.getIndexPath(), 10);
                userStream = new UserStream(oAuth1,
                        Lists.newArrayList(timelineSseResource, userStreamIndexer),
                        Lists.newArrayList(timelineSseResource, userRawStreamLogger));
                environment.lifecycle().manage(userStreamIndexer);
            } else {
                userStream = new UserStream(oAuth1,
                        Lists.newArrayList(timelineSseResource),
                        Lists.newArrayList(timelineSseResource, userRawStreamLogger));
            }
            environment.lifecycle().manage(userStream);

            String statusLogPath = "./data/archive/sample";
            if (StringUtils.isNotBlank(configuration.getStatusStreamLogPath()))
                statusLogPath = configuration.getStatusStreamLogPath();

            final RawStreamLogger statusRawStreamLogger = new RawStreamLogger(statusLogPath);
            final SampleSseResource sampleSseResource = new SampleSseResource();
            environment.jersey().register(sampleSseResource);

            SampleStream statusStream;
            if (configuration.isIndexing()) {
                final LiveStreamIndexer statusStreamIndexer = new LiveStreamIndexer(searchManager.getIndexPath(), 10000);
                statusStream = new SampleStream(oAuth2BearerToken,
                        Lists.newArrayList(sampleSseResource, statusStreamIndexer),
                        Lists.newArrayList(sampleSseResource, statusRawStreamLogger));
                environment.lifecycle().manage(statusStreamIndexer);
            } else {
                statusStream = new SampleStream(oAuth2BearerToken,
                        Lists.newArrayList(sampleSseResource),
                        Lists.newArrayList(sampleSseResource, statusRawStreamLogger));
            }
            environment.lifecycle().manage(statusStream);
        }
    }
}
