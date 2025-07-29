package io.jitter.core.stream;

import com.google.common.collect.ImmutableList;
import io.dropwizard.lifecycle.Managed;
import io.jitter.core.hbc.twitter4j.RawTwitter4jUserstreamClient;
import io.jitter.core.twitter.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.util.List;
import java.util.concurrent.*;

@SuppressWarnings("LoggingSimilarMessage")
public class UserStream implements Managed {

    private final static Logger logger = LoggerFactory.getLogger(UserStream.class);

    private final OAuth1 oAuth1;
    private final List<UserStreamListener> userStreamListeners;
    private final List<RawStreamListener> rawStreamListeners;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public UserStream(OAuth1 oAuth1, List<UserStreamListener> userStreamListeners, List<RawStreamListener> rawStreamListeners) {
        this.oAuth1 = oAuth1;
        this.userStreamListeners = ImmutableList.copyOf(userStreamListeners);
        this.rawStreamListeners = ImmutableList.copyOf(rawStreamListeners);
    }

    @Override
    public void start() throws Exception {
        // Create an appropriately sized blocking queue
        final BlockingQueue<String> queue = new LinkedBlockingQueue<>(10000);
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setJSONStoreEnabled(true);
        try {
            Twitter twitter = new TwitterFactory().getInstance();
            twitter.setOAuthAccessToken(new AccessToken(oAuth1.getToken(), oAuth1.getTokenSecret()));
            User user = twitter.verifyCredentials();
            final Paging paging = new Paging();
            paging.setCount(200);

            ScheduledFuture<?> scheduledFuture = scheduler.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        List<Status> statuses = twitter.getHomeTimeline(paging);
                        logger.info("Getting @" + user.getScreenName() + "'s home timeline: found " + statuses.size() + ".");
                        for (int i = statuses.size() - 1; i >= 0; i--) {
                            Status status = statuses.get(i);
                            String rawJSON = TwitterObjectFactory.getRawJSON(status);
                            queue.add(rawJSON);
                            paging.setSinceId(status.getId());
                        }
                        statuses.clear();
                    } catch (TwitterException te) {
                        if (te.getStatusCode() == 429) {
                            int secondsUntilReset = te.getRateLimitStatus().getSecondsUntilReset();
                            try {
                                Thread.sleep(secondsUntilReset * 1000L);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        } else {
                            te.printStackTrace();
                        }
                        logger.error("Failed to get timeline: " + te.getMessage());
                    } catch (IllegalStateException ise) {
                        logger.error("Failed to get rawJSON: " + ise.getMessage());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, 0, 1, TimeUnit.MINUTES);
        } catch (TwitterException te) {
            te.printStackTrace();
            logger.error("Failed to get timeline: " + te.getMessage());
        }

        // Create an executor service which will spawn threads to do the actual work of parsing the incoming messages and
        // calling the listeners on each message
        int numProcessingThreads = 4;
        ExecutorService service = Executors.newFixedThreadPool(numProcessingThreads);

        // Wrap our BasicClient with the twitter4j client
        RawTwitter4jUserstreamClient t4jClient = new RawTwitter4jUserstreamClient(
                queue, userStreamListeners, service, rawStreamListeners);

        // Establish a connection
        for (int threads = 0; threads < numProcessingThreads; threads++) {
            // This must be called once per processing thread
            t4jClient.process();
        }
    }

    @Override
    public void stop() throws Exception {

    }

}
