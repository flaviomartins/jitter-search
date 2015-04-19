package org.novasearch.jitter.core.stream;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.UserstreamEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.Configuration;

import java.util.ArrayList;
import java.util.EventListener;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class UserStream implements Managed {
    final static Logger logger = LoggerFactory.getLogger(UserStream.class);


    private Authentication auth;
    private List<StreamEventListener> eventListeners = new ArrayList<>();

    public UserStream(OAuth1 oAuth1) {
        this.auth = oAuth1;
    }

    public UserStream(org.novasearch.jitter.core.twitter.OAuth1 oAuth1) {
        this.auth = new OAuth1(oAuth1.getConsumerKey(), oAuth1.getConsumerSecret(), oAuth1.getToken(), oAuth1.getTokenSecret());
    }

    public void addEventListener(StreamEventListener streamEventListener) {
        eventListeners.add(streamEventListener);
    }

    public void run() throws InterruptedException {
        // Create an appropriately sized blocking queue
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

        // Define our endpoint: By default, delimited=length is set (we need this for our processor)
        // and stall warnings are on.
        UserstreamEndpoint endpoint = new UserstreamEndpoint();
        endpoint.stallWarnings(false);

        // Create a new BasicClient. By default gzip is enabled.
        BasicClient client = new ClientBuilder()
                .name("userStreamClient")
                .hosts(Constants.USERSTREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        // Establish a connection
        client.connect();

        // Do whatever needs to be done with messages
        while (!client.isDone()) {
            String msg = queue.poll(5, TimeUnit.SECONDS);
            if (msg == null) {
                logger.debug("Did not receive a message in 5 seconds");
            } else {
                logger.info(msg);
                notifyOnMsg(msg);
            }
        }

        client.stop();
        logger.error("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
        // Print some stats
        logger.info(String.format("The client read %d messages!", client.getStatsTracker().getNumMessages()));
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }

    private void notifyOnMsg(String msg) {
        for (StreamEventListener eventListener : eventListeners) {
            eventListener.onMsg(msg);
        }
    }

    public static void main(String[] args) {
        // The factory instance is re-useable and thread safe.
        final Twitter twitter = TwitterFactory.getSingleton();
        Configuration cfg = twitter.getConfiguration();
        org.novasearch.jitter.core.twitter.OAuth1 oAuth1 = new org.novasearch.jitter.core.twitter.OAuth1(cfg.getOAuthConsumerKey(), cfg.getOAuthConsumerSecret(), cfg.getOAuthAccessToken(), cfg.getOAuthAccessTokenSecret());
        UserStream userStream = new UserStream(oAuth1);
        userStream.addEventListener(new StreamEventListener() {
            @Override
            public void onMsg(String msg) {
                System.out.println("ON_MSG " + msg);
            }
        });
        try {
            userStream.run();
        } catch (InterruptedException e) {
            System.out.println(e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
