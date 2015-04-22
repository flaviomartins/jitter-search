package org.novasearch.jitter.core.stream;

import com.google.common.collect.ImmutableList;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.RawStreamListener;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class SampleStream implements Managed {
    final static Logger logger = LoggerFactory.getLogger(SampleStream.class);


    private final Authentication auth;
    private final List<RawStreamListener> rawStreamListeners;

    private BasicClient client;

    public SampleStream(OAuth1 oAuth1, List<RawStreamListener> listeners) {
        this.auth = oAuth1;
        this.rawStreamListeners = ImmutableList.copyOf(listeners);
    }

    public SampleStream(org.novasearch.jitter.core.twitter.OAuth1 oAuth1, List<RawStreamListener> listeners) {
        this(new OAuth1(oAuth1.getConsumerKey(), oAuth1.getConsumerSecret(), oAuth1.getToken(), oAuth1.getTokenSecret()), listeners);
    }


    @Override
    public void start() throws Exception {
        // Create an appropriately sized blocking queue
        final BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>(10000);

        // Define our endpoint: By default, delimited=length is set (we need this for our processor)
        // and stall warnings are on.
        StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
        endpoint.stallWarnings(false);

        // Create a new BasicClient. By default gzip is enabled.
        client = new ClientBuilder()
                .name("sampleStreamClient")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(messageQueue))
                .build();

        ExecutorService executorService = Executors.newSingleThreadExecutor();

        // Establish a connection
        client.connect();

        if (client.isDone() || executorService.isTerminated()) {
            throw new IllegalStateException("Client is already stopped");
        }
        Runnable runner = new Runnable() {
            @Override
            public void run() {
                try {
                    while (!client.isDone()) {
                        String msg = messageQueue.take();
                        try {
                            onMessage(msg);
                        } catch (Exception e) {
                            logger.warn("Exception thrown during parsing msg {}", msg, e);
                            onException(e);
                        }
                    }
                } catch (Exception e) {
                    onException(e);
                }
            }
        };

        executorService.execute(runner);
    }

    private void onMessage(String rawString) {
        for (RawStreamListener eventListener : rawStreamListeners) {
            eventListener.onMessage(rawString);
        }
    }

    private void onException(Exception e) {
        for (RawStreamListener eventListener : rawStreamListeners) {
            eventListener.onException(e);
        }
    }

    @Override
    public void stop() throws Exception {
        if (client != null) {
            client.stop();
            logger.error("Client connection closed: {}", client.getExitEvent().getMessage());
            logger.info("The client read {} messages!", client.getStatsTracker().getNumMessages());
        }
    }

}
