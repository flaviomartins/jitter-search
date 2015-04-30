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
import org.novasearch.jitter.core.twitter4j.CustomTwitter4jStatusClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.RawStreamListener;
import twitter4j.StatusListener;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class SampleStream implements Managed {

    final static Logger logger = LoggerFactory.getLogger(SampleStream.class);

    private final Authentication auth;
    private final List<? extends StatusListener> statusListeners;
    private final List<RawStreamListener> rawStreamListeners;

    private BasicClient client;

    public SampleStream(OAuth1 oAuth1, List<StatusListener> statusListeners, List<RawStreamListener> rawStreamListeners) {
        this.auth = oAuth1;
        this.statusListeners = ImmutableList.copyOf(statusListeners);
        this.rawStreamListeners = ImmutableList.copyOf(rawStreamListeners);
    }

    public SampleStream(org.novasearch.jitter.core.twitter.OAuth1 oAuth1, List<StatusListener> statusListeners, List<RawStreamListener> rawStreamListeners) {
        this(new OAuth1(oAuth1.getConsumerKey(), oAuth1.getConsumerSecret(), oAuth1.getToken(), oAuth1.getTokenSecret()), statusListeners, rawStreamListeners);
    }

    @Override
    public void start() throws Exception {
        // Create an appropriately sized blocking queue
        final BlockingQueue<String> queue = new LinkedBlockingQueue<>(10000);

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
                .processor(new StringDelimitedProcessor(queue))
                .build();

        // Create an executor service which will spawn threads to do the actual work of parsing the incoming messages and
        // calling the listeners on each message
        int numProcessingThreads = 4;
        ExecutorService service = Executors.newFixedThreadPool(numProcessingThreads);

        // Wrap our BasicClient with the twitter4j client
        CustomTwitter4jStatusClient t4jClient = new CustomTwitter4jStatusClient(
                client, queue, statusListeners, service, rawStreamListeners);

        // Establish a connection
        t4jClient.connect();
        for (int threads = 0; threads < numProcessingThreads; threads++) {
            // This must be called once per processing thread
            t4jClient.process();
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
