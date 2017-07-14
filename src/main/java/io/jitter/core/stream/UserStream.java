package io.jitter.core.stream;

import com.google.common.collect.ImmutableList;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.UserstreamEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import io.dropwizard.lifecycle.Managed;
import io.jitter.core.twitter4j.RawTwitter4jUserstreamClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;

import java.util.List;
import java.util.concurrent.*;

public class UserStream implements Managed {

    private final static Logger logger = LoggerFactory.getLogger(UserStream.class);

    private final Authentication auth;
    private final List<UserStreamListener> userStreamListeners;
    private final List<RawStreamListener> rawStreamListeners;
    private BasicClient client;

    public UserStream(OAuth1 oAuth1, List<UserStreamListener> userStreamListeners, List<RawStreamListener> rawStreamListeners) {
        this.auth = oAuth1;
        this.userStreamListeners = ImmutableList.copyOf(userStreamListeners);
        this.rawStreamListeners = ImmutableList.copyOf(rawStreamListeners);
    }

    public UserStream(io.jitter.core.twitter.OAuth1 oAuth1, List<UserStreamListener> userStreamListeners, List<RawStreamListener> rawStreamListeners) {
        this(new OAuth1(oAuth1.getConsumerKey(), oAuth1.getConsumerSecret(), oAuth1.getToken(), oAuth1.getTokenSecret()), userStreamListeners, rawStreamListeners);
    }

    @Override
    public void start() throws Exception {
        // Create an appropriately sized blocking queue
        final BlockingQueue<String> queue = new LinkedBlockingQueue<>(10000);

        // Define our endpoint: By default, delimited=length is set (we need this for our processor)
        // and stall warnings are on.
        UserstreamEndpoint endpoint = new UserstreamEndpoint();
        endpoint.stallWarnings(false);

        // Create a new BasicClient. By default gzip is enabled.
        client = new ClientBuilder()
                .name("userStreamClient")
                .hosts(Constants.USERSTREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        // Create an executor service which will spawn threads to do the actual work of parsing the incoming messages and
        // calling the listeners on each message
        int numProcessingThreads = 4;
        ExecutorService service = Executors.newFixedThreadPool(numProcessingThreads);

        // Wrap our BasicClient with the twitter4j client
        RawTwitter4jUserstreamClient t4jClient = new RawTwitter4jUserstreamClient(
                client, queue, userStreamListeners, service, rawStreamListeners);

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
