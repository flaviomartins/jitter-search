package io.jitter.core.stream;

import com.google.common.collect.ImmutableList;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.LineStringProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth2BearerToken;
import io.dropwizard.lifecycle.Managed;
import io.jitter.core.hbc.twitter4j.RawTwitter4jStatusClient;
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

    private final static Logger logger = LoggerFactory.getLogger(SampleStream.class);

    private final Authentication auth;
    private final List<? extends StatusListener> statusListeners;
    private final List<RawStreamListener> rawStreamListeners;

    private BasicClient client;

    public SampleStream(OAuth2BearerToken oAuth2, List<StatusListener> statusListeners, List<RawStreamListener> rawStreamListeners) {
        this.auth = oAuth2;
        this.statusListeners = ImmutableList.copyOf(statusListeners);
        this.rawStreamListeners = ImmutableList.copyOf(rawStreamListeners);
    }

    public SampleStream(io.jitter.core.twitter.OAuth2BearerToken oAuth2, List<StatusListener> statusListeners, List<RawStreamListener> rawStreamListeners) {
        this(new OAuth2BearerToken(oAuth2.getBearerToken()), statusListeners, rawStreamListeners);
    }

    @Override
    public void start() throws Exception {
        // Create an appropriately sized blocking queue
        final BlockingQueue<String> queue = new LinkedBlockingQueue<>(10000);

        // Define our endpoint: By default, delimited=length is set (we need this for our processor)
        // and stall warnings are on.
        StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
        endpoint.delimited(false);
        endpoint.stallWarnings(false);
        endpoint.addQueryParameter("tweet.fields", "attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,source,text,withheld");
        endpoint.addQueryParameter("expansions", "author_id");
        endpoint.addQueryParameter("user.fields", "created_at,description,entities,location,pinned_tweet_id,profile_image_url,protected,public_metrics,url,verified,withheld");

        // Create a new BasicClient. By default gzip is enabled.
        client = new ClientBuilder()
                .name("sampleStreamClient")
                .hosts(Constants.API_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new LineStringProcessor(queue))
                .build();

        // Create an executor service which will spawn threads to do the actual work of parsing the incoming messages and
        // calling the listeners on each message
        int numProcessingThreads = 4;
        ExecutorService service = Executors.newFixedThreadPool(numProcessingThreads);

        // Wrap our BasicClient with the twitter4j client
        RawTwitter4jStatusClient t4jClient = new RawTwitter4jStatusClient(
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
