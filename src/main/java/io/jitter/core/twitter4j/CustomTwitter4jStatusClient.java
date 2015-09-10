package io.jitter.core.twitter4j;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.twitter4j.Twitter4jStatusClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.RawStreamListener;
import twitter4j.StatusListener;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

public class CustomTwitter4jStatusClient extends Twitter4jStatusClient {

    private final static Logger logger = LoggerFactory.getLogger(CustomTwitter4jStatusClient.class);

    private final Client client;
    private final BlockingQueue<String> messageQueue;
    private final ExecutorService executorService;
    private final List<RawStreamListener> rawStreamListeners;

    public CustomTwitter4jStatusClient(Client client, BlockingQueue<String> blockingQueue, List<? extends StatusListener> listeners, ExecutorService executorService, List<RawStreamListener> rawStreamListeners) {
        super(client, blockingQueue, listeners, executorService);
        this.client = Preconditions.checkNotNull(client);
        this.messageQueue = Preconditions.checkNotNull(blockingQueue);
        this.executorService = Preconditions.checkNotNull(executorService);
        Preconditions.checkNotNull(rawStreamListeners);
        this.rawStreamListeners = ImmutableList.copyOf(rawStreamListeners);
    }

    /**
     * Forks off a runnable with the executor provided. Multiple calls are allowed, but the listeners must be
     * threadsafe.
     */
    @Override
    public void process() {
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
                            parseMessage(msg);
                        } catch (Exception e) {
                            logger.warn("Exception thrown during parsing msg " + msg, e);
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
        for (RawStreamListener listener : rawStreamListeners) {
            listener.onMessage(rawString);
        }
    }

}
