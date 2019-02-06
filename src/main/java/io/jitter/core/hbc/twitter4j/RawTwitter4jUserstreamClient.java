package io.jitter.core.hbc.twitter4j;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

public class RawTwitter4jUserstreamClient {

    private final static Logger logger = LoggerFactory.getLogger(RawTwitter4jUserstreamClient.class);

    private final BlockingQueue<String> messageQueue;
    private final ExecutorService executorService;
    private final List<UserStreamListener> listeners;
    private final List<RawStreamListener> rawStreamListeners;

    public RawTwitter4jUserstreamClient(BlockingQueue<String> blockingQueue, List<UserStreamListener> listeners, ExecutorService executorService, List<RawStreamListener> rawStreamListeners) {
        this.messageQueue = Preconditions.checkNotNull(blockingQueue);
        this.executorService = Preconditions.checkNotNull(executorService);
        Preconditions.checkNotNull(listeners);
        this.listeners = ImmutableList.copyOf(listeners);
        Preconditions.checkNotNull(rawStreamListeners);
        this.rawStreamListeners = ImmutableList.copyOf(rawStreamListeners);
    }

    /**
     * Forks off a runnable with the executor provided. Multiple calls are allowed, but the listeners must be
     * threadsafe.
     */
    public void process() {
        if (executorService.isTerminated()) {
            throw new IllegalStateException("Client is already stopped");
        }
        Runnable runner = () -> {
            try {
                while (true) {
                    String msg = messageQueue.take();
                    Status status = TwitterObjectFactory.createStatus(msg);
                    try {
                        onMessage(msg);
                        onStatus(status);
                    } catch (Exception e) {
                        logger.warn("Exception thrown during parsing msg " + msg, e);
                        onException(e);
                    }
                }
            } catch (Exception e) {
                onException(e);
            }
        };

        executorService.execute(runner);
    }

    private void onMessage(String rawString) {
        for (RawStreamListener listener : rawStreamListeners) {
            listener.onMessage(rawString);
        }
    }

    protected void onStatus(final Status status) {
        for (UserStreamListener listener : listeners) {
            listener.onStatus(status);
        }
    }

    private void onException(Exception e) {
        for (UserStreamListener listener : listeners) {
            listener.onException(e);
        }
    }

}
