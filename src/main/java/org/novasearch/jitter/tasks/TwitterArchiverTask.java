package org.novasearch.jitter.tasks;

import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import org.novasearch.jitter.twitter.TwitterManager;

import java.io.PrintWriter;

public class TwitterArchiverTask extends Task {

    private final TwitterManager twitterManager;

    public TwitterArchiverTask(TwitterManager twitterManager) {
        super("twitter-archiver");
        this.twitterManager = twitterManager;
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter output) throws Exception {
        twitterManager.archive();
    }
}
