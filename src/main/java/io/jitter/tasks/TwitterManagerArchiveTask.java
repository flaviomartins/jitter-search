package io.jitter.tasks;

import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import io.jitter.core.twitter.manager.TwitterManager;

import java.io.PrintWriter;

public class TwitterManagerArchiveTask extends Task {

    private final TwitterManager twitterManager;

    public TwitterManagerArchiveTask(TwitterManager twitterManager) {
        super("tm-archive");
        this.twitterManager = twitterManager;
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter output) throws Exception {
        twitterManager.archive();
    }
}