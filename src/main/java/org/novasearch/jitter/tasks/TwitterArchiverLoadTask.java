package org.novasearch.jitter.tasks;

import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import org.novasearch.jitter.core.twitter_archiver.TwitterArchiver;

import java.io.PrintWriter;

public class TwitterArchiverLoadTask extends Task {

    private final TwitterArchiver twitterArchiver;

    public TwitterArchiverLoadTask(TwitterArchiver twitterArchiver) {
        super("ta-load");
        this.twitterArchiver = twitterArchiver;
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter output) throws Exception {
        twitterArchiver.load();
    }
}
