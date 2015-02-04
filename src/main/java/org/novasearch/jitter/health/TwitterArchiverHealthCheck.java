package org.novasearch.jitter.health;

import com.codahale.metrics.health.HealthCheck;
import org.novasearch.jitter.core.twitter_archiver.TwitterArchiver;

public class TwitterArchiverHealthCheck extends HealthCheck {
    private final TwitterArchiver twitterArchiver;

    public TwitterArchiverHealthCheck(TwitterArchiver twitterArchiver) {
        this.twitterArchiver = twitterArchiver;
    }

    @Override
    protected Result check() throws Exception {
        final int numUsers = twitterArchiver.getUsers().size();
        if (numUsers == 0) {
            return Result.unhealthy("TwitterArchiver doesn't have any Twitter users");
        }
        return Result.healthy();
    }
}
