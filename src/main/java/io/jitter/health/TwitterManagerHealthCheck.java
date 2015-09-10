package io.jitter.health;

import com.codahale.metrics.health.HealthCheck;
import io.jitter.core.twitter.manager.TwitterManager;

public class TwitterManagerHealthCheck extends HealthCheck {
    private final TwitterManager twitterManager;

    public TwitterManagerHealthCheck(TwitterManager twitterManager) {
        this.twitterManager = twitterManager;
    }

    @Override
    protected Result check() throws Exception {
        final int numUsers = twitterManager.getUsers().size();
        if (numUsers == 0) {
            return Result.unhealthy("TwitterManager doesn't have any Twitter users");
        }
        return Result.healthy();
    }
}
