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
        return Result.healthy();
    }
}
