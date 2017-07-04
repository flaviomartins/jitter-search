package io.jitter.health;

import com.codahale.metrics.health.HealthCheck;
import io.jitter.core.wikipedia.WikipediaManager;

public class WikipediaManagerHealthCheck extends HealthCheck {
    private final WikipediaManager wikipediaManager;

    public WikipediaManagerHealthCheck(WikipediaManager wikipediaManager) {
        this.wikipediaManager = wikipediaManager;
    }

    @Override
    protected Result check() throws Exception {
        String index = wikipediaManager.getIndexPath();
        if (index == null) {
            return Result.unhealthy("WikipediaManager doesn't have any index");
        }
        return Result.healthy();
    }
}
