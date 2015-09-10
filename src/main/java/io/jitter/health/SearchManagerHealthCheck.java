package io.jitter.health;

import com.codahale.metrics.health.HealthCheck;
import io.jitter.core.search.SearchManager;

public class SearchManagerHealthCheck extends HealthCheck {
    private final SearchManager searchManager;

    public SearchManagerHealthCheck(SearchManager searchManager) {
        this.searchManager = searchManager;
    }

    @Override
    protected Result check() throws Exception {
        String index = searchManager.getIndexPath();
        if (index == null) {
            return Result.unhealthy("SearchManager doesn't have any index");
        }
        return Result.healthy();
    }
}
