package org.novasearch.jitter.health;

import com.codahale.metrics.health.HealthCheck;
import org.apache.log4j.Logger;
import org.novasearch.jitter.core.selection.taily.TailyManager;

public class TailyManagerHealthCheck extends HealthCheck {
    private static final Logger logger = Logger.getLogger(SelectionManagerHealthCheck.class);

    private final TailyManager tailyManager;

    public TailyManagerHealthCheck(TailyManager tailyManager) {
        this.tailyManager = tailyManager;
    }

    @Override
    protected Result check() throws Exception {
        return Result.healthy();
    }
}
