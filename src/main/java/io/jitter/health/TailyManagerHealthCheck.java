package io.jitter.health;

import com.codahale.metrics.health.HealthCheck;
import io.jitter.core.taily.TailyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TailyManagerHealthCheck extends HealthCheck {
    private static final Logger logger = LoggerFactory.getLogger(SelectionManagerHealthCheck.class);

    private final TailyManager tailyManager;

    public TailyManagerHealthCheck(TailyManager tailyManager) {
        this.tailyManager = tailyManager;
    }

    @Override
    protected Result check() throws Exception {
        if (tailyManager == null)
            return Result.unhealthy("TailyManager is null.");
        return Result.healthy();
    }
}
