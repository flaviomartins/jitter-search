package io.jitter.health;

import com.codahale.metrics.health.HealthCheck;
import io.jitter.core.shards.ShardsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardsManagerHealthCheck extends HealthCheck {

    private static final Logger logger = LoggerFactory.getLogger(SelectionManagerHealthCheck.class);

    private final ShardsManager shardsManager;

    public ShardsManagerHealthCheck(ShardsManager shardsManager) {
        this.shardsManager = shardsManager;
    }

    @Override
    protected Result check() throws Exception {
        String indexPath = shardsManager.getIndexPath();
        if (indexPath == null) {
            return Result.unhealthy("SelectionManager doesn't have any index");
        }

        return Result.healthy();
    }
}
