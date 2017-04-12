package io.jitter.health;

import com.codahale.metrics.health.HealthCheck;
import io.jitter.core.selection.SelectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelectionManagerHealthCheck extends HealthCheck {

    private static final Logger logger = LoggerFactory.getLogger(SelectionManagerHealthCheck.class);

    private final SelectionManager selectionManager;

    public SelectionManagerHealthCheck(SelectionManager selectionManager) {
        this.selectionManager = selectionManager;
    }

    @Override
    protected Result check() throws Exception {
        String indexPath = selectionManager.getIndexPath();
        if (indexPath == null) {
            return Result.unhealthy("SelectionManager doesn't have any index");
        }

        return Result.healthy();
    }
}
