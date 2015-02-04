package org.novasearch.jitter.health;

import com.codahale.metrics.health.HealthCheck;
import org.novasearch.jitter.core.selection.SelectionManager;

public class SelectionManagerHealthCheck extends HealthCheck {
    private final SelectionManager selectionManager;

    public SelectionManagerHealthCheck(SelectionManager selectionManager) {
        this.selectionManager = selectionManager;
    }

    @Override
    protected Result check() throws Exception {
        String index = selectionManager.getIndex();
        if (index == null) {
            return Result.unhealthy("ResourceSelection doesn't have any index");
        }
        return Result.healthy();
    }
}
