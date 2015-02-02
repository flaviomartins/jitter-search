package org.novasearch.jitter.health;

import com.codahale.metrics.health.HealthCheck;
import org.novasearch.jitter.rs.ResourceSelection;

public class ResourceSelectionHealthCheck extends HealthCheck {
    private final ResourceSelection resourceSelection;

    public ResourceSelectionHealthCheck(ResourceSelection resourceSelection) {
        this.resourceSelection = resourceSelection;
    }

    @Override
    protected Result check() throws Exception {
        String index = resourceSelection.getIndex();
        if (index == null) {
            return Result.unhealthy("ResourceSelection doesn't have any index");
        }
        return Result.healthy();
    }
}
