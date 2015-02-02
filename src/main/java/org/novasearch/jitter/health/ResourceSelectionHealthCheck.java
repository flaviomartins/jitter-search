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
        final int numTwitter = resourceSelection.getTwitter().size();
        if (numTwitter == 0) {
            return Result.unhealthy("resourceSelection doesn't have any Twitter sources");
        }
        return Result.healthy();
    }
}
