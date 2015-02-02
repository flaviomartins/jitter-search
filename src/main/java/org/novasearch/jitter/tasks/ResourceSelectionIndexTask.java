package org.novasearch.jitter.tasks;

import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import org.novasearch.jitter.rs.ResourceSelection;

import java.io.PrintWriter;

public class ResourceSelectionIndexTask extends Task {

    private final ResourceSelection resourceSelection;

    public ResourceSelectionIndexTask(ResourceSelection resourceSelection) {
        super("index");
        this.resourceSelection = resourceSelection;
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter output) throws Exception {
        resourceSelection.index();
    }
}
