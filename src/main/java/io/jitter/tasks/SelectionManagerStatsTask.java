package io.jitter.tasks;

import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import io.jitter.core.selection.SelectionManager;

import java.io.PrintWriter;

public class SelectionManagerStatsTask extends Task {

    private final SelectionManager selectionManager;

    public SelectionManagerStatsTask(SelectionManager selectionManager) {
        super("selection-stats");
        this.selectionManager = selectionManager;
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter output) throws Exception {
        selectionManager.collectStats();
    }
}
