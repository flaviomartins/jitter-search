package io.jitter.tasks;

import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import io.jitter.core.selection.SelectionManager;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

public class SelectionManagerStatsTask extends Task {

    private final SelectionManager selectionManager;

    public SelectionManagerStatsTask(SelectionManager selectionManager) {
        super("selection-stats");
        this.selectionManager = selectionManager;
    }

    @Override
    public void execute(Map<String, List<String>> parameters, PrintWriter output) throws Exception {
        selectionManager.collectStats();
    }
}
