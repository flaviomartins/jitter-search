package io.jitter.tasks;

import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import io.jitter.core.selection.SelectionManager;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

public class SelectionManagerForceMergeTask extends Task {

    private final SelectionManager selectionManager;

    public SelectionManagerForceMergeTask(SelectionManager selectionManager) {
        super("selection-forcemerge");
        this.selectionManager = selectionManager;
    }

    @Override
    public void execute(Map<String, List<String>> parameters, PrintWriter output) throws Exception {
        if (selectionManager.isIndexing())
            throw new TaskIsAlreadyRunningException(getName() + " is already running.");

        selectionManager.forceMerge();
    }
}
