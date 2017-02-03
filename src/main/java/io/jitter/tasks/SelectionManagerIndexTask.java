package io.jitter.tasks;

import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import io.jitter.core.selection.SelectionManager;

import java.io.PrintWriter;

public class SelectionManagerIndexTask extends Task {

    private final SelectionManager selectionManager;

    public SelectionManagerIndexTask(SelectionManager selectionManager) {
        super("selection-index");
        this.selectionManager = selectionManager;
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter output) throws Exception {
        if (selectionManager.isIndexing())
            throw new TaskIsAlreadyRunningException(getName() + " is already running.");

        selectionManager.index();
    }
}
