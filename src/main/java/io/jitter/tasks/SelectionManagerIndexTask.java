package io.jitter.tasks;

import io.dropwizard.servlets.tasks.Task;
import io.jitter.core.selection.SelectionManager;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

public class SelectionManagerIndexTask extends Task {

    private final SelectionManager selectionManager;

    public SelectionManagerIndexTask(SelectionManager selectionManager) {
        super("selection-index");
        this.selectionManager = selectionManager;
    }

    @Override
    public void execute(Map<String, List<String>> map, PrintWriter printWriter) throws Exception {
        if (selectionManager.isIndexing())
            throw new TaskIsAlreadyRunningException(getName() + " is already running.");

        selectionManager.index();
    }
}
