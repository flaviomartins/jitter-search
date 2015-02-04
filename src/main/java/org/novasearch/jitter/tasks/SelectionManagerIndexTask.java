package org.novasearch.jitter.tasks;

import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import org.novasearch.jitter.core.selection.SelectionManager;

import java.io.PrintWriter;

public class SelectionManagerIndexTask extends Task {

    private final SelectionManager selectionManager;

    public SelectionManagerIndexTask(SelectionManager selectionManager) {
        super("selection-index");
        this.selectionManager = selectionManager;
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter output) throws Exception {
        selectionManager.index();
    }
}
