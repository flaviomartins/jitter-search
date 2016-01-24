package io.jitter.tasks;

import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import io.jitter.core.taily.TailyManager;

import java.io.PrintWriter;

public class TailyManagerIndexTask extends Task {

    private final TailyManager tailyManager;

    public TailyManagerIndexTask(TailyManager tailyManager) {
        super("taily-index");
        this.tailyManager = tailyManager;
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter output) throws Exception {
        tailyManager.index();
    }
}
