package io.jitter.tasks;

import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import io.jitter.core.shards.ShardsManager;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

public class ShardsManagerForceMergeTask extends Task {

    private final ShardsManager shardsManager;

    public ShardsManagerForceMergeTask(ShardsManager shardsManager) {
        super("shards-forcemerge");
        this.shardsManager = shardsManager;
    }

    @Override
    public void execute(Map<String, List<String>> parameters, PrintWriter output) throws Exception {
        if (shardsManager.isIndexing())
            throw new TaskIsAlreadyRunningException(getName() + " is already running.");

        shardsManager.forceMerge();
    }
}
