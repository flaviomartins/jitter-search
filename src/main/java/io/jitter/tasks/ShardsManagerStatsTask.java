package io.jitter.tasks;

import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import io.jitter.core.shards.ShardsManager;

import java.io.PrintWriter;

public class ShardsManagerStatsTask extends Task {

    private final ShardsManager shardsManager;

    public ShardsManagerStatsTask(ShardsManager shardsManager) {
        super("shards-stats");
        this.shardsManager = shardsManager;
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter output) throws Exception {
        shardsManager.collectStats();
    }
}
