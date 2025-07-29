package io.jitter.tasks;

import io.dropwizard.servlets.tasks.Task;
import io.jitter.core.shards.ShardsManager;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

public class ShardsManagerStatsTask extends Task {

    private final ShardsManager shardsManager;

    public ShardsManagerStatsTask(ShardsManager shardsManager) {
        super("shards-stats");
        this.shardsManager = shardsManager;
    }

    @Override
    public void execute(Map<String, List<String>> map, PrintWriter printWriter) throws Exception {
        shardsManager.collectStats();
    }
}
