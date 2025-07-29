package io.jitter.tasks;

import io.dropwizard.servlets.tasks.Task;
import io.jitter.core.search.SearchManager;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

public class SearchManagerForceMergeTask extends Task {

    private final SearchManager searchManager;

    public SearchManagerForceMergeTask(SearchManager searchManager) {
        super("search-forcemerge");
        this.searchManager = searchManager;
    }

    @Override
    public void execute(Map<String, List<String>> map, PrintWriter printWriter) throws Exception {
        searchManager.forceMerge();
    }
}
