package io.jitter.tasks;

import com.google.common.collect.ImmutableMultimap;
import io.dropwizard.servlets.tasks.Task;
import io.jitter.core.search.SearchManager;

import java.io.PrintWriter;

public class SearchManagerIndexTask extends Task {

    private final SearchManager searchManager;

    public SearchManagerIndexTask(SearchManager searchManager) {
        super("search-index");
        this.searchManager = searchManager;
    }

    @Override
    public void execute(ImmutableMultimap<String, String> parameters, PrintWriter output) throws Exception {
        // not implemented
    }
}
