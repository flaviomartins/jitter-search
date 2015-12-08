package io.jitter.core.filter;

import io.jitter.api.search.Document;

import java.util.List;

public abstract class SearchFilter {
    List<Document> results;

    protected abstract void filter();

    public List<Document> getFiltered() {
        return results;
    }
}
