package io.jitter.core.rerank;

import io.jitter.api.search.Document;

import java.util.Collections;
import java.util.List;


public abstract class Reranker {
    List<Document> results;

    protected abstract void score();

    public List<Document> getReranked() {
        DocumentComparator comparator = new DocumentComparator(true);
        Collections.sort(results, comparator);
        return results;
    }

    public List<Document> getResults() {
        return results;
    }
}
