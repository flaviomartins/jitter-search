package org.novasearch.jitter.core.selection.methods;

import org.novasearch.jitter.api.search.Document;

import java.util.List;
import java.util.Map;

public class Votes extends SelectionMethod {

    protected Votes() {
    }

    @Override
    public Map<String, Double> rank(List<Document> results) {
        return getCounts(results);
    }
}
