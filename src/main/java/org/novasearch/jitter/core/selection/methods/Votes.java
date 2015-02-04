package org.novasearch.jitter.core.selection.methods;

import org.novasearch.jitter.api.search.Document;
import org.novasearch.jitter.core.selection.SelectionMethod;

import java.util.List;
import java.util.Map;

public class Votes extends SelectionMethod {


    public Votes() {
    }

    @Override
    public Map<String, Float> rank(List<Document> results) {
        return getCounts(results);
    }
}
