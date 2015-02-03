package org.novasearch.jitter.rs.methods;

import org.novasearch.jitter.api.search.Document;
import org.novasearch.jitter.rs.ResourceSelectionMethod;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Votes extends ResourceSelectionMethod {


    public Votes() {
    }

    @Override
    public Map<String, Float> rank(List<Document> results) {
        return getCounts(results);
    }
}
