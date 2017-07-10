package io.jitter.core.selection.methods;

import io.jitter.api.search.StatusDocument;
import io.jitter.core.shards.ShardStats;

import java.util.List;
import java.util.Map;

public class Votes extends SelectionMethod {

    Votes() {
    }

    @Override
    public Map<String, Double> rank(List<StatusDocument> results, ShardStats csiStats) {
        return getCounts(results);
    }
}
