package io.jitter.core.selection.methods;

import io.jitter.api.search.ShardedDocument;
import io.jitter.core.shards.ShardStats;

import java.util.List;
import java.util.Map;

public class Votes extends SelectionMethod {

    Votes() {
    }

    @Override
    public Map<String, Double> rank(List<ShardedDocument> results, ShardStats csiStats) {
        return getCounts(results);
    }
}
