package io.jitter.core.selection.methods;

import io.jitter.api.search.ShardedDocument;
import io.jitter.core.shards.ShardStats;

import java.util.List;
import java.util.Map;

public class Votes<E extends ShardedDocument> extends SelectionMethod<E> {

    Votes() {
    }

    @Override
    public Map<String, Double> rank(List<E> results, ShardStats csiStats) {
        return getCounts(results);
    }
}
